package group

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/interrupt"
	C "github.com/sagernet/sing-box/constant"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

// One outer URLTest owns the whole request budget; nested groups cannot multiply it.
type dialFallbackKey struct{}
type dialBudgetKey struct{}
type dialBudget struct {
	access   sync.Mutex
	deadline time.Time
	attempts int
}

func (s *URLTest) DialFallbackContext(ctx context.Context) context.Context {
	if ctx.Value(dialBudgetKey{}) != nil {
		return ctx
	}
	deadline := time.Now().Add(dialFallbackTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	return context.WithValue(ctx, dialBudgetKey{}, &dialBudget{deadline: deadline})
}
func (b *dialBudget) take() bool {
	b.access.Lock()
	defer b.access.Unlock()
	if b.attempts >= 2 || !time.Now().Before(b.deadline) {
		return false
	}
	b.attempts++
	return true
}

var fallbackDialSlots = make(chan struct{}, 4)
var dialFallbackTimeout = 15 * time.Second

type frozenDialOutbound struct {
	adapter.Outbound
	groups []adapter.OutboundGroup
}
type frozenDialConn struct {
	net.Conn
	detach   []func()
	once     sync.Once
	closeErr error
}

func (c *frozenDialConn) Close() error {
	c.once.Do(func() {
		for _, remove := range c.detach {
			remove()
		}
		c.closeErr = c.Conn.Close()
	})
	return c.closeErr
}
func (c *frozenDialConn) Upstream() any           { return c.Conn }
func (c *frozenDialConn) ReaderReplaceable() bool { return true }
func (c *frozenDialConn) WriterReplaceable() bool { return true }
func (o *frozenDialOutbound) DialContext(ctx context.Context, n string, d M.Socksaddr) (net.Conn, error) {
	for _, group := range o.groups {
		if child, ok := group.(*URLTest); ok {
			child.group.Touch()
		}
	}
	conn, err := o.Outbound.DialContext(ctx, n, d)
	if err != nil {
		return conn, err
	}
	tracked := &frozenDialConn{Conn: conn}
	for _, g := range o.groups {
		tracked.detach = append(tracked.detach, g.AttachConnection(tracked))
	}
	return tracked, nil
}
func freezeDial(outbound adapter.Outbound) *frozenDialOutbound {
	f := &frozenDialOutbound{}
	seen := map[string]bool{}
	for {
		if outbound == nil || seen[outbound.Tag()] {
			return nil
		}
		seen[outbound.Tag()] = true
		g, ok := outbound.(adapter.OutboundGroup)
		if !ok {
			f.Outbound = outbound
			return f
		}
		f.groups = append(f.groups, g)
		outbound = g.Selected(N.NetworkTCP)
	}
}
func (s *URLTest) ConfirmFailure(leaf adapter.Outbound, network string) {
	s.group.confirmFailure(leaf, network)
}
func (s *URLTest) DialContextWithFallback(ctx context.Context, network string, d M.Socksaddr, leaf adapter.Outbound) (net.Conn, error) {
	return s.dialTCPWithFallback(ctx, network, d, leaf)
}
func (s *URLTest) recordDialChain(ctx context.Context, chosen *frozenDialOutbound) {
	metadata := adapter.ContextFrom(ctx)
	if metadata == nil {
		return
	}
	for i, it := range metadata.OutboundChain {
		if it == s {
			chain := append([]adapter.Outbound(nil), metadata.OutboundChain[:i+1]...)
			for _, g := range chosen.groups {
				chain = append(chain, g)
			}
			metadata.OutboundChain = append(chain, chosen.Outbound)
			return
		}
	}
}

func (g *URLTestGroup) dialBackup(primary adapter.Outbound) adapter.Outbound {
	failed := RealTag(primary, N.NetworkTCP)
	var best adapter.Outbound
	var delay uint16
	now := time.Now()
	for _, candidate := range g.outbounds {
		tag := RealTag(candidate, N.NetworkTCP)
		if tag == "" || tag == failed {
			continue
		}
		leaf, ok := g.outbound.Outbound(tag)
		if !ok {
			continue
		}
		if leaf.Type() == C.TypeDirect || leaf.Type() == C.TypeBlock {
			continue
		}
		supports := false
		for _, network := range leaf.Network() {
			if network == N.NetworkTCP {
				supports = true
				break
			}
		}
		if !supports {
			continue
		}
		h := g.history.LoadProbeHistory(tag, g.ProbeLink())
		if h == nil || h.Delay == 0 || now.Before(h.Time) || now.Sub(h.Time) > g.interval {
			continue
		}
		if best == nil || h.Delay < delay {
			best = candidate
			delay = h.Delay
		}
	}
	return best
}

// A successful connection may lazily finish its protocol handshake on first
// write. Retire the dialing timer without cancelling the context it still owns.
type dialAttemptContext struct {
	context.Context
	access          sync.Mutex
	deadline        time.Time
	active, expired bool
}

func (c *dialAttemptContext) Deadline() (time.Time, bool) {
	c.access.Lock()
	active := c.active
	deadline := c.deadline
	c.access.Unlock()
	if active {
		return deadline, true
	}
	return c.Context.Deadline()
}
func (c *dialAttemptContext) Err() error {
	c.access.Lock()
	expired := c.expired
	c.access.Unlock()
	if expired {
		return context.DeadlineExceeded
	}
	return c.Context.Err()
}

type dialAttemptConn struct {
	net.Conn
	cancel context.CancelFunc
}

func (c *dialAttemptConn) Upstream() any           { return c.Conn }
func (c *dialAttemptConn) ReaderReplaceable() bool { return true }
func (c *dialAttemptConn) WriterReplaceable() bool { return true }
func (c *dialAttemptConn) Close() error            { err := c.Conn.Close(); c.cancel(); return err }

func dialAttempt(parent context.Context, timeout time.Duration, outbound adapter.Outbound, network string, destination M.Socksaddr) (net.Conn, error) {
	base, cancel := context.WithCancel(parent)
	ctx := &dialAttemptContext{Context: base, deadline: time.Now().Add(timeout), active: true}
	timer := time.AfterFunc(timeout, func() {
		ctx.access.Lock()
		expire := ctx.active
		if expire {
			ctx.active = false
			ctx.expired = true
		}
		ctx.access.Unlock()
		if expire {
			cancel()
		}
	})
	conn, err := outbound.DialContext(ctx, network, destination)
	ctx.access.Lock()
	ctx.active = false
	expired := ctx.expired
	timer.Stop()
	ctx.access.Unlock()
	if err == nil && !expired && parent.Err() == nil {
		return &dialAttemptConn{Conn: conn, cancel: cancel}, nil
	}
	if conn != nil {
		conn.Close()
	}
	cancel()
	if parent.Err() != nil {
		return nil, parent.Err()
	}
	if expired {
		return nil, context.DeadlineExceeded
	}
	return nil, err
}

func (s *URLTest) dialTCPWithFallback(ctx context.Context, network string, destination M.Socksaddr, primary adapter.Outbound) (net.Conn, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	s.group.Touch()
	primaryFrozen := freezeDial(primary)
	if primaryFrozen == nil {
		return nil, errors.New("missing TCP leaf")
	}
	primary = primaryFrozen
	backup := s.group.dialBackup(primary)
	ctx = s.DialFallbackContext(ctx)
	budget := ctx.Value(dialBudgetKey{}).(*dialBudget)
	deadline := budget.deadline
	nested := context.WithValue(ctx, dialFallbackKey{}, true)
	firstBudget := time.Until(deadline)
	if backup != nil {
		firstBudget /= 2
	}
	if firstBudget <= 0 {
		return nil, context.DeadlineExceeded
	}
	if !budget.take() {
		return nil, errors.New("URLTest dial attempt budget exhausted")
	}
	conn, err := dialAttempt(nested, firstBudget, primary, network, destination)
	if err == nil {
		return s.group.interruptGroup.NewConn(conn, interrupt.IsExternalConnectionFromContext(ctx)), nil
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	s.group.confirmFailure(primary, network)
	remaining := time.Until(deadline)
	if backup == nil || remaining <= 0 {
		return nil, err
	}
	backupFrozen := freezeDial(backup)
	if backupFrozen == nil || backupFrozen.Tag() == primaryFrozen.Tag() || backupFrozen.Type() == C.TypeDirect || backupFrozen.Type() == C.TypeBlock {
		return nil, err
	}
	if current, ok := s.group.outbound.Outbound(backupFrozen.Tag()); !ok || current != backupFrozen.Outbound {
		return nil, err
	}
	h := s.group.history.LoadProbeHistory(backupFrozen.Tag(), s.group.ProbeLink())
	if h == nil || h.Delay == 0 || time.Since(h.Time) < 0 || time.Since(h.Time) > s.group.interval {
		return nil, err
	}
	select {
	case fallbackDialSlots <- struct{}{}:
		defer func() { <-fallbackDialSlots }()
	default:
		return nil, err
	}
	if !budget.take() {
		return nil, err
	}
	s.logger.DebugContext(ctx, "dial fallback: primary failed, trying ", backup.Tag())
	next, nextErr := dialAttempt(nested, remaining, backupFrozen, network, destination)
	if nextErr == nil {
		s.logger.DebugContext(ctx, "dial fallback: backup connected")
		s.recordDialChain(ctx, backupFrozen)
		return s.group.interruptGroup.NewConn(next, interrupt.IsExternalConnectionFromContext(ctx)), nil
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	s.group.confirmFailure(backupFrozen, network)
	s.logger.DebugContext(ctx, "dial fallback: attempts exhausted")
	return nil, errors.Join(err, nextErr)
}
