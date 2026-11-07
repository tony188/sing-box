package group

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

// Only failures allocate entries. Never evict an unconfirmed fault to admit a
// new target: a flood of destinations must not silently readmit a known fault.
const balanceTargetLimit = 256
const balanceTargetIdle = 2 * time.Minute

type balanceTargetKey struct {
	index       int
	destination M.Socksaddr
}
type balanceTargetHealth struct {
	dial                        M.Socksaddr
	state                       balanceHealthState
	failed, checked, used, next time.Time
	successes                   int
	checking                    bool
	revision                    uint64
}
type balanceTargetCheck struct {
	dial     M.Socksaddr
	key      balanceTargetKey
	entry    *balanceTargetHealth
	revision uint64
	since    time.Time
}
type balanceHealthJob struct {
	index  int
	target *balanceTargetCheck
}

func balanceTargetAddress(dst M.Socksaddr) M.Socksaddr {
	dst.Fqdn = strings.ToLower(strings.TrimSuffix(dst.Fqdn, "."))
	return dst
}

func (s *LoadBalance) recordTargetFailure(index int, dst M.Socksaddr, now time.Time, confirmed bool, dial ...M.Socksaddr) {
	if !dst.IsValid() {
		return
	}
	s.access.Lock()
	defer s.access.Unlock()
	if s.closed {
		return
	}
	key := balanceTargetKey{index, balanceTargetAddress(dst)}
	t := s.targets[key]
	if t == nil {
		// An idle unconfirmed suspicion may retire; a confirmed fault may not.
		if len(s.targets) >= balanceTargetLimit {
			for k, old := range s.targets {
				if old.state == balanceUnknown && !old.checking && now.Sub(old.used) > balanceTargetIdle {
					delete(s.targets, k)
				}
			}
		}
		if len(s.targets) >= balanceTargetLimit {
			return
		}
		if s.targets == nil {
			s.targets = make(map[balanceTargetKey]*balanceTargetHealth)
		}
		t = &balanceTargetHealth{state: balanceUnknown}
		s.targets[key] = t
	}
	// A no-first-response watchdog is only a suspicion. It cannot turn a normal
	// idle/server-first application into a fault without a failed target probe.
	if confirmed || t.state == balanceRecovering {
		t.state, t.successes = balanceDown, 0
	}
	t.successes = 0
	t.revision++
	t.dial = dst
	if len(dial) > 0 {
		t.dial = dial[0]
	}
	t.failed, t.used = now, now
	if t.next.IsZero() || t.next.Before(now) {
		t.next = now
	}
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Called with access held, from the same selection/reservation transaction.
func (s *LoadBalance) targetExcludedLocked(index int, dst M.Socksaddr, now time.Time) bool {
	t := s.targets[balanceTargetKey{index, balanceTargetAddress(dst)}]
	if t == nil {
		return false
	}
	if now.Sub(t.used) > balanceTargetIdle {
		t.next = now
		select {
		case s.wake <- struct{}{}:
		default:
		}
	}
	t.used = now
	return t.state == balanceDown || t.state == balanceRecovering
}

func (s *LoadBalance) reserveTargetCheck(now time.Time) (*balanceTargetCheck, time.Duration) {
	s.access.Lock()
	defer s.access.Unlock()
	wait := 24 * time.Hour
	if s.closed {
		return nil, wait
	}
	var key balanceTargetKey
	var chosen *balanceTargetHealth
	for k, t := range s.targets {
		if t.checking || now.Sub(t.used) > balanceTargetIdle {
			continue
		}
		if t.next.After(now) {
			wait = min(wait, t.next.Sub(now))
			continue
		}
		if chosen == nil || t.next.Before(chosen.next) {
			key, chosen = k, t
		}
	}
	if chosen == nil {
		return nil, wait
	}
	chosen.checking = true
	return &balanceTargetCheck{key: key, dial: chosen.dial, entry: chosen, revision: chosen.revision, since: maxTime(chosen.checked, chosen.failed)}, 0
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

// Both sorts of checks share four workers and the kernel-wide probe pacer.
// Alternate the queues so a large fault set cannot starve ordinary health.
func (s *LoadBalance) reserveHealthJob(now time.Time) (*balanceHealthJob, time.Duration) {
	s.targetTurn = !s.targetTurn // scheduler goroutine owns this flag
	wait := 24 * time.Hour
	for pass := 0; pass < 2; pass++ {
		if (pass == 0) == s.targetTurn {
			check, delay := s.reserveTargetCheck(now)
			wait = min(wait, delay)
			if check != nil {
				return &balanceHealthJob{target: check}, 0
			}
		} else {
			index, delay := s.reserveCheck(now)
			wait = min(wait, delay)
			if index >= 0 {
				return &balanceHealthJob{index: index}, 0
			}
		}
	}
	return nil, wait
}

func (s *LoadBalance) checkTarget(ctx context.Context, check *balanceTargetCheck) {
	result := urltest.ProbeResult{Err: context.Canceled}
	member := s.members[check.key.index].outbound
	if s.history != nil && ctx.Err() == nil {
		// A distinct pacing owner and no shared history keep this result private.
		key := "openbox-lb-target:" + check.key.destination.String()
		result = s.history.ProbeTransient(ctx, key, func(probeCtx context.Context) (uint16, error) {
			if s.manager != nil {
				if current, ok := s.manager.Outbound(member.Tag()); !ok || current != member {
					return 0, context.Canceled
				}
			}
			probeCtx, cancel := context.WithTimeout(probeCtx, s.timeout)
			defer cancel()
			delay, err := balanceProbeTarget(probeCtx, member, check.key.destination, check.dial)
			if s.manager != nil {
				if current, ok := s.manager.Outbound(member.Tag()); !ok || current != member {
					return 0, context.Canceled
				}
			}
			return delay, err
		})
	}
	s.finishTargetCheck(check, result, time.Now())
}

func balanceApplicationDestination(ctx context.Context, dst M.Socksaddr) M.Socksaddr {
	if dst.IsFqdn() {
		return dst
	}
	if m := adapter.ContextFrom(ctx); m != nil {
		if m.Destination.IsFqdn() && m.Destination.Port == dst.Port {
			return m.Destination
		}
		if m.Domain != "" {
			return M.Socksaddr{Fqdn: m.Domain, Port: dst.Port}
		}
	}
	return dst
}

type balancePinnedDialer struct {
	adapter.Outbound
	destination M.Socksaddr
}

func (d balancePinnedDialer) DialContext(ctx context.Context, network string, _ M.Socksaddr) (net.Conn, error) {
	return d.Outbound.DialContext(ctx, network, d.destination)
}
func (d balancePinnedDialer) Upstream() any { return d.Outbound }

func balanceProbeTarget(ctx context.Context, member adapter.Outbound, dst M.Socksaddr, dial ...M.Socksaddr) (uint16, error) {
	if len(dial) > 0 && dial[0] != dst {
		member = balancePinnedDialer{Outbound: member, destination: dial[0]}
	}
	if dst.Port == 80 || dst.Port == 443 {
		scheme := "http"
		if dst.Port == 443 {
			scheme = "https"
		}
		// HEAD / without cookies/auth, no redirects or response body. Every HTTP
		// status proves reachability; this is not an application-status health policy.
		delay, err := urltest.URLTestExpected(ctx, scheme+"://"+dst.String()+"/", urltest.ExpectedStatus{}, member)
		var cert *tls.CertificateVerificationError
		if errors.As(err, &cert) {
			return 0, context.Canceled
		} // inconclusive, do not weaken certificate verification
		return delay, err
	}
	// For an opaque TCP service only the proxy/transport handshake is knowable.
	start := time.Now()
	c, err := member.DialContext(ctx, N.NetworkTCP, dst)
	if err != nil {
		return 0, err
	}
	defer c.Close()
	stop := context.AfterFunc(ctx, func() { c.Close() })
	defer stop()
	if N.NeedHandshakeForWrite(c) {
		_, err = c.Write(nil)
	}
	return uint16(max(1, time.Since(start)/time.Millisecond)), err
}

func (s *LoadBalance) finishTargetCheck(check *balanceTargetCheck, result urltest.ProbeResult, now time.Time) {
	if check == nil {
		return
	}
	s.access.Lock()
	defer s.access.Unlock()
	t := s.targets[check.key]
	if t != check.entry {
		return
	}
	t.checking = false
	if s.closed {
		return
	}
	fresh := t.revision == check.revision && result.Time.After(check.since)
	if fresh && !errors.Is(result.Err, context.Canceled) {
		t.checked = result.Time
		if result.Err == nil && result.Delay > 0 {
			t.successes++
			if t.state == balanceUnknown || t.successes >= balanceRise {
				delete(s.targets, check.key)
				return
			}
			t.state = balanceRecovering
		} else {
			t.state, t.successes = balanceDown, 0
		}
	}
	t.next = now.Add(s.recoveryInterval())
}

type balanceConn struct {
	net.Conn
	once               sync.Once
	release            func()
	ctx                context.Context
	group              *LoadBalance
	member             adapter.Outbound
	destination        M.Socksaddr
	dialDestination    M.Socksaddr
	tcp                bool
	closed             atomic.Bool
	fault              sync.Once
	watch              sync.Mutex
	written, responded atomic.Bool
	timer              *time.Timer
}

func (c *balanceConn) Close() error {
	c.closed.Store(true)
	c.watch.Lock()
	if c.timer != nil {
		c.timer.Stop()
	}
	c.watch.Unlock()
	c.once.Do(c.release)
	return c.Conn.Close()
}
func (c *balanceConn) Upstream() any           { return c.Conn }
func (c *balanceConn) ReaderReplaceable() bool { return !c.tcp || c.responded.Load() }
func (c *balanceConn) WriterReplaceable() bool { return !c.tcp || c.written.Load() }

// The copy engine refreshes its unwrapped reader/writer after these initial
// observations. Bulk data then uses the original syscall/read-waiter path,
// without an extra callback or atomic operation for each transferred buffer.
func (c *balanceConn) NeedHandshakeForRead() bool {
	return c.tcp && !c.responded.Load() || N.NeedHandshakeForRead(c.Conn)
}
func (c *balanceConn) NeedHandshakeForWrite() bool {
	return c.tcp && !c.written.Load() || N.NeedHandshakeForWrite(c.Conn)
}
func (c *balanceConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.countRead(int64(n))
	if errors.Is(err, io.EOF) {
		c.ReportCopyResult(true, int64(n), err)
	}
	c.ReportOutboundHandshakeFailure(err)
	return n, err
}
func (c *balanceConn) Write(p []byte) (int, error) {
	// Advertising the initial observation must not add a zero-byte write or
	// handshake to transports which did not need one before this wrapper.
	if c.tcp && len(p) == 0 && !N.NeedHandshakeForWrite(c.Conn) {
		return 0, nil
	}
	n, err := c.Conn.Write(p)
	c.countWrite(int64(n))
	c.ReportOutboundHandshakeFailure(err)
	return n, err
}
func (c *balanceConn) countRead(n int64) {
	if n <= 0 || !c.tcp || c.responded.Load() {
		return
	}
	if c.responded.Swap(true) {
		return
	}
	c.watch.Lock()
	if c.timer != nil {
		c.timer.Stop()
	}
	c.watch.Unlock()
}
func (c *balanceConn) countWrite(n int64) {
	if n <= 0 || !c.tcp || c.written.Load() {
		return
	}
	c.watch.Lock()
	defer c.watch.Unlock()
	if c.written.Load() {
		return
	}
	c.written.Store(true)
	if c.responded.Load() || c.closed.Load() || c.destination.Port != 80 && c.destination.Port != 443 {
		return
	}
	c.timer = time.AfterFunc(c.group.timeout, func() {
		c.watch.Lock()
		suspect := !c.responded.Load() && !c.closed.Load()
		c.watch.Unlock()
		if suspect && c.ctx.Err() == nil {
			c.recordFault(false)
		}
	})
}
func (c *balanceConn) recordFault(confirmed bool) {
	if c.group == nil || !c.tcp || c.closed.Load() || c.ctx.Err() != nil {
		return
	}
	c.fault.Do(func() { c.recordTarget(confirmed) })
}
func (c *balanceConn) recordTarget(confirmed bool) {
	for i, m := range c.group.members {
		if m.outbound == c.member {
			c.group.recordTargetFailure(i, c.destination, time.Now(), confirmed, c.dialDestination)
			return
		}
	}
}

// The router calls this before closing either half. Typed net.OpError identifies
// the failing side, so a client reset does not poison the destination's node.
func (c *balanceConn) ReportCopyResult(download bool, n int64, err error) {
	// A peer closing HTTP(S) without any response after a request is worth a
	// bounded verification. Plain EOF on an idle/finished connection is not.
	if download && n == 0 && (err == nil || errors.Is(err, io.EOF)) && c.written.Load() && !c.responded.Load() {
		c.recordFault(false)
		return
	}
	if !balanceMeaningfulStreamError(err) {
		return
	}
	var op *net.OpError
	if errors.As(err, &op) {
		if download && op.Op == "read" || !download && op.Op == "write" {
			// A peer may reset an idle or completed connection intentionally.
			// Verify this destination before excluding it, rather than guessing
			// the encrypted application's success/failure from a TCP reset.
			c.recordFault(false)
		}
		return
	}
	// Protocol errors may be untyped. Confirm against the target before exclusion.
	c.recordFault(false)
}

func balanceMeaningfulStreamError(err error) bool {
	return err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}

// Here the failing I/O is known to belong to the outbound: deferred proxy
// handshakes may fail with a read error inside Write, before copying begins.
func (c *balanceConn) ReportOutboundHandshakeFailure(err error) {
	if balanceMeaningfulStreamError(err) {
		c.recordFault(false)
	}
}
