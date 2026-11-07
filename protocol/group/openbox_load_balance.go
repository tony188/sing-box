package group

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/urltest"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
)

const LoadBalanceType = "loadbalance"

func RegisterLoadBalance(registry *outbound.Registry) {
	outbound.Register[option.LoadBalanceOutboundOptions](registry, LoadBalanceType, NewLoadBalance)
}

type balanceHealthState string

const (
	balanceUnknown    balanceHealthState = "unknown"
	balanceHealthy    balanceHealthState = "healthy"
	balanceDown       balanceHealthState = "down"
	balanceRecovering balanceHealthState = "recovering"
	balanceRise                          = 2
	balanceFall                          = 2
)

type balanceMember struct {
	outbound    adapter.Outbound
	active      [2]int
	failed      [2]time.Time
	checked     time.Time
	state       balanceHealthState
	successes   int
	failures    int
	lastRecheck time.Time
	nextCheck   time.Time
	checking    bool
	delay       uint32 // successful generic probes, EWMA in milliseconds
}

// Deliberately NOT adapter.OutboundGroup: the router flattens that interface to
// a single leaf before DialContext, bypassing balancing, retries and accounting.
// All() is exposed separately by Clash API, and reads never select a member.
type LoadBalance struct {
	bandwidthEnabled bool
	outbound.Adapter
	manager    adapter.OutboundManager
	history    *urltest.HistoryStorage
	tags       []string
	link       string
	expected   urltest.ExpectedStatus
	interval   time.Duration
	timeout    time.Duration
	access     sync.Mutex
	members    []balanceMember
	cursor     int
	closed     bool
	cancel     context.CancelFunc
	wake       chan struct{}
	targets    map[balanceTargetKey]*balanceTargetHealth
	targetTurn bool
}

func NewLoadBalance(ctx context.Context, _ adapter.Router, _ log.ContextLogger, tag string, options option.LoadBalanceOutboundOptions) (adapter.Outbound, error) {
	if len(options.Outbounds) > 64 {
		return nil, errors.New("load balance supports at most 64 nodes")
	}
	for i, member := range options.Outbounds {
		if member == "" || member == tag || slices.Contains(options.Outbounds[:i], member) {
			return nil, errors.New("invalid or duplicate load balance member")
		}
	}
	expected, err := urltest.ParseExpectedStatus(options.ExpectedStatus)
	if err != nil {
		return nil, err
	}
	interval := time.Duration(options.Interval)
	if interval == 0 {
		interval = 120 * time.Second
	}
	if interval < 5*time.Second || interval > 24*time.Hour {
		return nil, errors.New("load balance interval must be 5s to 24h")
	}
	link := options.URL
	if link == "" {
		link = "http://www.gstatic.com/generate_204"
	}
	return &LoadBalance{
		bandwidthEnabled: options.Bandwidth,
		Adapter:          outbound.NewAdapter(LoadBalanceType, tag, []string{N.NetworkTCP, N.NetworkUDP}, options.Outbounds),
		manager:          service.FromContext[adapter.OutboundManager](ctx), history: service.PtrFromContext[urltest.HistoryStorage](ctx),
		tags: append([]string(nil), options.Outbounds...), link: link, expected: expected, interval: interval, timeout: urltest.BackgroundProbeTimeout, wake: make(chan struct{}, 1),
	}, nil
}

func (s *LoadBalance) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	switch stage {
	case adapter.StartStateStart:
		if s.bandwidthEnabled && s.history != nil {
			s.history.Bandwidth().Enable()
		}
		for _, tag := range s.tags {
			member, ok := s.manager.Outbound(tag)
			if !ok {
				return fmt.Errorf("load balance member not found: %s", tag)
			}
			if member.Type() == C.TypeSelector || member.Type() == C.TypeURLTest || member.Type() == LoadBalanceType || member.Type() == C.TypeDirect || member.Type() == C.TypeBlock {
				return fmt.Errorf("load balance requires real proxy nodes: %s", tag)
			}
			s.members = append(s.members, balanceMember{outbound: member, state: balanceUnknown})
		}
	case adapter.StartStateStarted:
		ctx, cancel := context.WithCancel(scope.Context())
		s.cancel = cancel
		scope.Add(s.Close)
		go s.loop(ctx)
	}
	return nil
}

func (s *LoadBalance) Close() error {
	s.access.Lock()
	s.closed = true
	s.targets = nil
	cancel := s.cancel
	s.access.Unlock()
	if cancel != nil {
		cancel()
	}
	// Existing connections belong to the caller. Hot replacement must not close them.
	return nil
}

func (s *LoadBalance) All() []string        { return append([]string{}, s.tags...) }
func (s *LoadBalance) References() []string { return s.All() }

type BalanceMemberStatus struct {
	Name         string             `json:"name"`
	Active       int                `json:"active"`
	Healthy      *bool              `json:"healthy"`
	CheckedAt    int64              `json:"checkedAt"`
	DialFailedAt int64              `json:"dialFailedAt,omitempty"`
	State        balanceHealthState `json:"state"`
	Successes    int                `json:"successes"`
	Failures     int                `json:"failures"`
	NextCheckAt  int64              `json:"nextCheckAt,omitempty"`
}

func (s *LoadBalance) Status() []BalanceMemberStatus {
	s.access.Lock()
	defer s.access.Unlock()
	result := make([]BalanceMemberStatus, 0, len(s.members))
	for _, member := range s.members {
		var healthy *bool
		var checkedAt, failedAt int64
		if !member.checked.IsZero() {
			value := member.state == balanceHealthy
			healthy = &value
			checkedAt = member.checked.UnixMilli()
		}
		latestFailure := member.failed[0]
		if member.failed[1].After(latestFailure) {
			latestFailure = member.failed[1]
		}
		if !latestFailure.IsZero() {
			failedAt = latestFailure.UnixMilli()
		}
		state := member.state
		if state == "" {
			state = balanceUnknown
		}
		var nextCheckAt int64
		if !member.nextCheck.IsZero() {
			nextCheckAt = member.nextCheck.UnixMilli()
		}
		result = append(result, BalanceMemberStatus{Name: member.outbound.Tag(), Active: member.active[0] + member.active[1], Healthy: healthy, CheckedAt: checkedAt, DialFailedAt: failedAt, State: state, Successes: member.successes, Failures: member.failures, NextCheckAt: nextCheckAt})
	}
	return result
}

// Reserve under the same lock as selection: simultaneous connects cannot all
// observe the same idle node. Active includes in-flight dials and UDP sessions.
func (s *LoadBalance) reserve(network string, tried map[int]bool, destination ...M.Socksaddr) (int, adapter.Outbound, int) {
	s.access.Lock()
	defer s.access.Unlock()
	if s.closed {
		return -1, nil, 0
	}
	netIndex := 0
	if N.NetworkName(network) == N.NetworkUDP {
		netIndex = 1
	}
	if s.bandwidthEnabled && s.history != nil {
		if picked, candidates, ok := s.reserveBandwidthLocked(network, tried, destination...); ok {
			s.members[picked].active[netIndex]++
			s.cursor = (picked + 1) % len(s.members)
			return picked, s.members[picked].outbound, candidates
		}
	}
	best := -1
	candidates := 0
	now := time.Now()
	var idle [64]int
	idleCount := 0
	// A failed target is not evidence that every proxy is down. Retry recent
	// dial failures only when no other health-eligible node remains. Confirmed
	// down/recovering nodes stay excluded in every pass until fresh probes pass.
	// Prefer confirmed healthy members over untested ones, even when the latter
	// have fewer active connections. A background check can admit them later.
	for pass := 0; pass < 4 && best == -1; pass++ {
		for step := 0; step < len(s.members); step++ {
			i := (s.cursor + step) % len(s.members)
			m := &s.members[i]
			if tried[i] || !slices.Contains(m.outbound.Network(), N.NetworkName(network)) {
				continue
			}
			if netIndex == 0 && len(destination) > 0 && s.targetExcludedLocked(i, destination[0], now) {
				continue
			}
			if (pass < 2) != m.failed[netIndex].IsZero() {
				continue
			}
			if m.state == balanceDown || m.state == balanceRecovering {
				continue
			}
			if (pass%2 == 1) != (m.state == balanceUnknown) {
				continue
			}
			candidates++
			if best == -1 || m.active[0]+m.active[1] < s.members[best].active[0]+s.members[best].active[1] {
				best = i
				idleCount = 0
			}
			if m.active[0]+m.active[1] == s.members[best].active[0]+s.members[best].active[1] && idleCount < len(idle) {
				idle[idleCount] = i
				idleCount++
			}
		}
	}
	if best == -1 {
		return -1, nil, 0
	}
	// Load always wins over latency. Within the least-active tier prefer the
	// low-latency window (20 ms or 20%), retaining rotating ties. Stale/unknown
	// latency cannot exclude a node; parallel connections still fill every tier.
	shortest := uint32(0)
	for _, i := range idle[:idleCount] {
		m := &s.members[i]
		if m.delay == 0 || now.Sub(m.checked) > max(2*s.interval, 30*time.Second) {
			shortest = 0
			break
		}
		if shortest == 0 || m.delay < shortest {
			shortest = m.delay
		}
	}
	if shortest > 0 {
		for _, i := range idle[:idleCount] {
			if s.members[i].delay <= shortest+max(20, shortest/5) {
				best = i
				break
			}
		}
	}
	s.members[best].active[netIndex]++
	if s.history != nil {
		s.history.Bandwidth().Reserve(s.members[best].outbound.Tag(), []string{s.Tag()})
	}
	s.cursor = (best + 1) % len(s.members)
	return best, s.members[best].outbound, candidates
}

func (s *LoadBalance) release(index int, network string, failed bool) {
	s.access.Lock()
	defer s.access.Unlock()
	n := 0
	if N.NetworkName(network) == N.NetworkUDP {
		n = 1
	}
	m := &s.members[index]
	m.active[n]--
	if s.history != nil {
		s.history.Bandwidth().Release(m.outbound.Tag(), []string{s.Tag()})
	}
	if failed {
		at := time.Now()
		m.failed[n] = at
		// Repeated failures cannot turn every business request into a probe.
		due := at
		if earliest := m.lastRecheck.Add(s.recoveryInterval()); earliest.After(due) {
			due = earliest
		}
		if m.nextCheck.IsZero() || due.Before(m.nextCheck) {
			m.nextCheck = due
		}
		select {
		case s.wake <- struct{}{}:
		default:
		}
	}
}

func (s *LoadBalance) record(ctx context.Context, member adapter.Outbound) {
	if metadata := adapter.ContextFrom(ctx); metadata != nil {
		metadata.OutboundChain = append(append([]adapter.Outbound{}, metadata.OutboundChain...), member)
	}
}

func (s *LoadBalance) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	if N.NetworkName(network) != N.NetworkTCP && N.NetworkName(network) != N.NetworkUDP {
		return nil, N.ErrUnknownNetwork
	}
	healthDestination := balanceApplicationDestination(ctx, destination)
	conn, member, release, err := balanceConnect(s, ctx, network, func(attemptCtx context.Context, budget time.Duration, member adapter.Outbound) (net.Conn, error) {
		return dialAttempt(attemptCtx, budget, member, network, destination)
	}, healthDestination, destination)
	if err != nil {
		return nil, err
	}
	s.record(ctx, member)
	observed := &balanceConn{Conn: conn, release: release, ctx: ctx, group: s, destination: healthDestination, dialDestination: destination, member: member, tcp: N.NetworkName(network) == N.NetworkTCP}
	return observed, nil
}

type balanceConnectResult[T io.Closer] struct {
	conn    T
	member  adapter.Outbound
	index   int
	release func()
	err     error
}

// Keep a full handshake allowance for each member while hedging a stalled dial.
// Only transport connections race: application bytes are sent once, after a
// winner is returned. Bound in-flight attempts and close every losing result.
func balanceConnect[T io.Closer](s *LoadBalance, ctx context.Context, network string, attempt func(context.Context, time.Duration, adapter.Outbound) (T, error), destination ...M.Socksaddr) (T, adapter.Outbound, func(), error) {
	var zero T
	deadline := time.Now().Add(15 * time.Second)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	const parallel = 4
	results := make(chan balanceConnectResult[T], parallel)
	pending := make(map[int]context.CancelFunc)
	tried := make(map[int]bool)
	defer func() {
		for _, cancel := range pending {
			cancel()
		}
		if count := len(pending); count > 0 {
			go func() {
				for range count {
					r := <-results
					if r.err == nil {
						r.conn.Close()
						r.release()
					}
				}
			}()
		}
	}()
	start := func() bool {
		if ctx.Err() != nil || time.Until(deadline) <= 0 {
			return false
		}
		i, member, _ := s.reserve(network, tried, destination...)
		if member == nil {
			return false
		}
		tried[i] = true
		attemptCtx, cancel := context.WithCancel(ctx)
		var released sync.Once
		release := func(failed bool) { released.Do(func() { s.release(i, network, failed) }) }
		pending[i] = func() { cancel(); release(false) }
		budget := min(s.timeout, time.Until(deadline))
		go func() {
			conn, err := attempt(attemptCtx, budget, member)
			if err != nil {
				// A losing/cancelled attempt is not a node failure.
				callerTimeout := budget < s.timeout && errors.Is(err, context.DeadlineExceeded)
				failed := attemptCtx.Err() == nil && ctx.Err() == nil && !callerTimeout && time.Now().Before(deadline)
				if failed && N.NetworkName(network) == N.NetworkTCP && len(destination) > 0 {
					s.recordTargetFailure(i, destination[0], time.Now(), true, destination[1:]...)
				}
				release(failed)
				cancel()
			}
			results <- balanceConnectResult[T]{conn: conn, member: member, index: i, err: err, release: func() { cancel(); release(false) }}
		}()
		return true
	}
	stagger := time.NewTicker(200 * time.Millisecond)
	defer stagger.Stop()
	timer := time.NewTimer(max(time.Until(deadline), 0))
	defer timer.Stop()
	var last error
	for {
		if err := ctx.Err(); err != nil {
			return zero, nil, nil, err
		}
		if len(pending) == 0 && !start() {
			if last == nil {
				last = errors.New("no available load balance node")
			}
			return zero, nil, nil, last
		}
		select {
		case <-ctx.Done():
			return zero, nil, nil, ctx.Err()
		case <-timer.C:
			return zero, nil, nil, context.DeadlineExceeded
		case <-stagger.C:
			if len(pending) < parallel {
				start()
			}
		case r := <-results:
			delete(pending, r.index)
			if r.err == nil {
				return r.conn, r.member, r.release, nil
			}
			last = r.err
			start()
		}
	}
}

// A UDP association stays on one node for its lifetime; never stripe packets
// of an existing QUIC/UDP session across different public source addresses.
func (s *LoadBalance) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	conn, member, release, err := balanceConnect(s, ctx, N.NetworkUDP, func(attemptCtx context.Context, budget time.Duration, member adapter.Outbound) (net.PacketConn, error) {
		return balancePacketAttempt(attemptCtx, budget, member, destination)
	})
	if err != nil {
		return nil, err
	}
	s.record(ctx, member)
	return &balancePacketConn{PacketConn: conn, release: release}, nil
}

// Retire the dialing timer after success, retaining the context until Close:
// cancelling it on return breaks transports with deferred handshakes.
func balancePacketAttempt(parent context.Context, timeout time.Duration, member adapter.Outbound, destination M.Socksaddr) (net.PacketConn, error) {
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
	conn, err := member.ListenPacket(ctx, destination)
	ctx.access.Lock()
	ctx.active = false
	expired := ctx.expired
	timer.Stop()
	ctx.access.Unlock()
	if err == nil && !expired && parent.Err() == nil {
		return &balancePacketConn{PacketConn: conn, release: cancel}, nil
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

type balancePacketConn struct {
	net.PacketConn
	once    sync.Once
	release func()
}

func (c *balancePacketConn) Close() error { c.once.Do(c.release); return c.PacketConn.Close() }

// Healthy members keep their configured interval. Fault/recovery confirmation
// has its own five-second cadence and shares the existing global probe budget.
func (s *LoadBalance) recoveryInterval() time.Duration {
	if s.interval <= 0 {
		return 5 * time.Second
	}
	return min(s.interval, 5*time.Second)
}

func (s *LoadBalance) checkInterval(m *balanceMember) time.Duration {
	if m.state != balanceHealthy || m.failures > 0 || !m.failed[0].IsZero() || !m.failed[1].IsZero() {
		return s.recoveryInterval()
	}
	return s.interval
}

// reserveCheck returns the earliest due member and marks it in flight. A slow
// member cannot hold a whole batch open or postpone a survivor's next check.
func (s *LoadBalance) reserveCheck(now time.Time) (int, time.Duration) {
	s.access.Lock()
	defer s.access.Unlock()
	wait := 24 * time.Hour
	if s.closed {
		return -1, wait
	}
	index := -1
	for i := range s.members {
		m := &s.members[i]
		if m.checking {
			continue
		}
		if !m.nextCheck.After(now) {
			if index < 0 || m.nextCheck.Before(s.members[index].nextCheck) {
				index = i
			}
		} else {
			wait = min(wait, m.nextCheck.Sub(now))
		}
	}
	if index >= 0 {
		s.members[index].checking = true
		s.members[index].lastRecheck = now
		return index, 0
	}
	return -1, wait
}

func (s *LoadBalance) loop(ctx context.Context) {
	workers := min(4, len(s.members))
	if workers == 0 {
		return
	}
	jobs := make(chan balanceHealthJob, workers)
	done := make(chan struct{}, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case job, ok := <-jobs:
					if !ok {
						return
					}
					if job.target != nil {
						s.checkTarget(ctx, job.target)
					} else {
						s.checkMember(ctx, job.index)
					}
					select {
					case done <- struct{}{}:
					case <-ctx.Done():
						return
					}
				}
			}
		}()
	}
	defer func() { close(jobs); wg.Wait() }()
	timer := time.NewTimer(0)
	defer timer.Stop()
	running := 0
	for {
		if ctx.Err() != nil {
			return
		}
		wait := 24 * time.Hour
		for running < workers {
			job, delay := s.reserveHealthJob(time.Now())
			if job == nil {
				wait = delay
				break
			}
			jobs <- *job
			running++
		}
		// When all workers are occupied, completion wakes the scheduler. Do not
		// spin on an already-due member that currently has no available worker.
		timer.Reset(max(wait, time.Millisecond))
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		case <-s.wake:
		case <-done:
			running--
		}
	}
}

func (s *LoadBalance) checkMember(ctx context.Context, index int) {
	result := urltest.ProbeResult{Err: context.Canceled}
	if s.history != nil && ctx.Err() == nil {
		s.access.Lock()
		m := &s.members[index]
		member := m.outbound
		failedAt := m.failed[0]
		if m.failed[1].After(failedAt) {
			failedAt = m.failed[1]
		}
		force := m.state == balanceDown || m.state == balanceRecovering || m.failures > 0 || failedAt.After(m.checked)
		since := m.checked
		if failedAt.After(since) {
			since = failedAt
		}
		interval := s.checkInterval(m)
		s.access.Unlock()
		probeCtx := ctx
		if force {
			probeCtx = urltest.WithNewerThan(ctx, since)
		}
		result = s.history.Probe(probeCtx, member.Tag(), urltest.ProbeLink(s.link, s.expected), interval, force, func(probeCtx context.Context) (uint16, error) {
			if ctx.Err() != nil {
				return 0, context.Canceled
			}
			if current, ok := s.manager.Outbound(member.Tag()); !ok || current != member {
				return 0, context.Canceled
			}
			probeCtx, cancel := context.WithTimeout(probeCtx, s.timeout)
			defer cancel()
			return urltest.URLTestExpected(probeCtx, s.link, s.expected, member)
		})
	}
	s.finishCheck(index, result, time.Now())
}

func (s *LoadBalance) finishCheck(index int, result urltest.ProbeResult, now time.Time) {
	s.access.Lock()
	defer s.access.Unlock()
	m := &s.members[index]
	m.checking = false
	if s.closed {
		return
	}
	// Re-reading one cached result must not count as two recovery successes or
	// two failures. A pass older than the last dial failure cannot readmit it.
	fresh := !result.Time.IsZero() && result.Time.UnixMilli() > m.checked.UnixMilli()
	ok := result.Err == nil && result.Delay > 0
	if ok && (result.Time.UnixMilli() <= m.failed[0].UnixMilli() || result.Time.UnixMilli() <= m.failed[1].UnixMilli()) {
		fresh = false
	}
	if fresh && !errors.Is(result.Err, context.Canceled) {
		m.checked = result.Time
		if ok {
			if m.delay == 0 {
				m.delay = uint32(result.Delay)
			} else {
				m.delay = (m.delay*3 + uint32(result.Delay)) / 4
			}
			m.failures = 0
			m.successes = min(m.successes+1, balanceRise)
			switch m.state {
			case balanceDown:
				m.state, m.successes = balanceRecovering, 1
			case balanceRecovering:
				if m.successes >= balanceRise {
					m.state = balanceHealthy
				}
			default:
				m.state = balanceHealthy
			}
			if m.state == balanceHealthy {
				m.failed = [2]time.Time{}
			}
		} else {
			m.successes = 0
			m.failures = min(m.failures+1, balanceFall)
			// An untested or recovering member is never admitted on a failure.
			// A known healthy member gets one fast confirmation against jitter.
			if m.state != balanceHealthy || m.failures >= balanceFall {
				m.state = balanceDown
			}
		}
	}
	interval := s.checkInterval(m)
	m.nextCheck = m.checked.Add(interval)
	if !m.nextCheck.After(now) {
		m.nextCheck = now.Add(interval)
	}
}
