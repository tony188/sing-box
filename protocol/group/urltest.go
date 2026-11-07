package group

import (
	"context"
	"io"
	"maps"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/interrupt"
	"github.com/sagernet/sing-box/common/urltest"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/x/list"
	"github.com/sagernet/sing/service"
	"github.com/sagernet/sing/service/pause"
)

func RegisterURLTest(registry *outbound.Registry) {
	outbound.Register[option.URLTestOutboundOptions](registry, C.TypeURLTest, NewURLTest)
}

var (
	_ adapter.OutboundGroup           = (*URLTest)(nil)
	_ adapter.InterfaceUpdateListener = (*URLTest)(nil)
	_ adapter.Referrer                = (*URLTest)(nil)
)

type URLTest struct {
	outbound.Adapter
	ctx                          context.Context
	outbound                     adapter.OutboundManager
	logger                       log.ContextLogger
	tags                         []string
	link                         string
	expected                     urltest.ExpectedStatus
	interval                     time.Duration
	tolerance                    uint16
	idleTimeout                  time.Duration
	group                        *URLTestGroup
	checkAccess                  sync.Mutex
	interruptExternalConnections bool
}

func NewURLTest(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.URLTestOutboundOptions) (adapter.Outbound, error) {
	expected, err := urltest.ParseExpectedStatus(options.ExpectedStatus)
	if err != nil {
		return nil, E.Cause(err, "parse expected_status")
	}
	outbound := &URLTest{
		Adapter:                      outbound.NewAdapter(C.TypeURLTest, tag, []string{N.NetworkTCP, N.NetworkUDP}, options.Outbounds),
		ctx:                          ctx,
		outbound:                     service.FromContext[adapter.OutboundManager](ctx),
		logger:                       logger,
		tags:                         options.Outbounds,
		link:                         options.URL,
		expected:                     expected,
		interval:                     time.Duration(options.Interval),
		tolerance:                    options.Tolerance,
		idleTimeout:                  time.Duration(options.IdleTimeout),
		interruptExternalConnections: options.InterruptExistConnections,
	}
	if len(outbound.tags) == 0 {
		return nil, E.New("missing tags")
	}
	for i, tag := range outbound.tags {
		if slices.Contains(outbound.tags[:i], tag) {
			return nil, E.New("duplicate tag: ", tag)
		}
	}
	return outbound, nil
}

func (s *URLTest) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	switch stage {
	case adapter.StartStateStart:
		outbounds := make([]adapter.Outbound, 0, len(s.tags))
		for i, tag := range s.tags {
			detour, loaded := s.outbound.Outbound(tag)
			if !loaded {
				return E.New("outbound ", i, " not found: ", tag)
			}
			outbounds = append(outbounds, detour)
		}
		group, err := NewURLTestGroup(s.ctx, s.outbound, s.logger, outbounds, s.link, s.expected, s.interval, s.tolerance, s.idleTimeout, s.interruptExternalConnections)
		if err != nil {
			return err
		}
		s.group = group
	case adapter.StartStateStarted:
		s.group.PostStart()
		scope.Add(s.group.Close)
	}
	return nil
}

func (s *URLTest) All() []string {
	return s.tags
}

func (s *URLTest) Selected(network string) adapter.Outbound {
	var outbound adapter.Outbound
	if network == N.NetworkUDP {
		outbound = s.group.selectedOutboundUDP
	} else {
		outbound = s.group.selectedOutboundTCP
	}
	if outbound == nil {
		outbound, _ = s.group.Select(network)
	}
	return outbound
}

func (s *URLTest) AttachConnection(closer io.Closer) func() {
	s.group.Touch()
	return s.group.interruptGroup.Add(closer, true)
}

func (s *URLTest) References() []string {
	group := s.group
	if group == nil {
		return nil
	}
	var references []string
	if group.selectedOutboundTCP != nil {
		references = append(references, group.selectedOutboundTCP.Tag())
	}
	if group.selectedOutboundUDP != nil && group.selectedOutboundUDP != group.selectedOutboundTCP {
		references = append(references, group.selectedOutboundUDP.Tag())
	}
	return references
}

func (s *URLTest) URLTest(ctx context.Context) (map[string]uint16, error) {
	return s.group.URLTest(ctx)
}

// URLTestWithForce lets the panel scheduler request a group check while still
// honoring sing-box's per-node interval cache. The regular URLTest API remains
// a forced check for interactive/manual requests. memberTimeout bounds each
// member's own probe (0 keeps C.TCPTimeout); it is the panel's latency test
// timeout, so a dead member costs what the user configured, not 15 seconds.
func (s *URLTest) URLTestWithForce(ctx context.Context, force bool, memberTimeout time.Duration) (map[string]uint16, error) {
	return s.group.urlTest(ctx, force, memberTimeout)
}

func (s *URLTest) CheckOutbounds() {
	s.group.CheckOutbounds(s.ctx, true)
}

func (s *URLTest) PerformUpdateCheck() {
	s.group.performUpdateCheck()
}

func (s *URLTest) InterfaceUpdated(ctx context.Context) {
	group := s.group
	if group == nil {
		return
	}
	if group.pause.IsDevicePaused() || group.pause.IsNetworkPaused() {
		return
	}
	go func() {
		s.checkAccess.Lock()
		defer s.checkAccess.Unlock()
		if ctx.Err() != nil {
			return
		}
		group.CheckOutbounds(ctx, true)
	}()
}

func (s *URLTest) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	s.group.Touch()
	var outbound adapter.Outbound
	switch N.NetworkName(network) {
	case N.NetworkTCP:
		outbound = s.group.selectedOutboundTCP
	case N.NetworkUDP:
		outbound = s.group.selectedOutboundUDP
	default:
		return nil, E.Extend(N.ErrUnknownNetwork, network)
	}
	if outbound == nil {
		outbound, _ = s.group.Select(network)
	}
	if outbound == nil {
		return nil, E.New("missing supported outbound")
	}
	conn, err := outbound.DialContext(ctx, network, destination)
	if err == nil {
		return s.group.interruptGroup.NewConn(conn, interrupt.IsExternalConnectionFromContext(ctx)), nil
	}
	s.logger.ErrorContext(ctx, err)
	s.group.confirmFailure(outbound, network)
	return nil, err
}

func (s *URLTest) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	s.group.Touch()
	outbound := s.group.selectedOutboundUDP
	if outbound == nil {
		outbound, _ = s.group.Select(N.NetworkUDP)
	}
	if outbound == nil {
		return nil, E.New("missing supported outbound")
	}
	conn, err := outbound.ListenPacket(ctx, destination)
	if err == nil {
		return s.group.interruptGroup.NewPacketConn(conn, interrupt.IsExternalConnectionFromContext(ctx)), nil
	}
	s.logger.ErrorContext(ctx, err)
	s.group.confirmFailure(outbound, N.NetworkUDP)
	return nil, err
}

type URLTestGroup struct {
	ctx                          context.Context
	outbound                     adapter.OutboundManager
	pause                        pause.Manager
	pauseCallback                *list.Element[pause.Callback]
	logger                       log.Logger
	outbounds                    []adapter.Outbound
	link                         string
	expected                     urltest.ExpectedStatus
	interval                     time.Duration
	tolerance                    uint16
	idleTimeout                  time.Duration
	history                      *urltest.HistoryStorage
	checkAccess                  sync.Mutex
	checkRunning                 *groupCheck
	selectedOutboundTCP          adapter.Outbound
	selectedOutboundUDP          adapter.Outbound
	interruptGroup               *interrupt.Group
	interruptExternalConnections bool
	access                       sync.Mutex
	updateAccess                 sync.Mutex
	ticker                       *time.Ticker
	close                        chan struct{}
	started                      bool
	lastActive                   common.TypedValue[time.Time]
	unregisterConfirm            func()
}

func NewURLTestGroup(ctx context.Context, outboundManager adapter.OutboundManager, logger log.Logger, outbounds []adapter.Outbound, link string, expected urltest.ExpectedStatus, interval time.Duration, tolerance uint16, idleTimeout time.Duration, interruptExternalConnections bool) (*URLTestGroup, error) {
	if interval == 0 {
		interval = C.DefaultURLTestInterval
	}
	if tolerance == 0 {
		tolerance = 50
	}
	if idleTimeout == 0 {
		idleTimeout = C.DefaultURLTestIdleTimeout
	}
	if interval > idleTimeout {
		return nil, E.New("interval must be less or equal than idle_timeout")
	}
	history := service.PtrFromContext[urltest.HistoryStorage](ctx)
	if history == nil {
		return nil, E.New("missing URL test history storage")
	}
	return &URLTestGroup{
		ctx:                          ctx,
		outbound:                     outboundManager,
		logger:                       logger,
		outbounds:                    outbounds,
		link:                         link,
		expected:                     expected,
		interval:                     interval,
		tolerance:                    tolerance,
		idleTimeout:                  idleTimeout,
		history:                      history,
		close:                        make(chan struct{}),
		pause:                        service.FromContext[pause.Manager](ctx),
		interruptGroup:               interrupt.NewGroup(),
		interruptExternalConnections: interruptExternalConnections,
	}, nil
}

func (g *URLTestGroup) PostStart() {
	g.access.Lock()
	defer g.access.Unlock()
	g.started = true
	g.lastActive.Store(time.Now())
	if g.unregisterConfirm == nil {
		g.unregisterConfirm = g.history.RegisterGroup(g)
	}
	go g.CheckOutbounds(g.ctx, false)
}

func (g *URLTestGroup) Touch() {
	if !g.started {
		return
	}
	g.access.Lock()
	defer g.access.Unlock()
	if g.ticker != nil {
		g.lastActive.Store(time.Now())
		return
	}
	ticker := time.NewTicker(g.interval)
	g.ticker = ticker
	g.pauseCallback = pause.RegisterTicker(g.pause, ticker, g.interval, nil)
	go g.loopCheck(ticker, g.close)
}

func (g *URLTestGroup) Close() error {
	g.access.Lock()
	defer g.access.Unlock()
	if g.unregisterConfirm != nil {
		g.unregisterConfirm()
		g.unregisterConfirm = nil
	}
	if g.ticker == nil {
		return nil
	}
	g.ticker.Stop()
	g.ticker = nil
	g.pause.UnregisterCallback(g.pauseCallback)
	g.pauseCallback = nil
	close(g.close)
	return nil
}

func (g *URLTestGroup) Select(network string) (adapter.Outbound, bool) {
	var minDelay uint16
	var minOutbound adapter.Outbound
	switch network {
	case N.NetworkTCP:
		if g.selectedOutboundTCP != nil {
			if history := g.history.LoadProbeHistory(RealTag(g.selectedOutboundTCP, N.NetworkTCP), g.ProbeLink()); history != nil {
				minOutbound = g.selectedOutboundTCP
				minDelay = history.Delay
			}
		}
	case N.NetworkUDP:
		if g.selectedOutboundUDP != nil {
			if history := g.history.LoadProbeHistory(RealTag(g.selectedOutboundUDP, N.NetworkUDP), g.ProbeLink()); history != nil {
				minOutbound = g.selectedOutboundUDP
				minDelay = history.Delay
			}
		}
	}
	for _, detour := range g.outbounds {
		if !common.Contains(detour.Network(), network) {
			continue
		}
		history := g.history.LoadProbeHistory(RealTag(detour, network), g.ProbeLink())
		if history == nil {
			continue
		}
		if minDelay == 0 || minDelay > history.Delay+g.tolerance {
			minDelay = history.Delay
			minOutbound = detour
		}
	}
	if minOutbound == nil {
		for _, detour := range g.outbounds {
			if !common.Contains(detour.Network(), network) {
				continue
			}
			return detour, false
		}
		return nil, false
	}
	return minOutbound, true
}

func (g *URLTestGroup) loopCheck(ticker *time.Ticker, closeChan <-chan struct{}) {
	if time.Since(g.lastActive.Load()) > g.interval {
		g.lastActive.Store(time.Now())
		g.CheckOutbounds(g.ctx, false)
	}
	for {
		select {
		case <-closeChan:
			return
		case <-ticker.C:
		}
		if time.Since(g.lastActive.Load()) > g.idleTimeout {
			g.access.Lock()
			if g.ticker == ticker {
				g.ticker.Stop()
				g.ticker = nil
				g.pause.UnregisterCallback(g.pauseCallback)
				g.pauseCallback = nil
			}
			g.access.Unlock()
			return
		}
		g.CheckOutbounds(g.ctx, false)
	}
}

func (g *URLTestGroup) CheckOutbounds(ctx context.Context, force bool) {
	_, _ = g.urlTest(ctx, force, 0)
}

func (g *URLTestGroup) URLTest(ctx context.Context) (map[string]uint16, error) {
	return g.urlTest(ctx, true, 0)
}

// groupCheck is one run of a group check; callers arriving while it runs wait for
// it (Open-Box tcp14).
type groupCheck struct {
	done     chan struct{}
	result   map[string]uint16
	complete bool
}

func (g *URLTestGroup) urlTest(ctx context.Context, force bool, memberTimeout time.Duration) (map[string]uint16, error) {
	for {
		g.checkAccess.Lock()
		running := g.checkRunning
		if running == nil {
			break
		}
		g.checkAccess.Unlock()
		// Open-Box (tcp14): upstream answered a check requested while another one ran
		// with an empty result, which a caller cannot tell from "every member failed"
		// (the panel's scheduler then recorded all due members as timed out). Wait for
		// the running check and answer with its results; it probed every member (or
		// reused results within the interval, as this call would have). If that check
		// was cut short (its request ended), run one ourselves.
		select {
		case <-running.done:
		case <-ctx.Done():
			return make(map[string]uint16), ctx.Err()
		}
		if running.complete {
			return maps.Clone(running.result), nil
		}
	}
	check := &groupCheck{done: make(chan struct{})}
	g.checkRunning = check
	g.checkAccess.Unlock()
	result := URLTestOutbounds(ctx, g.outbound, g.history, g.logger, g.outbounds, g.link, g.expected, g.interval, force, memberTimeout)
	g.performUpdateCheck()
	g.checkAccess.Lock()
	check.result = result
	check.complete = ctx.Err() == nil
	g.checkRunning = nil
	close(check.done)
	g.checkAccess.Unlock()
	return result, nil
}

type urlTestResult struct {
	delay uint16
	err   error
}

type urlTestBatch struct {
	ctx           context.Context
	outbound      adapter.OutboundManager
	history       *urltest.HistoryStorage
	logger        log.Logger
	tasks         []func()
	checked       map[string]bool
	groups        []adapter.OutboundGroup
	access        sync.Mutex
	result        map[string]uint16
	memberTimeout time.Duration
}

// memberTimeout bounds each member's own probe; 0 means C.TCPTimeout (the kernel's
// own background checks). The Clash API passes the panel's latency test timeout.
// expected: the statuses that count as a pass (Open-Box tcp19); nested URL test
// groups test with their own link and expectation.
func URLTestOutbounds(ctx context.Context, outboundManager adapter.OutboundManager, history *urltest.HistoryStorage, logger log.Logger, outbounds []adapter.Outbound, link string, expected urltest.ExpectedStatus, interval time.Duration, force bool, memberTimeout time.Duration) map[string]uint16 {
	if memberTimeout <= 0 {
		memberTimeout = C.TCPTimeout
	}
	testBatch := &urlTestBatch{
		ctx:           ctx,
		outbound:      outboundManager,
		history:       history,
		logger:        logger,
		checked:       make(map[string]bool),
		result:        make(map[string]uint16),
		memberTimeout: memberTimeout,
	}
	testBatch.test(outbounds, link, expected, interval, force)
	// Open-Box (tcp20): ten workers take the queued checks in turn instead of one goroutine
	// per member parked on a slot (openbox_workers.go); same ten at a time as upstream.
	workers := 10
	if common.LowMemory {
		workers = 4
	}
	runBounded(workers, testBatch.tasks)
	for _, outboundGroup := range testBatch.groups {
		groupHistory := history.LoadProbeHistory(RealTag(outboundGroup, N.NetworkTCP), urltest.ProbeLink(link, expected))
		if groupHistory != nil {
			testBatch.result[outboundGroup.Tag()] = groupHistory.Delay
		}
	}
	return testBatch.result
}

func (b *urlTestBatch) test(outbounds []adapter.Outbound, link string, expected urltest.ExpectedStatus, interval time.Duration, force bool) {
	for _, detour := range outbounds {
		tag := detour.Tag()
		if b.checked[tag] {
			continue
		}
		switch nested := detour.(type) {
		case *URLTest:
			b.checked[tag] = true
			b.groups = append(b.groups, nested)
			b.tasks = append(b.tasks, func() {
				nestedResult, _ := nested.group.urlTest(b.ctx, force, b.memberTimeout)
				if urltest.ProbeLink(link, expected) == nested.group.ProbeLink() {
					b.access.Lock()
					maps.Copy(b.result, nestedResult)
					b.access.Unlock()
				} else {
					// The child's own test does not prove its chosen leaf works for the parent.
					b.probe(nested, link, expected, interval, force)
				}
			})
		case adapter.OutboundGroup:
			b.checked[tag] = true
			b.groups = append(b.groups, nested)
			b.test(common.FilterNotNil(common.Map(nested.All(), func(it string) adapter.Outbound {
				member, _ := b.outbound.Outbound(it)
				return member
			})), link, expected, interval, force)
		default:
			b.checked[tag] = true
			b.tasks = append(b.tasks, func() { b.probe(detour, link, expected, interval, force) })
		}
	}
}

func (b *urlTestBatch) probe(detour adapter.Outbound, link string, expected urltest.ExpectedStatus, interval time.Duration, force bool) {
	tag := RealTag(detour, N.NetworkTCP)
	if leaf, loaded := b.outbound.Outbound(tag); loaded { detour = leaf }
	result := b.history.Probe(b.ctx, tag, urltest.ProbeLink(link, expected), interval, force, func(ctx context.Context) (uint16, error) {
		// Each member gets its own timeout, detached from the request's deadline:
		// with ten members probed at a time a group with many dead members outlives
		// any single request bound. Deriving from the request context made every
		// member still pending fail at once and lose its history, leaving the group
		// nothing to select (observed on a production router: six live members
		// recorded as timed out in one second). A member that fails now really
		// failed within its own timeout.
		// Probe paces this against the kernel-wide budget (common/urltest/pacer.go)
		// before calling here, so the member timeout starts once the slot is ours.
		testCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), b.memberTimeout)
		defer cancel()
		return urltest.URLTestExpected(testCtx, link, expected, detour)
	})
	if result.Err != nil {
		b.logger.Debug("outbound ", tag, " unavailable: ", result.Err)
	} else {
		if !result.Reused {
			b.logger.Debug("outbound ", tag, " available: ", result.Delay, "ms")
		}
		b.access.Lock()
		b.result[tag] = result.Delay
		b.access.Unlock()
	}
}

// SelectedTags and Reselect register the group with the history storage, which
// confirms a failure of a member in use before the group switches (Open-Box,
// see common/urltest/confirm.go).
func (g *URLTestGroup) SelectedTags() []string {
	g.updateAccess.Lock()
	tcp, udp := g.selectedOutboundTCP, g.selectedOutboundUDP
	g.updateAccess.Unlock()
	var tags []string
	if tcp != nil {
		tags = append(tags, RealTag(tcp, N.NetworkTCP))
	}
	if udp != nil && (tcp == nil || RealTag(udp, N.NetworkUDP) != RealTag(tcp, N.NetworkTCP)) {
		tags = append(tags, RealTag(udp, N.NetworkUDP))
	}
	return tags
}

func (g *URLTestGroup) ProbeLink() string {
	return urltest.ProbeLink(g.link, g.expected)
}

func (g *URLTestGroup) Reselect() {
	g.performUpdateCheck()
}

// confirmFailure replaces upstream's "delete the history of the outbound a dial
// just failed through": the member in use is probed again after the confirmation
// delay and only dropped if it fails again.
func (g *URLTestGroup) confirmFailure(outbound adapter.Outbound, network string) {
	link, expected := g.link, g.expected
	tag := RealTag(outbound, network)
	// A nested group may select another leaf during the ten-second delay.
	if leaf, loaded := g.outbound.Outbound(tag); loaded {
		outbound = leaf
	}
	g.history.Failed(tag, urltest.ProbeLink(link, expected), func(ctx context.Context) (uint16, error) {
		probeCtx, cancel := context.WithTimeout(ctx, C.TCPTimeout)
		defer cancel()
		return urltest.URLTestExpected(probeCtx, link, expected, outbound)
	})
}

func (g *URLTestGroup) performUpdateCheck() {
	g.updateAccess.Lock()
	defer g.updateAccess.Unlock()
	var (
		updated  bool
		selected bool
	)
	if outbound, exists := g.Select(N.NetworkTCP); outbound != nil && (g.selectedOutboundTCP == nil || (exists && outbound != g.selectedOutboundTCP)) {
		if g.selectedOutboundTCP != nil {
			updated = true
		}
		g.selectedOutboundTCP = outbound
		selected = true
	}
	if outbound, exists := g.Select(N.NetworkUDP); outbound != nil && (g.selectedOutboundUDP == nil || (exists && outbound != g.selectedOutboundUDP)) {
		if g.selectedOutboundUDP != nil {
			updated = true
		}
		g.selectedOutboundUDP = outbound
		selected = true
	}
	if updated {
		g.interruptGroup.Interrupt(g.interruptExternalConnections)
	}
	if selected {
		g.history.NotifyUpdated()
	}
}
