package group

import (
	"context"
	"errors"
	"github.com/sagernet/sing-box/adapter"
	O "github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/interrupt"
	"github.com/sagernet/sing-box/common/urltest"
	"github.com/sagernet/sing-box/log"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

type recoveryLeaf struct {
	adapter.Outbound
	tag   string
	calls atomic.Int32
	dial  func(context.Context) (net.Conn, error)
}

func (o *recoveryLeaf) Tag() string       { return o.tag }
func (o *recoveryLeaf) Type() string      { return "shadowsocks" }
func (o *recoveryLeaf) Network() []string { return []string{N.NetworkTCP} }
func (o *recoveryLeaf) DialContext(ctx context.Context, _ string, _ M.Socksaddr) (net.Conn, error) {
	o.calls.Add(1)
	return o.dial(ctx)
}
func recoveryGroup(t *testing.T, leaves ...*recoveryLeaf) *URLTest {
	t.Helper()
	h := urltest.NewHistoryStorage()
	t.Cleanup(func() { h.Close() })
	m := &isolationManager{leaves: map[string]adapter.Outbound{}}
	g := &URLTestGroup{history: h, outbound: m, link: "http://probe.example/test", interval: time.Hour, interruptGroup: interrupt.NewGroup()}
	for _, o := range leaves {
		m.leaves[o.tag] = o
		g.outbounds = append(g.outbounds, o)
	}
	g.selectedOutboundTCP = leaves[0]
	g.selectedOutboundUDP = leaves[0]
	return &URLTest{Adapter: O.NewAdapter("urltest", "test", []string{N.NetworkTCP}, nil), group: g, logger: log.NewNOPFactory().NewLogger("test")}
}
func seedRecovery(g *URLTest, tag string, link string) {
	g.group.history.Probe(context.Background(), tag, link, time.Hour, true, func(context.Context) (uint16, error) { return 30, nil })
}
func downRecovery(context.Context) (net.Conn, error) { return nil, errors.New("connection refused") }
func TestOpenBoxDialFallbackOneRequest(t *testing.T) {
	a := &recoveryLeaf{tag: "a", dial: downRecovery}
	var held context.Context
	peer := make(chan net.Conn, 1)
	b := &recoveryLeaf{tag: "b", dial: func(ctx context.Context) (net.Conn, error) { held = ctx; x, y := net.Pipe(); peer <- y; return x, nil }}
	g := recoveryGroup(t, a, b)
	seedRecovery(g, "b", g.group.ProbeLink())
	metadata := adapter.InboundContext{OutboundChain: []adapter.Outbound{g, a}}
	ctx := adapter.WithContext(context.Background(), &metadata)
	old := dialFallbackTimeout
	dialFallbackTimeout = 60 * time.Millisecond
	t.Cleanup(func() { dialFallbackTimeout = old })
	conn, err := g.DialContext(ctx, N.NetworkTCP, M.ParseSocksaddr("example.org:443"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	remote := <-peer
	defer remote.Close()
	result := make(chan string, 1)
	go func() {
		buf := make([]byte, 4)
		_, err := io.ReadFull(remote, buf)
		if err != nil {
			result <- err.Error()
		} else {
			result <- string(buf)
		}
	}()
	time.Sleep(80 * time.Millisecond)
	if held.Err() != nil {
		t.Fatal("successful lazy connection was cancelled", held.Err())
	}
	if _, err = conn.Write([]byte("once")); err != nil {
		t.Fatal(err)
	}
	if got := <-result; got != "once" {
		t.Fatal(got)
	}
	if a.calls.Load() != 1 || b.calls.Load() != 1 || g.Selected(N.NetworkTCP) != a {
		t.Fatal("unbounded attempts or premature global switch")
	}
	if metadata.OutboundChain[len(metadata.OutboundChain)-1] != b {
		t.Fatal("connection reports the failed primary")
	}
	conn.Close() // twice must not detach nested registrations twice
}
func TestOpenBoxDialFallbackBounds(t *testing.T) {
	old := dialFallbackTimeout
	dialFallbackTimeout = 80 * time.Millisecond
	t.Cleanup(func() { dialFallbackTimeout = old })
	wait := func(ctx context.Context) (net.Conn, error) { <-ctx.Done(); return nil, ctx.Err() }
	a, b := &recoveryLeaf{tag: "a", dial: wait}, &recoveryLeaf{tag: "b", dial: wait}
	g := recoveryGroup(t, a, b)
	seedRecovery(g, "b", g.group.ProbeLink())
	start := time.Now()
	ctx := g.DialFallbackContext(context.Background())
	_, err := g.DialContextWithFallback(ctx, N.NetworkTCP, M.ParseSocksaddr("example.org:443"), a)
	if err == nil || time.Since(start) > 300*time.Millisecond || a.calls.Load() != 1 || b.calls.Load() != 1 {
		t.Fatal("attempt/deadline bound failed", err, a.calls.Load(), b.calls.Load())
	}
	_, _ = g.DialContextWithFallback(ctx, N.NetworkTCP, M.ParseSocksaddr("192.0.2.2:443"), a)
	if a.calls.Load() != 1 || b.calls.Load() != 1 {
		t.Fatal("address fallback multiplied retry budget")
	}
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _ = g.DialContext(cancelCtx, N.NetworkTCP, M.ParseSocksaddr("example.org:443"))
	if a.calls.Load() != 1 {
		t.Fatal("cancelled caller dialled")
	}
}
func TestOpenBoxDialFallbackExactConditionAndSaturation(t *testing.T) {
	a, b := &recoveryLeaf{tag: "a", dial: downRecovery}, &recoveryLeaf{tag: "b", dial: downRecovery}
	g := recoveryGroup(t, a, b)
	seedRecovery(g, "b", "http://other.example/test")
	_, _ = g.DialContext(context.Background(), N.NetworkTCP, M.ParseSocksaddr("example.org:443"))
	if b.calls.Load() != 0 {
		t.Fatal("different test URL proved backup eligibility")
	}
	seedRecovery(g, "b", g.group.ProbeLink())
	for i := 0; i < cap(fallbackDialSlots); i++ {
		fallbackDialSlots <- struct{}{}
	}
	defer func() {
		for i := 0; i < cap(fallbackDialSlots); i++ {
			<-fallbackDialSlots
		}
	}()
	_, _ = g.DialContext(context.Background(), N.NetworkTCP, M.ParseSocksaddr("example.org:443"))
	if b.calls.Load() != 0 {
		t.Fatal("fallback exceeded its own concurrency cap")
	}
}
func TestOpenBoxDialMissingHistoryReselect(t *testing.T) {
	old := urltest.ConfirmDelay
	urltest.ConfirmDelay = 25 * time.Millisecond
	t.Cleanup(func() { urltest.ConfirmDelay = old })
	a, b := &recoveryLeaf{tag: "a", dial: downRecovery}, &recoveryLeaf{tag: "b", dial: downRecovery}
	g := recoveryGroup(t, a, b)
	seedRecovery(g, "b", g.group.ProbeLink())
	defer g.group.history.RegisterGroup(g.group)()
	// Exercise a real failure notification, not the /delay API's update defer.
	g.ConfirmFailure(a, N.NetworkTCP)
	if g.Selected(N.NetworkTCP) != a {
		t.Fatal("transient failure switched immediately")
	}
	deadline := time.Now().Add(3 * time.Second)
	for g.group.history.Confirming("a", g.group.ProbeLink()) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if g.Selected(N.NetworkTCP) != b || a.calls.Load() != 1 {
		t.Fatal("missing scoped history did not complete confirmation/reselection", a.calls.Load())
	}
	snap := g.group.history.Snapshot([]urltest.ProbeCondition{{Tag: "a", URL: g.group.link}})
	c := snap.Results[0].Confirmation
	if c == nil || c.Stage != "done" || c.Outcome != "failed" || c.StartedAt == 0 || c.FinishedAt < c.StartedAt {
		t.Fatal("missing confirmation lifecycle", c)
	}
}

func TestOpenBoxDialReplacedLeafCannotConfirm(t *testing.T) {
	old := &recoveryLeaf{tag: "a", dial: downRecovery}
	fresh := &recoveryLeaf{tag: "a", dial: downRecovery}
	g := recoveryGroup(t, old)
	seedRecovery(g, "a", g.group.ProbeLink())
	defer g.group.history.RegisterGroup(g.group)()
	g.group.outbound.(*isolationManager).leaves["a"] = fresh
	g.ConfirmFailure(old, N.NetworkTCP)
	if g.group.history.Confirming("a", g.group.ProbeLink()) {
		t.Fatal("old dial scheduled confirmation for replacement")
	}
}
