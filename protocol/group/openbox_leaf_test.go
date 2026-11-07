package group

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

type isolationLeaf struct {
	adapter.Outbound
	tag   string
	calls atomic.Int32
}

func (o *isolationLeaf) Tag() string { return o.tag }
func (o *isolationLeaf) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	o.calls.Add(1)
	return nil, errors.New("unavailable")
}

type isolationManager struct {
	adapter.OutboundManager
	leaves map[string]adapter.Outbound
}

func (m *isolationManager) Outbound(tag string) (adapter.Outbound, bool) {
	o, ok := m.leaves[tag]
	return o, ok
}

type isolationSelector struct {
	adapter.OutboundGroup
	selected atomic.Pointer[isolationLeaf]
}

func (g *isolationSelector) Tag() string                      { return "selector" }
func (g *isolationSelector) Now() string                      { return g.selected.Load().tag }
func (g *isolationSelector) Selected(string) adapter.Outbound { return g.selected.Load() }
func (g *isolationSelector) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	return g.selected.Load().DialContext(ctx, network, destination)
}

type isolationConfirmGroup struct{ link string }

func (g isolationConfirmGroup) SelectedTags() []string { return []string{"a"} }
func (g isolationConfirmGroup) ProbeLink() string      { return g.link }
func (g isolationConfirmGroup) Reselect()              {}

// A nested selector can change during the confirmation delay. Its new leaf must
// never be probed under the original leaf's cache key.
func TestOpenBoxURLTestConfirmLeaf(t *testing.T) {
	oldDelay := urltest.ConfirmDelay
	urltest.ConfirmDelay = 25 * time.Millisecond
	defer func() { urltest.ConfirmDelay = oldDelay }()
	s := urltest.NewHistoryStorage()
	defer s.Close()
	link := "http://check.example/test"
	defer s.RegisterGroup(isolationConfirmGroup{link})()
	s.Probe(context.Background(), "a", link, 0, true, func(context.Context) (uint16, error) { return 10, nil })
	a, b := &isolationLeaf{tag: "a"}, &isolationLeaf{tag: "b"}
	nested := &isolationSelector{}
	nested.selected.Store(a)
	manager := &isolationManager{leaves: map[string]adapter.Outbound{"a": a, "b": b}}
	g := &URLTestGroup{history: s, outbound: manager, link: link}
	g.confirmFailure(nested, N.NetworkTCP)
	nested.selected.Store(b)
	deadline := time.Now().Add(3 * time.Second)
	for s.Confirming("a", link) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.Confirming("a", link) || a.calls.Load() != 1 || b.calls.Load() != 0 {
		t.Fatalf("confirmation changed leaf: a=%d b=%d pending=%v", a.calls.Load(), b.calls.Load(), s.Confirming("a", link))
	}
}
