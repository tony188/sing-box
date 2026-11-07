package route

import (
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
	"sync"
)

// Account at the connection lifecycle, preserving the original dialer's
// parallel/packet interfaces and zero-copy paths. LB owns its own reservations.
type bandwidthLease struct {
	access   sync.Mutex
	state    *urltest.BandwidthState
	metadata *adapter.InboundContext
	tag      string
	groups   []string
	closed   bool
}

func (m *ConnectionManager) newBandwidthLease(metadata *adapter.InboundContext) *bandwidthLease {
	l := &bandwidthLease{metadata: metadata}
	if m.urlTestHistory == nil {
		return l
	}
	for _, o := range metadata.OutboundChain {
		if o.Type() == "loadbalance" {
			return l
		}
	}
	if !m.urlTestHistory.Bandwidth().Enabled() {
		return l
	}
	l.state = m.urlTestHistory.Bandwidth()
	l.Resolve()
	return l
}
func (l *bandwidthLease) Resolve() {
	l.access.Lock()
	defer l.access.Unlock()
	if l.state == nil || l.closed {
		return
	}
	tag := ""
	groups := []string{}
	for _, o := range l.metadata.OutboundChain {
		switch o.Type() {
		case "selector", "urltest":
			groups = append(groups, o.Tag())
		case "direct", "block", "loadbalance":
		default:
			tag = o.Tag()
		}
	}
	if tag == l.tag {
		return
	}
	if l.tag != "" {
		l.state.Release(l.tag, l.groups)
	}
	l.tag = tag
	l.groups = groups
	if tag != "" {
		l.state.Reserve(tag, groups)
	}
}
func (l *bandwidthLease) Close() {
	l.access.Lock()
	defer l.access.Unlock()
	if l.closed {
		return
	}
	l.closed = true
	if l.state != nil && l.tag != "" {
		l.state.Release(l.tag, l.groups)
	}
}
