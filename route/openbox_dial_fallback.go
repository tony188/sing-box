package route

import (
	"context"
	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"net"
)

type urlTestFallback interface {
	DialFallbackContext(context.Context) context.Context
	DialContextWithFallback(context.Context, string, M.Socksaddr, adapter.Outbound) (net.Conn, error)
}
type fallbackDialer struct {
	N.Dialer
	group urlTestFallback
	leaf  adapter.Outbound
}

func (d *fallbackDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	return d.group.DialContextWithFallback(ctx, network, destination, d.leaf)
}
func hasURLTestFallback(chain []adapter.Outbound) bool {
	for _, o := range chain {
		if _, ok := o.(urlTestFallback); ok {
			return true
		}
	}
	return false
}
func urlTestFallbackDialer(original N.Dialer, chain []adapter.Outbound) N.Dialer {
	if len(chain) == 0 {
		return original
	}
	for _, o := range chain {
		if group, ok := o.(urlTestFallback); ok {
			return &fallbackDialer{Dialer: original, group: group, leaf: chain[len(chain)-1]}
		}
	}
	return original
}

func (d *fallbackDialer) fallbackContext(ctx context.Context) context.Context {
	return d.group.DialFallbackContext(ctx)
}
