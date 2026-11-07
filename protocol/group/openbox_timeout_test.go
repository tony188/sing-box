package group

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json/badoption"
)

// Exercise the actual group -> pacer -> HTTP probe -> outbound dial path. This
// fails with the old 15-second default, without waiting for a real network timeout.
func TestOpenBoxURLTestProbeTimeout(t *testing.T) {
	for _, tc := range []struct {
		name           string
		override, want time.Duration
	}{
		{"background", 0, 5 * time.Second},
		{"manual-longer", 7 * time.Second, 7 * time.Second},
		{"manual-shorter", time.Second, time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deadlines := make(chan time.Duration, 1)
			leaf := &balanceTestLeaf{tag: "timeout-node", dial: func(ctx context.Context) (net.Conn, error) {
				deadline, ok := ctx.Deadline()
				if !ok {
					return nil, errors.New("missing probe deadline")
				}
				deadlines <- time.Until(deadline)
				return nil, errors.New("intentional test refusal")
			}}
			manager := &isolationManager{leaves: map[string]adapter.Outbound{leaf.Tag(): leaf}}
			history := urltest.NewHistoryStorage()
			defer history.Close()
			URLTestOutbounds(context.Background(), manager, history, log.NewNOPFactory().NewLogger("test"), []adapter.Outbound{leaf}, "http://probe.invalid/", urltest.ExpectedStatus{}, time.Minute, true, tc.override)
			select {
			case got := <-deadlines:
				if got > tc.want || got < tc.want-250*time.Millisecond {
					t.Fatalf("dial deadline %v, want %v", got, tc.want)
				}
			default:
				t.Fatal("probe did not reach the outbound")
			}
		})
	}
	if C.TCPTimeout != 15*time.Second {
		t.Fatal("application TCP timeout was changed")
	}
}

func TestOpenBoxLoadBalanceDefaultProbeSettings(t *testing.T) {
	for _, interval := range []time.Duration{0, 30 * time.Second} {
		group, err := NewLoadBalance(context.Background(), nil, log.NewNOPFactory().NewLogger("test"), "lb", option.LoadBalanceOutboundOptions{Outbounds: []string{"node"}, Interval: badoption.Duration(interval)})
		if err != nil {
			t.Fatal(err)
		}
		balance := group.(*LoadBalance)
		want := interval
		if want == 0 {
			want = 120 * time.Second
		}
		if balance.interval != want || balance.timeout != 5*time.Second {
			t.Fatalf("interval=%v timeout=%v", balance.interval, balance.timeout)
		}
	}
}
