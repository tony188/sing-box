package group

import (
	"context"
	"errors"
	"github.com/sagernet/sing-box/common/urltest"
	"testing"
	"time"
)

func TestOpenBoxBandwidthLoadBalanceHealthFirstAndOff(t *testing.T) {
	s, _ := balanceFixture(t, 2)
	s.history = urltest.NewHistoryStorage()
	s.bandwidthEnabled = true
	now := time.Now()
	s.history.Bandwidth().Replace(map[string]urltest.BandwidthSample{"node-0": {Mbps: 200, At: now.UnixMilli(), PeriodMs: 7200000}, "node-1": {Mbps: 50, At: now.UnixMilli(), PeriodMs: 7200000}})
	for i := range s.members {
		s.members[i].state = balanceHealthy
		s.members[i].checked = now
		s.members[i].delay = 100
	}
	expected := []int{0, 0, 0, 1}
	for _, want := range expected {
		got, _, _ := s.reserve("tcp", map[int]bool{})
		if got != want {
			t.Fatalf("got %d want %d", got, want)
		}
	}
	s.members[0].state = balanceRecovering
	if got, _, _ := s.reserve("tcp", map[int]bool{}); got != 1 {
		t.Fatal("recovering fast node was selected", got)
	}
	s.members[0].state = balanceDown
	if got, _, _ := s.reserve("tcp", map[int]bool{}); got != 1 {
		t.Fatal("down fast node was selected", got)
	}
	s.members[0].state = balanceHealthy
	s.bandwidthEnabled = false
	s.members[0].active[0] = 0
	s.members[1].active[0] = 1
	if got, _, _ := s.reserve("tcp", map[int]bool{}); got != 0 {
		t.Fatal("disabled mode stopped using least active", got)
	}
}

func TestOpenBoxBandwidthURLTestStableSingleAndHealth(t *testing.T) {
	a, b := &recoveryLeaf{tag: "a"}, &recoveryLeaf{tag: "b"}
	s := recoveryGroup(t, a, b)
	g := s.group
	g.bandwidthEnabled = true
	g.bandwidthTag = "test"
	seedRecovery(s, "a", g.ProbeLink())
	seedRecovery(s, "b", g.ProbeLink())
	now := time.Now()
	state := g.history.Bandwidth()
	state.Replace(map[string]urltest.BandwidthSample{"a": {Mbps: 200, At: now.UnixMilli(), PeriodMs: 7200000}, "b": {Mbps: 50, At: now.UnixMilli(), PeriodMs: 7200000}})
	g.selectedOutboundTCP = nil
	selected, ok := g.Select("tcp")
	if !ok || selected.Tag() != "a" {
		t.Fatal("did not prefer usable bandwidth", selected, ok)
	}
	g.selectedOutboundTCP = a
	for i := 0; i < 100; i++ {
		state.Reserve("a", []string{"test"})
	}
	selected, _ = g.Select("tcp")
	if selected.Tag() != "a" {
		t.Fatal("own load caused oscillation")
	}
	state.Replace(map[string]urltest.BandwidthSample{"a": {Mbps: 200, At: now.UnixMilli(), PeriodMs: 7200000}, "b": {Mbps: 240, At: now.UnixMilli(), PeriodMs: 7200000}})
	g.bandwidthSince[0] = now.Add(-2 * time.Minute)
	selected, _ = g.Select("tcp")
	if selected.Tag() != "a" {
		t.Fatal("switched below 25% advantage")
	}
	state.Replace(map[string]urltest.BandwidthSample{"a": {Mbps: 200, At: now.UnixMilli(), PeriodMs: 7200000}, "b": {Mbps: 300, At: now.UnixMilli(), PeriodMs: 7200000}})
	g.bandwidthSince[0] = now
	selected, _ = g.Select("tcp")
	if selected.Tag() != "a" {
		t.Fatal("minimum hold ignored")
	}
	g.bandwidthSince[0] = now.Add(-2 * time.Minute)
	selected, _ = g.Select("tcp")
	if selected.Tag() != "b" {
		t.Fatal("did not switch for meaningful advantage")
	}
	// Health history absence must win even with a much larger retained sample.
	g.history.DeleteURLTestHistory("b")
	g.history.Probe(context.Background(), "b", g.ProbeLink(), time.Hour, true, func(context.Context) (uint16, error) { return 0, errors.New("down") })
	selected, _ = g.Select("tcp")
	if selected.Tag() != "a" {
		t.Fatal("unhealthy fast node selected")
	}
	g.bandwidthEnabled = false
	selected, _ = g.Select("tcp")
	if selected.Tag() != "a" {
		t.Fatal("disabled mode changed")
	}
}
