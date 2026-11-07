package urltest

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestOpenBoxBandwidthWeightsNeutralAndExpiry(t *testing.T) {
	h := NewHistoryStorage()
	s := h.Bandwidth()
	now := time.Now()
	samples := map[string]BandwidthSample{"a": {200, now.UnixMilli(), 7200000}, "b": {50, now.UnixMilli(), 7200000}}
	s.Replace(samples)
	w, ok := s.Weights(map[string]uint32{"a": 100, "b": 100, "unknown": 100}, now)
	if !ok || w["a"] != 200 || w["b"] != 50 || w["unknown"] != 125 {
		t.Fatal(w, ok)
	}
	if _, ok = s.Weights(map[string]uint32{"a": 100, "b": 100}, now.Add(4*time.Hour)); ok {
		t.Fatal("expired history still influences selection")
	}
	s.Replace(map[string]BandwidthSample{"a": {200, now.UnixMilli(), 7200000}})
	w, _ = s.Weights(map[string]uint32{"a": 1, "b": 100000, "c": 100}, now)
	if w["b"] != 100 || w["a"] != 250 {
		t.Fatal("latency adjustment is not bounded", w)
	}
}
func TestOpenBoxBandwidthAtomicSharedReservations(t *testing.T) {
	s := NewHistoryStorage().Bandwidth()
	tags := []string{}
	weights := map[string]float64{}
	for i := 0; i < 10; i++ {
		tag := fmt.Sprint(i)
		tags = append(tags, tag)
		weights[tag] = 50
	}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); s.Pick(tags, weights, "group") }()
	}
	wg.Wait()
	for _, tag := range tags {
		if s.active[tag] != 10 {
			t.Fatalf("parallel groups piled onto %s: %v", tag, s.active)
		}
	}
	if s.External("0", "group") != 0 {
		t.Fatal("own connections counted as external load")
	}
	s.Reserve("0", []string{"other"})
	if s.External("0", "group") != 1 {
		t.Fatal("external load missing")
	}
}

func TestOpenBoxBandwidthLoadBalancePreservesEqualCapacity(t *testing.T) {
	for _, delays := range [][]uint32{{0, 1, 3, 4, 4, 4, 4, 4, 17, 3}, {20, 35, 55, 80, 120, 200, 300, 500, 800, 1200}} {
		s := NewHistoryStorage().Bandwidth()
		now := time.Now()
		samples := map[string]BandwidthSample{}
		latencies := map[string]uint32{}
		tags := []string{}
		for i, delay := range delays {
			tag := fmt.Sprint(i)
			tags = append(tags, tag)
			latencies[tag] = delay
			samples[tag] = BandwidthSample{50, now.UnixMilli(), 7200000}
		}
		s.Replace(samples)
		weights, ok := s.LoadBalanceWeights(latencies, now)
		if !ok {
			t.Fatal("fresh measurements ignored")
		}
		for range tags {
			if s.Pick(tags, weights, "balance") < 0 {
				t.Fatal("no healthy candidate")
			}
		}
		for _, tag := range tags {
			if s.active[tag] != 1 {
				t.Fatalf("RTT skew stranded equal bandwidth: latencies=%v active=%v weights=%v", delays, s.active, weights)
			}
		}
	}
}

func TestOpenBoxBandwidthLoadBalanceKeepsBandwidthRatio(t *testing.T) {
	s := NewHistoryStorage().Bandwidth()
	now := time.Now()
	s.Replace(map[string]BandwidthSample{"fast": {200, now.UnixMilli(), 7200000}, "slow": {50, now.UnixMilli(), 7200000}})
	w, _ := s.LoadBalanceWeights(map[string]uint32{"fast": 100, "slow": 100}, now)
	for range 20 {
		s.Pick([]string{"fast", "slow"}, w, "balance")
	}
	if s.active["fast"] != 16 || s.active["slow"] != 4 {
		t.Fatal(s.active)
	}
	if _, ok := s.LoadBalanceWeights(map[string]uint32{"fast": 100}, now.Add(4*time.Hour)); ok {
		t.Fatal("expired sample still affects balancing")
	}
}

func TestOpenBoxBandwidthAliasesShareLoad(t *testing.T) {
	s := NewHistoryStorage().Bandwidth()
	s.SetAliases(map[string]string{"a": "same", "alias": "same", "b": "other"})
	s.Reserve("a", []string{"g1"})
	if s.External("alias", "g2") != 1 {
		t.Fatal("alias ignored existing load")
	}
	if s.External("alias", "g1") != 0 {
		t.Fatal("alias counted own traffic")
	}
	if got := s.Pick([]string{"alias", "b"}, map[string]float64{"alias": 50, "b": 50}, "g2"); got != 1 {
		t.Fatal("alias bypassed shared occupancy")
	}
	s.Release("a", []string{"g1"})
	s.Release("b", []string{"g2"})
	if len(s.active) != 0 || len(s.groups) != 0 {
		t.Fatal("released reservations retained")
	}
}
