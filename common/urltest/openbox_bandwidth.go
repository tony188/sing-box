package urltest

import (
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// Measurements are selection hints, never health results. State belongs to one
// kernel history, not a global registry shared by unrelated cores.
type BandwidthSample struct {
	Mbps     float64 `json:"mbps"`
	At       int64   `json:"at"`
	PeriodMs int64   `json:"periodMs"`
}
type BandwidthState struct {
	enabled    atomic.Bool
	aliases    map[string]string
	access     sync.Mutex
	samples    map[string]BandwidthSample
	active     map[string]int
	groups     map[string]map[string]int
	monitoring map[string]ProbeCondition
}

func (s *BandwidthState) SetMonitoring(conditions []ProbeCondition) {
	s.access.Lock()
	defer s.access.Unlock()
	s.monitoring = make(map[string]ProbeCondition, len(conditions))
	for _, c := range conditions {
		s.monitoring[c.Tag] = c
	}
}
func (s *BandwidthState) Monitoring(tag string) (ProbeCondition, bool) {
	s.access.Lock()
	defer s.access.Unlock()
	c, ok := s.monitoring[tag]
	return c, ok
}

func (s *HistoryStorage) Bandwidth() *BandwidthState {
	s.bandwidthOnce.Do(func() { s.bandwidth = &BandwidthState{active: map[string]int{}, groups: map[string]map[string]int{}} })
	return s.bandwidth
}
func (s *BandwidthState) SetAliases(aliases map[string]string) {
	s.access.Lock()
	defer s.access.Unlock()
	for tag, id := range s.aliases {
		if _, ok := aliases[tag]; !ok && s.active[tag] > 0 {
			aliases[tag] = id
		}
	}
	s.aliases = aliases
}
func (s *BandwidthState) identityLocked(tag string) string {
	if id := s.aliases[tag]; id != "" {
		return id
	}
	return tag
}
func (s *BandwidthState) HasFresh(tag string, now time.Time) bool {
	s.access.Lock()
	defer s.access.Unlock()
	v := s.samples[tag]
	age := now.UnixMilli() - v.At
	return v.Mbps > 0 && v.PeriodMs > 0 && age >= 0 && age < 2*v.PeriodMs
}
func (s *BandwidthState) Replace(samples map[string]BandwidthSample) {
	s.access.Lock()
	defer s.access.Unlock()
	s.samples = samples
}
func bandwidthMedian(v []float64) float64 {
	slices.Sort(v)
	n := len(v)
	if n == 0 {
		return 0
	}
	if n%2 == 0 {
		return (v[n/2-1] + v[n/2]) / 2
	}
	return v[n/2]
}

// Missing measurements use a neutral median. Old samples fade to that median
// over the second period; with none left, preserve the original algorithm.
func (s *BandwidthState) Weights(delays map[string]uint32, now time.Time) (map[string]float64, bool) {
	return s.weights(delays, now, false)
}

// A load balancer assigns capacity to many simultaneous connections. Unlike a
// single-node selector, RTT must not outweigh an idle node's entire bandwidth
// slot: the previous 0.5..1.25 multiplier could put two transfers on one equal-
// capacity member while another stayed idle. Sub-20ms probe jitter is neutral;
// larger RTT differences adjust allocation by at most ten percent.
func (s *BandwidthState) LoadBalanceWeights(delays map[string]uint32, now time.Time) (map[string]float64, bool) {
	return s.weights(delays, now, true)
}

func (s *BandwidthState) weights(delays map[string]uint32, now time.Time, balance bool) (map[string]float64, bool) {
	s.access.Lock()
	defer s.access.Unlock()
	bs, rs := []float64{}, []float64{}
	confidence := map[string]float64{}
	for tag, r := range delays {
		if r > 0 {
			rs = append(rs, float64(r))
		}
		v := s.samples[tag]
		age := float64(now.UnixMilli() - v.At)
		if v.Mbps <= 0 || math.IsNaN(v.Mbps) || math.IsInf(v.Mbps, 0) || v.PeriodMs <= 0 || age < 0 || age >= 2*float64(v.PeriodMs) {
			continue
		}
		confidence[tag] = math.Min(1, 2-age/float64(v.PeriodMs))
		bs = append(bs, v.Mbps)
	}
	baseline, ref := bandwidthMedian(bs), bandwidthMedian(rs)
	if baseline == 0 {
		return nil, false
	}
	weights := map[string]float64{}
	for tag, r := range delays {
		b := baseline
		if c := confidence[tag]; c > 0 {
			b = c*s.samples[tag].Mbps + (1-c)*baseline
		}
		l := 1.0
		if r > 0 && ref > 0 {
			if balance {
				l = math.Max(0.9, math.Min(1.1, math.Sqrt(math.Max(20, ref)/math.Max(20, float64(r)))))
			} else {
				l = math.Max(0.5, math.Min(1.25, math.Sqrt(ref/float64(r))))
			}
		}
		weights[tag] = b * l
	}
	return weights, true
}
func (s *BandwidthState) reserveLocked(tag string, groups []string) {
	s.active[tag]++
	for _, g := range groups {
		if s.groups[g] == nil {
			s.groups[g] = map[string]int{}
		}
		s.groups[g][tag]++
	}
}
func (s *BandwidthState) Reserve(tag string, groups []string) {
	s.access.Lock()
	defer s.access.Unlock()
	s.reserveLocked(tag, groups)
}
func (s *BandwidthState) Release(tag string, groups []string) {
	s.access.Lock()
	defer s.access.Unlock()
	if s.active[tag] <= 1 {
		delete(s.active, tag)
	} else {
		s.active[tag]--
	}
	for _, g := range groups {
		if s.groups[g][tag] <= 1 {
			delete(s.groups[g], tag)
		} else {
			s.groups[g][tag]--
		}
		if len(s.groups[g]) == 0 {
			delete(s.groups, g)
		}
	}
}
func (s *BandwidthState) External(tag, group string) int {
	s.access.Lock()
	defer s.access.Unlock()
	total := 0
	id := s.identityLocked(tag)
	for t, n := range s.active {
		if s.identityLocked(t) == id {
			total += n - s.groups[group][t]
		}
	}
	return max(0, total)
}

// Pick and pre-reserve atomically across all groups. Pending dials and active
// connections/sessions occupy one slot each, never two.
func (s *BandwidthState) Pick(tags []string, weights map[string]float64, group string) int {
	s.access.Lock()
	defer s.access.Unlock()
	totals := map[string]int{}
	for tag, n := range s.active {
		totals[s.identityLocked(tag)] += n
	}
	best := -1
	cost := math.Inf(1)
	for i, tag := range tags {
		w := weights[tag]
		if w <= 0 {
			continue
		}
		c := float64(totals[s.identityLocked(tag)]+1) / w
		if c < cost-1e-12 {
			cost = c
			best = i
		}
	}
	if best >= 0 {
		s.reserveLocked(tags[best], []string{group})
	}
	return best
}

func (s *BandwidthState) Forget(tag string) {
	s.access.Lock()
	defer s.access.Unlock()
	delete(s.samples, tag)
}

func (s *BandwidthState) Enable()       { s.enabled.Store(true) }
func (s *BandwidthState) Enabled() bool { return s.enabled.Load() }
