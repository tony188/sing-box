package group

import (
	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"math"
	"slices"
	"time"
)

func (s *URLTest) BandwidthMembers() []string {
	if !s.bandwidthEnabled {
		return nil
	}
	return s.All()
}
func (s *URLTest) BandwidthHealthy(tag string) bool {
	if s.group == nil || s.group.history == nil {
		return false
	}
	h := s.group.history.LoadProbeHistory(tag, s.group.ProbeLink())
	return h != nil && h.Delay > 0 && !time.Now().Before(h.Time) && time.Since(h.Time) <= max(2*s.group.interval, 30*time.Second)
}
func (s *LoadBalance) BandwidthMembers() []string {
	if !s.bandwidthEnabled {
		return nil
	}
	return s.All()
}
func (s *LoadBalance) BandwidthHealthy(tag string) bool {
	s.access.Lock()
	defer s.access.Unlock()
	for _, m := range s.members {
		if m.outbound.Tag() == tag {
			return m.state == balanceHealthy && m.failed[0].IsZero() && time.Since(m.checked) <= max(2*s.interval, 30*time.Second)
		}
	}
	return false
}

// Stable single selection; own traffic must never cause self-induced switching.
func (g *URLTestGroup) selectBandwidth(network string, old adapter.Outbound) (adapter.Outbound, bool) {
	if !g.bandwidthEnabled || g.history == nil {
		return nil, false
	}
	g.bandwidthAccess.Lock()
	defer g.bandwidthAccess.Unlock()
	now := time.Now()
	delays := map[string]uint32{}
	byTag := map[string]adapter.Outbound{}
	for _, o := range g.outbounds {
		if !slices.Contains(o.Network(), network) {
			continue
		}
		tag := RealTag(o, network)
		h := g.history.LoadProbeHistory(tag, g.ProbeLink())
		if h == nil || h.Delay == 0 || now.Before(h.Time) || now.Sub(h.Time) > max(2*g.interval, 30*time.Second) {
			continue
		}
		delays[tag] = uint32(h.Delay)
		byTag[tag] = o
	}
	state := g.history.Bandwidth()
	weights, ok := state.Weights(delays, now)
	if !ok {
		return nil, false
	}
	bestTag := ""
	best := 0.0
	for _, o := range g.outbounds {
		tag := RealTag(o, network)
		if weights[tag] > best {
			best = weights[tag]
			bestTag = tag
		}
	}
	if bestTag == "" {
		return nil, false
	}
	index := 0
	if network == N.NetworkUDP {
		index = 1
	}
	current := ""
	if old != nil {
		current = RealTag(old, network)
	}
	if weights[current] > 0 {
		if current != bestTag && (!state.HasFresh(bestTag, now) || best < weights[current]*1.25 || now.Sub(g.bandwidthSince[index]) < time.Minute) {
			return byTag[current], true
		}
	} else {
		cost := math.Inf(1)
		for _, o := range g.outbounds {
			tag := RealTag(o, network)
			w := weights[tag]
			if w <= 0 || w < best*0.85 {
				continue
			}
			c := float64(state.External(tag, g.bandwidthTag)+1) / w
			if c < cost {
				cost = c
				bestTag = tag
			}
		}
	}
	if current != bestTag || g.bandwidthSince[index].IsZero() {
		g.bandwidthSince[index] = now
	}
	return byTag[bestTag], true
}
func (s *LoadBalance) reserveBandwidthLocked(network string, tried map[int]bool, destination ...M.Socksaddr) (int, int, bool) {
	now := time.Now()
	for pass := 0; pass < 4; pass++ {
		indices := []int{}
		tags := []string{}
		delays := map[string]uint32{}
		for step := 0; step < len(s.members); step++ {
			i := (s.cursor + step) % len(s.members)
			m := &s.members[i]
			ni := 0
			if N.NetworkName(network) == N.NetworkUDP {
				ni = 1
			}
			if tried[i] || !slices.Contains(m.outbound.Network(), N.NetworkName(network)) || m.state == balanceDown || m.state == balanceRecovering {
				continue
			}
			if ni == 0 && len(destination) > 0 && s.targetExcludedLocked(i, destination[0], now) {
				continue
			}
			if (pass < 2) != m.failed[ni].IsZero() || (pass%2 == 1) != (m.state == balanceUnknown) {
				continue
			}
			indices = append(indices, i)
			tag := m.outbound.Tag()
			tags = append(tags, tag)
			delay := uint32(0)
			if now.Sub(m.checked) <= max(2*s.interval, 30*time.Second) {
				delay = m.delay
			}
			delays[tag] = delay
		}
		if len(indices) == 0 {
			continue
		}
		if pass%2 == 1 {
			return -1, 0, false
		} // Unknown health never gains a bandwidth advantage.
		weights, ok := s.history.Bandwidth().LoadBalanceWeights(delays, now)
		if !ok {
			return -1, 0, false
		}
		picked := s.history.Bandwidth().Pick(tags, weights, s.Tag())
		if picked < 0 {
			return -1, 0, false
		}
		return indices[picked], len(indices), true
	}
	return -1, 0, false
}

func (s *URLTest) ReselectBandwidth() {
	if s.bandwidthEnabled && s.group != nil {
		s.group.Reselect()
	}
}
