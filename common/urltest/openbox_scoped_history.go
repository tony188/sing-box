package urltest

import (
	"time"

	"github.com/sagernet/sing-box/adapter"
)

const maxCompletedProbeEntries = 4096

// Distinct manual URLs/status policies must not remain for the lifetime of an
// outbound. Dropping an old finished cache entry merely makes its next caller
// measure again. In-flight work and scheduled confirmations retain ownership.
func (s *HistoryStorage) pruneProbesLocked() {
	if len(s.probes) < maxCompletedProbeEntries {
		return
	}
	r := s.confirmState()
	r.access.Lock()
	defer r.access.Unlock()
	// Group selectors rely on every member's pass for their configured URL.
	// ProbeLink is immutable; do not call SelectedTags here, whose group lock
	// is also held by selection while it reads the probe cache.
	activeLinks := make(map[string]bool, len(r.groups))
	for _, group := range r.groups {
		activeLinks[probeLinkDefault(group.ProbeLink())] = true
	}
	var oldest probeKey
	var at time.Time
	found := false
	for key, entry := range s.probes {
		if r.confirming[key] != nil || activeLinks[key.link] {
			continue
		}
		select {
		case <-entry.done:
		default:
			continue
		}
		if !found || entry.result.Time.Before(at) {
			oldest, at, found = key, entry.result.Time, true
		}
	}
	if found {
		delete(s.probes, oldest)
		delete(r.events, oldest)
	}
}

func probeLinkDefault(link string) string {
	if link == "" {
		return "http://www.gstatic.com/generate_204"
	}
	return link
}

// LoadProbeHistory reads the pass used for selection, including the pass retained
// during a ten-second confirmation. The existing probe cache owns its lifetime.
// Generic /proxies history remains for display; it cannot prove another URL works.
func (s *HistoryStorage) LoadProbeHistory(tag, link string) *adapter.URLTestHistory {
	if s == nil {
		return nil
	}
	s.probeAccess.Lock()
	defer s.probeAccess.Unlock()
	if entry := s.probes[probeKey{tag, probeLinkDefault(link)}]; entry != nil {
		return entry.history
	}
	return nil
}

// Delete only this condition's pass, unless a newer pass has already replaced it.
func (s *HistoryStorage) deleteProbeHistory(tag, link string, failedAt time.Time) {
	s.probeAccess.Lock()
	defer s.probeAccess.Unlock()
	entry := s.probes[probeKey{tag, probeLinkDefault(link)}]
	if entry == nil || entry.history == nil || entry.history.Time.After(failedAt) {
		return
	}
	history := entry.history
	entry.history = nil
	s.access.Lock()
	// A different condition's more recent display sample must survive this failure.
	if s.delayHistory[tag] == history {
		delete(s.delayHistory, tag)
		s.notifyUpdated()
	}
	s.access.Unlock()
}

func (s *HistoryStorage) cancelConfirm(tag string) {
	r := s.confirmState()
	r.access.Lock()
	defer r.access.Unlock()
	for key := range r.events {
		if key.tag == tag {
			delete(r.events, key)
		}
	}
	for key, cancel := range r.confirming {
		if key.tag == tag {
			close(cancel)
			delete(r.confirming, key)
		}
	}
}
