package urltest

import (
	"context"
	"sync"
	"time"
)

// Open-Box: confirm a failure before switching away (1.14.1-openbox-tcp14).
//
// Upstream drops a member's history on its first failed probe or failed dial, and
// the next selection immediately moves every URLTest group off it. One transient
// timeout (a busy batch, a lost handshake) was enough to move a group onto a worse
// node, where it then stayed until the next interval.
//
// Rule (Open-Box user, 2026-09-30): test on the configured interval; if the node in
// use fails, test it once more after ConfirmDelay; if that passes, keep it and go
// back to the normal interval; only a second failure drops it so the group switches.
//
// Only members that some URLTest group currently uses are protected this way. A
// failed member nobody uses loses its history at once, as upstream does: nothing
// switches because of it, and confirming every failed member of a large batch would
// fire a second burst of probes ten seconds after the first.
var ConfirmDelay = 10 * time.Second

// ConfirmGroup is a URLTest group registered with the storage: which leaf tags it
// uses right now, and how to make it select again once a confirmation settled.
type ConfirmGroup interface {
	SelectedTags() []string
	ProbeLink() string
	Reselect()
}

type confirmRegistry struct {
	access     sync.Mutex
	groups     map[uint64]ConfirmGroup
	nextID     uint64
	confirming map[probeKey]chan struct{}
	events     map[probeKey]Confirmation
	done       chan struct{}
}

func (s *HistoryStorage) confirmState() *confirmRegistry {
	s.confirmOnce.Do(func() {
		s.confirm = &confirmRegistry{
			groups:     make(map[uint64]ConfirmGroup),
			confirming: make(map[probeKey]chan struct{}),
			events:     make(map[probeKey]Confirmation),
			done:       make(chan struct{}),
		}
	})
	return s.confirm
}

// RegisterGroup adds a URLTest group; the returned function removes it (call it
// when the group closes, including when a running kernel replaces it).
func (s *HistoryStorage) RegisterGroup(group ConfirmGroup) func() {
	if s == nil || group == nil {
		return func() {}
	}
	r := s.confirmState()
	r.access.Lock()
	r.nextID++
	id := r.nextID
	r.groups[id] = group
	r.access.Unlock()
	return func() {
		r.access.Lock()
		delete(r.groups, id)
		r.access.Unlock()
	}
}

func (s *HistoryStorage) inUse(tag, link string) bool {
	r := s.confirmState()
	r.access.Lock()
	groups := make([]ConfirmGroup, 0, len(r.groups))
	for _, group := range r.groups {
		groups = append(groups, group)
	}
	r.access.Unlock()
	for _, group := range groups {
		if probeLinkDefault(group.ProbeLink()) != link {
			continue
		}
		for _, selected := range group.SelectedTags() {
			if selected == tag {
				return true
			}
		}
	}
	return false
}

func (s *HistoryStorage) reselectAll() {
	r := s.confirmState()
	r.access.Lock()
	groups := make([]ConfirmGroup, 0, len(r.groups))
	for _, group := range r.groups {
		groups = append(groups, group)
	}
	r.access.Unlock()
	for _, group := range groups {
		group.Reselect()
	}
}

// Failed records that a probe of tag (or a real dial through it) failed.
//
// A member in use that still has a passing record keeps it for now, and is
// rechecked after ConfirmDelay: a pass keeps it with the new result, a failure
// deletes the record; either way the registered groups select again. Anything
// else loses its record at once. It reports whether a recheck was scheduled or is
// already pending.
//
// The recheck is "one result newer than the failure", taken through Probe like
// any other test: if something else has probed the member since (the panel's
// failover recheck, which also comes ten seconds after the failure it saw; a
// manual test), or is probing it right now, that result is the recheck and the
// member is not probed a second time. A recheck is critical: first in the probe
// queue, inside the same budget.
func (s *HistoryStorage) Failed(tag, link string, probe func(ctx context.Context) (uint16, error)) bool {
	return s.failed(tag, link, probe, time.Now())
}

func (s *HistoryStorage) failed(tag, link string, probe func(ctx context.Context) (uint16, error), failedAt time.Time) bool {
	if s == nil {
		return false
	}
	link = probeLinkDefault(link)
	history := s.LoadProbeHistory(tag, link)
	if history != nil && history.Time.After(failedAt) {
		return false
	}
	if probe == nil || !s.inUse(tag, link) {
		s.deleteProbeHistory(tag, link, failedAt)
		return false
	}
	// Keep lock order consistent with outbound invalidation: probe -> confirmation.
	s.probeAccess.Lock()
	if s.probeClosed {
		s.probeAccess.Unlock()
		return false
	}
	entry := s.probes[probeKey{tag, link}]
	if entry == nil {
		s.pruneProbesLocked()
		if s.probes == nil {
			s.probes = make(map[probeKey]*probeEntry)
		}
		entry = &probeEntry{done: make(chan struct{}), epoch: new(byte), generation: newProbeIdentity()}
		close(entry.done)
		s.probes[probeKey{tag, link}] = entry
	}
	epoch := entry.epoch
	r := s.confirmState()
	r.access.Lock()
	s.probeAccess.Unlock()
	key := probeKey{tag, link}
	if r.confirming[key] != nil {
		r.access.Unlock()
		return true
	}
	cancel := make(chan struct{})
	r.confirming[key] = cancel
	r.events[key] = Confirmation{FailedAt: failedAt.UnixMilli(), DueAt: time.Now().Add(ConfirmDelay).UnixMilli(), Stage: "waiting"}
	done := r.done
	r.access.Unlock()
	delay := ConfirmDelay
	go func() {
		defer func() {
			r.access.Lock()
			if r.confirming[key] == cancel {
				delete(r.confirming, key)
			}
			r.access.Unlock()
		}()
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-done:
			return
		case <-cancel:
			return
		case <-timer.C:
		}
		// probe (the failed caller's run function) sets its own timeout.
		ctx := withConfirming(WithNewerThan(WithPriority(context.Background(), PriorityCritical), failedAt))
		ctx = context.WithValue(ctx, probeEpochKey{}, epoch)
		s.markConfirmation(key, cancel, "queued")
		before := s.selectedFor(link)
		result := s.Probe(ctx, tag, link, 0, true, func(ctx context.Context) (uint16, error) {
			s.markConfirmation(key, cancel, "probing")
			return probe(ctx)
		})
		select {
		case <-done:
			return
		case <-cancel:
			return
		default:
		}
		if result.Err != nil || result.Delay == 0 {
			// Only a newer pass for this same URL/status condition can save it.
			s.deleteProbeHistory(tag, link, failedAt)
		}
		s.reselectAll()
		s.finishConfirmation(key, cancel, result, before, s.selectedFor(link))
	}()
	return true
}

// Confirming reports whether tag has a confirmation pending (tests and the Clash API).
func (s *HistoryStorage) Confirming(tag, link string) bool {
	if s == nil {
		return false
	}
	r := s.confirmState()
	r.access.Lock()
	defer r.access.Unlock()
	return r.confirming[probeKey{tag, probeLinkDefault(link)}] != nil
}

func (s *HistoryStorage) closeConfirm() {
	r := s.confirmState()
	r.access.Lock()
	defer r.access.Unlock()
	select {
	case <-r.done:
	default:
		close(r.done)
	}
}
