package urltest

import (
	"context"
	"errors"
	"time"

	"github.com/sagernet/sing-box/adapter"
)

// ProbeResult keeps the original completion time when another consumer reuses
// a result. A cache read must never manufacture a second history sample.
type ProbeResult struct {
	Status int
	Time   time.Time
	Delay  uint16
	Err    error
	Reused bool
}
type probeKey struct{ tag, link string }

type newerThanKey struct{}

type confirmingKey struct{}

// A pending recheck must not publish results for an outbound replaced in place.
type probeEpochKey struct{}

// WithNewerThan makes Probe accept any result for the member completed after t,
// even under force, and join one in flight: "a result newer than the failure" is
// the recheck, whoever produced it (Open-Box tcp14, see confirm.go). "After" counts
// whole milliseconds, see newerThan.
func WithNewerThan(ctx context.Context, t time.Time) context.Context {
	return context.WithValue(ctx, newerThanKey{}, t)
}

func newerThanOf(ctx context.Context) time.Time {
	t, _ := ctx.Value(newerThanKey{}).(time.Time)
	return t
}

// newerThan reports whether a result completed at t is newer than bound, in whole
// milliseconds (Open-Box tcp21). The panel's and the App's failover recheck send the
// failure they saw as `since` in unix milliseconds, all the Clash API's result time
// gives them, and the failure itself completed somewhere inside that millisecond.
// Compared to the nanosecond (tcp14-tcp20), the failure counted as newer than itself:
// the recheck got the very failure back instead of a probe, so a member no URL test
// group was using was never tested again and the lane switched even when the node
// had recovered. The kernel's own recheck passes its exact failure time; a result
// finishing later within that same millisecond does not count either, which costs
// at most one more probe.
func newerThan(t, bound time.Time) bool {
	return !bound.IsZero() && t.UnixMilli() > bound.UnixMilli()
}

// withConfirming marks the recheck confirm.go runs itself: its failure is judged
// there, not turned into another recheck.
func withConfirming(ctx context.Context) context.Context {
	return context.WithValue(ctx, confirmingKey{}, true)
}

func confirmingOf(ctx context.Context) bool {
	v, _ := ctx.Value(confirmingKey{}).(bool)
	return v
}

type probeEntry struct {
	generation  uint64
	previous    ProbeResult
	done        chan struct{}
	result      ProbeResult
	history     *adapter.URLTestHistory // last valid pass for this exact probe key
	epoch       *byte                   // shared by successive probes until the outbound is invalidated
	invalidated chan struct{}           // wakes obsolete queued owners and callers sharing their result
	// ticket is the entry's place in the probe queue (pacer.go) while it waits for
	// a slot; an interactive caller joining a queued background probe moves it up.
	ticket *pacerTicket
}

// Probe arbitrates native URLTest batches and Clash API probes in the same
// kernel. Panel-only locks cannot see native batches or survive panel restarts.
// Failed probes are cached too, so refreshing a group's selection cannot retry
// a failed member behind the failover manager's back.
func (s *HistoryStorage) Probe(ctx context.Context, tag, link string, interval time.Duration, force bool, run func(context.Context) (uint16, error)) ProbeResult {
	if link == "" {
		link = "http://www.gstatic.com/generate_204"
	}
	key := probeKey{tag, link}
	s.probeAccess.Lock()
	if s.probeClosed {
		s.probeAccess.Unlock()
		return ProbeResult{Err: context.Canceled}
	}
	previous := s.probes[key]
	if epoch, ok := ctx.Value(probeEpochKey{}).(*byte); ok && (previous == nil || previous.epoch != epoch) {
		s.probeAccess.Unlock()
		return ProbeResult{Err: context.Canceled}
	}
	if s.probes == nil {
		s.probes = make(map[probeKey]*probeEntry)
	}
	if entry := s.probes[key]; entry != nil {
		select {
		case <-entry.done:
			age := time.Since(entry.result.Time)
			if newerThan(entry.result.Time, newerThanOf(ctx)) || (!force && age >= 0 && age < interval) {
				result := entry.result
				result.Reused = true
				s.probeAccess.Unlock()
				return result
			}
		default:
			if priority := PriorityOf(ctx); priority > PriorityBackground && entry.ticket != nil {
				s.probePacer().upgrade(entry.ticket, priority)
			}
			s.probeAccess.Unlock()
			select {
			case <-entry.done:
				select {
				case <-entry.invalidated:
					return ProbeResult{Err: context.Canceled}
				default:
				}
				result := entry.result
				result.Reused = true
				return result
			case <-ctx.Done():
				return ProbeResult{Err: ctx.Err()}
			case <-entry.invalidated:
				return ProbeResult{Err: context.Canceled}
			}
		}
	}
	if previous == nil {
		s.pruneProbesLocked()
	}
	entry := &probeEntry{done: make(chan struct{}), invalidated: make(chan struct{}), epoch: new(byte), generation: newProbeIdentity()}
	if previous != nil {
		entry.history, entry.epoch, entry.generation = previous.history, previous.epoch, previous.generation
		if previous.invalidated != nil {
			entry.invalidated = previous.invalidated
		}
		entry.previous = previous.result
		if entry.previous.Time.IsZero() {
			entry.previous = previous.previous
		}
	}
	s.probes[key] = entry
	// Kernel-wide probe budget (pacer.go). The entry is registered first, so callers
	// for the same member join it while it waits for a slot instead of queueing twice.
	// run sets its own timeout (each caller's closure does), so waiting for the slot
	// never eats into it.
	ticket := s.probePacer().enqueueValid(PriorityOf(ctx), link, entry.invalidated)
	entry.ticket = ticket
	s.probeAccess.Unlock()
	result := ProbeResult{Err: context.Canceled}
	if s.probePacer().waitValid(ticket, entry.invalidated) {
		status := 0
		delay, err := run(context.WithValue(ctx, httpStatusKey{}, &status))
		result = ProbeResult{Time: time.Now(), Delay: delay, Err: err, Status: status}
		if errors.Is(err, context.Canceled) {
			result = ProbeResult{Err: context.Canceled}
		}
		s.probePacer().release(ticket)
	}
	// Publish only if this outbound has not been invalidated during the probe.
	// Retain the scoped pass on the first failure until its confirmation settles.
	s.probeAccess.Lock()
	current := !s.probeClosed && s.probes[key] == entry
	if !current {
		result = ProbeResult{Err: context.Canceled}
	}
	if current && result.Err == nil {
		entry.history = &adapter.URLTestHistory{Time: result.Time, Delay: result.Delay}
		s.StoreURLTestHistory(tag, entry.history)
	}
	entry.result = result
	close(entry.done)
	s.probeAccess.Unlock()
	if current && result.Err != nil && !errors.Is(result.Err, context.Canceled) && !confirmingOf(ctx) {
		s.failed(tag, link, run, result.Time)
	}
	return result
}
