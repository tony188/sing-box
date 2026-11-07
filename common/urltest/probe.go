package urltest

import (
	"context"
	"time"

	"github.com/sagernet/sing-box/adapter"
)

// ProbeResult keeps the original completion time when another consumer reuses
// a result. A cache read must never manufacture a second history sample.
type ProbeResult struct {
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
	done    chan struct{}
	result  ProbeResult
	history *adapter.URLTestHistory // last valid pass for this exact probe key
	epoch   *byte                   // shared by successive probes until the outbound is invalidated
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
				result := entry.result
				result.Reused = true
				return result
			case <-ctx.Done():
				return ProbeResult{Err: ctx.Err()}
			}
		}
	}
	if previous == nil { s.pruneProbesLocked() }
	entry := &probeEntry{done: make(chan struct{}), epoch: new(byte)}
	if previous != nil {
		entry.history, entry.epoch = previous.history, previous.epoch
	}
	s.probes[key] = entry
	// Kernel-wide probe budget (pacer.go). The entry is registered first, so callers
	// for the same member join it while it waits for a slot instead of queueing twice.
	// run sets its own timeout (each caller's closure does), so waiting for the slot
	// never eats into it.
	ticket := s.probePacer().enqueue(PriorityOf(ctx))
	entry.ticket = ticket
	s.probeAccess.Unlock()
	s.probePacer().wait(ticket)
	delay, err := run(ctx)
	s.probePacer().release(ticket)
	result := ProbeResult{Time: time.Now(), Delay: delay, Err: err}
	// Publish only if this outbound has not been invalidated during the probe.
	// Retain the scoped pass on the first failure until its confirmation settles.
	s.probeAccess.Lock()
	current := s.probes[key] == entry
	if current && err == nil {
		entry.history = &adapter.URLTestHistory{Time: result.Time, Delay: delay}
		s.StoreURLTestHistory(tag, entry.history)
	}
	entry.result = result
	close(entry.done)
	s.probeAccess.Unlock()
	if current && err != nil && !confirmingOf(ctx) {
		s.failed(tag, link, run, result.Time)
	}
	return result
}
