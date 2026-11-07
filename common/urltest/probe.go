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

// WithNewerThan makes Probe accept any result for the member completed after t,
// even under force, and join one in flight: "a result newer than the failure" is
// the recheck, whoever produced it (Open-Box tcp14, see confirm.go).
func WithNewerThan(ctx context.Context, t time.Time) context.Context {
	return context.WithValue(ctx, newerThanKey{}, t)
}

func newerThanOf(ctx context.Context) time.Time {
	t, _ := ctx.Value(newerThanKey{}).(time.Time)
	return t
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
	done   chan struct{}
	result ProbeResult
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
	if s.probes == nil {
		s.probes = make(map[probeKey]*probeEntry)
	}
	if entry := s.probes[key]; entry != nil {
		select {
		case <-entry.done:
			age := time.Since(entry.result.Time)
			newer := newerThanOf(ctx)
			if (!newer.IsZero() && entry.result.Time.After(newer)) || (!force && age >= 0 && age < interval) {
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
	entry := &probeEntry{done: make(chan struct{})}
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
	// Publish history before releasing waiters. Cache reads and overlapping
	// requests preserve the original completion time rather than creating samples.
	if err != nil {
		if !confirmingOf(ctx) {
			s.Failed(tag, link, run)
		}
	} else {
		s.StoreURLTestHistory(tag, &adapter.URLTestHistory{Time: result.Time, Delay: delay})
	}
	s.probeAccess.Lock()
	entry.result = result
	close(entry.done)
	s.probeAccess.Unlock()
	return result
}
