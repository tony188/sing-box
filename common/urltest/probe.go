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
type probeEntry struct {
	done   chan struct{}
	result ProbeResult
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
			if !force && age >= 0 && age < interval {
				result := entry.result
				result.Reused = true
				s.probeAccess.Unlock()
				return result
			}
		default:
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
	s.probeAccess.Unlock()
	delay, err := run(ctx)
	result := ProbeResult{Time: time.Now(), Delay: delay, Err: err}
	// Publish history before releasing waiters. Cache reads and overlapping
	// requests preserve the original completion time rather than creating samples.
	if err != nil {
		s.DeleteURLTestHistory(tag)
	} else {
		s.StoreURLTestHistory(tag, &adapter.URLTestHistory{Time: result.Time, Delay: delay})
	}
	s.probeAccess.Lock()
	entry.result = result
	close(entry.done)
	s.probeAccess.Unlock()
	return result
}
