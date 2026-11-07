package urltest

import (
	"context"
	"time"
)

// ProbeTransient uses the same kernel-wide pacing/concurrency budget without
// publishing to URL-test latency/confirmation history. Destination reachability
// is private to its balancer and must not influence other groups or UI delay.
func (s *HistoryStorage) ProbeTransient(ctx context.Context, owner string, run func(context.Context) (uint16, error)) ProbeResult {
	if ctx.Err() != nil {
		return ProbeResult{Err: context.Canceled}
	}
	s.probeAccess.Lock()
	closed := s.probeClosed
	s.probeAccess.Unlock()
	if closed {
		return ProbeResult{Err: context.Canceled}
	}
	p := s.probePacer()
	ticket := p.enqueueValid(PriorityOf(ctx), owner, ctx.Done())
	if !p.waitValid(ticket, ctx.Done()) {
		return ProbeResult{Err: context.Canceled}
	}
	defer p.release(ticket)
	s.probeAccess.Lock()
	closed = s.probeClosed
	s.probeAccess.Unlock()
	if ctx.Err() != nil || closed {
		return ProbeResult{Err: context.Canceled}
	}
	delay, err := run(ctx)
	return ProbeResult{Time: time.Now(), Delay: delay, Err: err}
}
