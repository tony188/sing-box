package urltest

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type joiningProbeContext struct {
	context.Context
	joined  chan struct{}
	once    sync.Once
	release <-chan struct{}
}

func (c *joiningProbeContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.joined) })
	if c.release != nil {
		<-c.release
	}
	return c.Context.Done()
}

func TestOpenBoxProbeInvalidationAfterRefreshWakesOldWaiters(t *testing.T) {
	withPacing(t, 2, 0)
	s := NewHistoryStorage()
	t.Cleanup(func() { s.Close() })
	gate, joinGate, started := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce, joinOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gate) }) }
	joinRelease := func() { joinOnce.Do(func() { close(joinGate) }) }
	t.Cleanup(release)
	t.Cleanup(joinRelease)
	old := make(chan ProbeResult, 1)
	go func() {
		old <- s.Probe(context.Background(), "node", "http://test/", 0, true, func(context.Context) (uint16, error) {
			close(started)
			<-gate
			return 10, nil
		})
	}()
	<-started
	ctx := &joiningProbeContext{Context: context.Background(), joined: make(chan struct{}), release: joinGate}
	joined := make(chan ProbeResult, 1)
	go func() {
		joined <- s.Probe(ctx, "node", "http://test/", 0, true, func(context.Context) (uint16, error) { t.Error("joiner dialled"); return 0, nil })
	}()
	<-ctx.joined
	release()
	<-old
	if r := s.Probe(context.Background(), "node", "http://test/", 0, true, func(context.Context) (uint16, error) { return 20, nil }); r.Err != nil {
		t.Fatal(r)
	}
	s.DeleteURLTestHistory("node")
	joinRelease()
	if r := <-joined; !errors.Is(r.Err, context.Canceled) || !r.Time.IsZero() {
		t.Fatal("old waiter returned a pre-invalidation success after refresh", r)
	}
}

func holdProbe(t *testing.T, s *HistoryStorage) func() {
	t.Helper()
	gate, done := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(func() { release(); <-done })
	go func() {
		defer close(done)
		s.Probe(context.Background(), "busy", "http://busy.test/", time.Hour, false, func(context.Context) (uint16, error) {
			<-gate
			return 10, nil
		})
	}()
	waitFor(t, "occupied probe slot", func() bool { return s.ProbeQueue().Running == 1 })
	return release
}

func TestOpenBoxProbeInvalidationRemovesQueuedWork(t *testing.T) {
	withPacing(t, 1, 0)
	s := NewHistoryStorage()
	t.Cleanup(func() { s.Close() })
	release := holdProbe(t, s)
	old := make(chan ProbeResult, 1)
	var oldCalls atomic.Int32
	go func() {
		old <- s.Probe(context.Background(), "node", "http://test/", time.Hour, false, func(context.Context) (uint16, error) {
			oldCalls.Add(1)
			return 1, nil
		})
	}()
	waitFor(t, "old queued probe", func() bool { return s.ProbeQueue().Background == 1 })
	s.DeleteURLTestHistory("node")
	select {
	case result := <-old:
		if !errors.Is(result.Err, context.Canceled) || !result.Time.IsZero() {
			t.Fatal("invalidated queue item reported a network sample", result)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("invalidated probe still waits behind the occupied slot")
	}
	if s.ProbeQueue().Background != 0 || oldCalls.Load() != 0 {
		t.Fatal("obsolete queued work retained its slot or dialled")
	}
	fresh := make(chan ProbeResult, 1)
	go func() {
		fresh <- s.Probe(context.Background(), "node", "http://test/", time.Hour, false, func(context.Context) (uint16, error) { return 20, nil })
	}()
	waitFor(t, "new queued probe", func() bool { return s.ProbeQueue().Background == 1 })
	release()
	if result := <-fresh; result.Err != nil || result.Delay != 20 {
		t.Fatal("replacement failed", result)
	}
	if oldCalls.Load() != 0 {
		t.Fatal("obsolete callback ran after the replacement")
	}
}

func TestOpenBoxProbeInvalidationWakesJoinersAndRejectsLatePass(t *testing.T) {
	withPacing(t, 2, 0)
	s := NewHistoryStorage()
	t.Cleanup(func() { s.Close() })
	gate, started := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	old := make(chan ProbeResult, 1)
	go func() {
		old <- s.Probe(context.Background(), "node", "http://test/", time.Hour, false, func(context.Context) (uint16, error) {
			close(started)
			<-gate
			return 1, nil
		})
	}()
	<-started
	joined := make(chan ProbeResult, 1)
	base, cancel := context.WithCancel(context.Background())
	ctx := &joiningProbeContext{Context: base, joined: make(chan struct{})}
	t.Cleanup(cancel)
	go func() {
		joined <- s.Probe(ctx, "node", "http://test/", time.Hour, false, func(context.Context) (uint16, error) { t.Error("joiner dialled"); return 0, nil })
	}()
	<-ctx.joined
	// A cancelled caller is not the source of the cancellation being verified.
	s.DeleteURLTestHistory("node")
	select {
	case result := <-joined:
		if !errors.Is(result.Err, context.Canceled) {
			t.Fatal(result)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("joiner still waits for an obsolete in-flight probe")
	}
	result := s.Probe(WithPriority(context.Background(), PriorityInteractive), "node", "http://test/", time.Hour, false, func(context.Context) (uint16, error) { return 20, nil })
	if result.Err != nil {
		t.Fatal(result.Err)
	}
	release()
	if result := <-old; !errors.Is(result.Err, context.Canceled) || !result.Time.IsZero() {
		t.Fatal("old caller received a stale pass", result)
	}
	if h := s.LoadProbeHistory("node", "http://test/"); h == nil || h.Delay != 20 {
		t.Fatal("late pass replaced fresh result", h)
	}
}

func TestOpenBoxProbeCloseRemovesQueueAndRejectsNewWork(t *testing.T) {
	withPacing(t, 1, 0)
	s := NewHistoryStorage()
	release := holdProbe(t, s)
	done := make(chan ProbeResult, 1)
	var calls atomic.Int32
	go func() {
		done <- s.Probe(context.Background(), "node", "http://test/", 0, true, func(context.Context) (uint16, error) { calls.Add(1); return 1, nil })
	}()
	waitFor(t, "queued probe", func() bool { return s.ProbeQueue().Background == 1 })
	s.Close()
	select {
	case result := <-done:
		if !errors.Is(result.Err, context.Canceled) {
			t.Fatal(result)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("closed storage retained queued work")
	}
	result := s.Probe(context.Background(), "new", "http://test/", 0, true, func(context.Context) (uint16, error) { calls.Add(1); return 2, nil })
	if !errors.Is(result.Err, context.Canceled) || calls.Load() != 0 || s.ProbeQueue().Background != 0 {
		t.Fatal("closed storage accepted work", result)
	}
	release()
}

func TestOpenBoxProbeInvalidatedBatchDoesNotReserveFutureStarts(t *testing.T) {
	p := &probePacer{limit: 1, spacing: 100 * time.Millisecond}
	busy := p.enqueue(PriorityBackground)
	p.wait(busy)
	invalidated := make(chan struct{})
	var obsolete []*pacerTicket
	for i := 0; i < 40; i++ {
		obsolete = append(obsolete, p.enqueueValid(PriorityBackground, "old", invalidated))
	}
	close(invalidated)
	// The dispatcher may free a slot before cancelled waiters get CPU time.
	// It must skip them without reserving another four seconds of start times.
	p.release(busy)
	fresh := p.enqueue(PriorityBackground)
	select {
	case start := <-fresh.grant:
		if time.Until(start) > 200*time.Millisecond {
			t.Fatal("obsolete batch reserved future pacing slots", time.Until(start))
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("obsolete batch blocked replacement")
	}
	for _, ticket := range obsolete {
		if p.waitValid(ticket, invalidated) {
			t.Fatal("obsolete ticket started")
		}
	}
	p.release(fresh)
	if p.running != 0 || p.runningBackground != 0 || len(p.queues[PriorityBackground]) != 0 {
		t.Fatal("cancel/release leaked or double-freed slots", p)
	}
}

func TestOpenBoxProbeCancelledAdmissionIsNotANetworkFailure(t *testing.T) {
	withPacing(t, 2, 0)
	s := NewHistoryStorage()
	t.Cleanup(func() { s.Close() })
	defer s.RegisterGroup(&fakeGroup{selected: []string{"node"}, link: "http://test/"})()
	pass := s.Probe(context.Background(), "node", "http://test/", 0, true, func(context.Context) (uint16, error) { return 10, nil })
	result := s.Probe(context.Background(), "node", "http://test/", 0, true, func(context.Context) (uint16, error) { return 0, context.Canceled })
	if !errors.Is(result.Err, context.Canceled) || !result.Time.IsZero() || s.Confirming("node", "http://test/") {
		t.Fatal("retired work manufactured a failed sample", result)
	}
	if h := s.LoadProbeHistory("node", "http://test/"); h == nil || h.Delay != 10 {
		t.Fatal("cancellation removed an existing pass", h)
	}
	sample := s.Snapshot([]ProbeCondition{{Tag: "node", URL: "http://test/"}}).Results[0]
	if sample.At != pass.Time.UnixMilli() || sample.OK == nil || !*sample.OK {
		t.Fatal("read-only snapshot lost the last real result", sample)
	}
	fresh := s.Probe(context.Background(), "node", "http://test/", time.Hour, false, func(context.Context) (uint16, error) { return 20, nil })
	if fresh.Err != nil || fresh.Reused || fresh.Delay != 20 {
		t.Fatal("cancellation was cached as a network outcome", fresh)
	}
}
