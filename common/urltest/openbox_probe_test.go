package urltest

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestOpenBoxProbeIntervals(t *testing.T) {
	s := NewHistoryStorage()
	var calls atomic.Int32
	run := func(context.Context) (uint16, error) { calls.Add(1); return 123, nil }
	ctx := context.Background()
	a := s.Probe(ctx, "node", "http://test", time.Second, false, run)
	b := s.Probe(ctx, "node", "http://test", time.Second, false, run)
	if calls.Load() != 1 || !b.Reused || !a.Time.Equal(b.Time) {
		t.Fatal("fresh result was not reused", a, b, calls.Load())
	}
	// Exercise the expired-entry branch without fake network traffic or a long sleep.
	s.probeAccess.Lock()
	s.probes[probeKey{"node", "http://test"}].result.Time = time.Now().Add(-time.Minute)
	s.probeAccess.Unlock()
	c := s.Probe(ctx, "node", "http://test", time.Second, false, run)
	if calls.Load() != 2 || c.Reused {
		t.Fatal("expired result was not retested", calls.Load())
	}
	s.Probe(ctx, "node", "http://other", time.Second, false, run)
	if calls.Load() != 3 {
		t.Fatal("different URLs shared a result")
	}
}

func TestOpenBoxProbeConcurrent(t *testing.T) {
	s := NewHistoryStorage()
	var calls atomic.Int32
	entered, release := make(chan struct{}), make(chan struct{})
	run := func(context.Context) (uint16, error) {
		if calls.Add(1) == 1 {
			close(entered)
		}
		<-release
		return 42, nil
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); s.Probe(context.Background(), "node", "http://test", time.Minute, false, run) }()
	<-entered
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); s.Probe(context.Background(), "node", "http://test", time.Minute, false, run) }()
	}
	close(release)
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatal("overlapping network requests", calls.Load())
	}
}

func TestOpenBoxProbeTimeoutRetry(t *testing.T) {
	s := NewHistoryStorage()
	var calls atomic.Int32
	run := func(context.Context) (uint16, error) { calls.Add(1); return 0, context.DeadlineExceeded }
	a := s.Probe(context.Background(), "node", "http://test", time.Minute, false, run)
	b := s.Probe(context.Background(), "node", "http://test", time.Minute, false, run)
	if !errors.Is(b.Err, context.DeadlineExceeded) || calls.Load() != 1 || !a.Time.Equal(b.Time) {
		t.Fatal("failed result was not cached")
	}
	s.Probe(context.Background(), "node", "http://test", time.Minute, true, run)
	if calls.Load() != 2 {
		t.Fatal("explicit timeout retry was suppressed")
	}
	if s.LoadURLTestHistory("node") != nil {
		t.Fatal("failed node retained successful history")
	}
}
