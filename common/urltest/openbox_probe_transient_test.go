package urltest

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestOpenBoxProbeTransientBudgetCancellationAndHistory(t *testing.T) {
	oldLimit, oldSpacing := ProbeConcurrency, ProbeSpacing
	ProbeConcurrency, ProbeSpacing = 1, time.Millisecond
	defer func() { ProbeConcurrency, ProbeSpacing = oldLimit, oldSpacing }()
	s := NewHistoryStorage()
	defer s.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	done := make(chan ProbeResult, 1)
	go func() {
		done <- s.ProbeTransient(ctx, "target", func(ctx context.Context) (uint16, error) { close(started); <-ctx.Done(); return 0, ctx.Err() })
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("probe did not start")
	}
	queuedCtx, queuedCancel := context.WithCancel(context.Background())
	queuedDone := make(chan ProbeResult, 1)
	var ran atomic.Bool
	go func() {
		queuedDone <- s.ProbeTransient(queuedCtx, "other", func(context.Context) (uint16, error) { ran.Store(true); return 12, nil })
	}()
	queuedCancel()
	select {
	case <-queuedDone:
	case <-time.After(time.Second):
		t.Fatal("canceled waiter retained slot")
	}
	if ran.Load() {
		t.Fatal("canceled probe performed I/O")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("running probe not canceled")
	}
	if q := s.ProbeQueue(); q.Running != 0 || q.Background != 0 {
		t.Fatal("probe budget leak", q)
	}
	result := s.ProbeTransient(context.Background(), "target", func(context.Context) (uint16, error) { return 12, nil })
	if result.Err != nil || result.Delay != 12 || result.Time.IsZero() {
		t.Fatal(result)
	}
	if len(s.probes) != 0 || len(s.delayHistory) != 0 {
		t.Fatal("reachability polluted normal probe history")
	}
	s.Close()
	s.ProbeTransient(context.Background(), "target", func(context.Context) (uint16, error) { t.Fatal("closed history started a probe"); return 0, nil })
}
