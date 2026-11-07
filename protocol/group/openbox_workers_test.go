package group

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// tcp20: a round of hundreds of members keeps at most n tasks in flight, starts them in
// order, runs every one, and does not park a goroutine per waiting member.
func TestOpenBoxURLTestWorkers(t *testing.T) {
	const members = 300
	const workers = 10
	var running, peak atomic.Int64
	var mu sync.Mutex
	var order []int
	release := make(chan struct{})
	tasks := make([]func(), members)
	for i := range tasks {
		tasks[i] = func() {
			mu.Lock()
			order = append(order, i)
			mu.Unlock()
			now := running.Add(1)
			for {
				old := peak.Load()
				if now <= old || peak.CompareAndSwap(old, now) {
					break
				}
			}
			<-release
			running.Add(-1)
		}
	}
	before := runtime.NumGoroutine()
	done := make(chan struct{})
	go func() {
		runBounded(workers, tasks)
		close(done)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for running.Load() < workers && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	// Upstream's batch would have had all 300 parked here.
	if extra := runtime.NumGoroutine() - before; extra > workers+5 {
		t.Fatalf("%d goroutines for %d workers", extra, workers)
	}
	close(release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("round did not finish")
	}
	if peak.Load() > workers {
		t.Fatalf("%d tasks ran at once, limit %d", peak.Load(), workers)
	}
	if len(order) != members {
		t.Fatalf("%d of %d tasks ran", len(order), members)
	}
	seen := make(map[int]bool, members)
	for _, i := range order {
		if seen[i] {
			t.Fatalf("task %d ran twice", i)
		}
		seen[i] = true
	}
	// Started in order (each worker takes the next one); only the first n race each other.
	for position, i := range order {
		if i > position+workers {
			t.Fatalf("task %d started at position %d", i, position)
		}
	}
	runBounded(workers, nil)
	var single int
	runBounded(0, []func(){func() { single++ }})
	if single != 1 {
		t.Fatal("n below 1 must still run the tasks")
	}
}
