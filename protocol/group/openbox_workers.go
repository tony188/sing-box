package group

import (
	"sync"
	"sync/atomic"
)

// runBounded runs tasks in order with at most n running at a time, on n goroutines that
// take the next task when one finishes (Open-Box tcp20).
//
// URL test rounds used sing's batch.Go, which starts one goroutine per member up front,
// each parked until one of the ten slots frees up. With a dozen groups of a few hundred
// members starting together that was some 600 parked goroutines and their stacks during
// every round: about 5 MB the iOS network extension (50 MB, phys_footprint) cannot spare.
// The concurrency is unchanged: still at most ten probes per group in flight, and the
// kernel-wide probe pacer (tcp14, four at a time) is still the bottleneck, so a round
// takes as long as before.
func runBounded(n int, tasks []func()) {
	if len(tasks) == 0 {
		return
	}
	n = min(max(n, 1), len(tasks))
	var next atomic.Int64
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			for {
				index := int(next.Add(1) - 1)
				if index >= len(tasks) {
					return
				}
				tasks[index]()
			}
		})
	}
	wg.Wait()
}
