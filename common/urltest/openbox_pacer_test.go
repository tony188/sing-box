package urltest

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func withPacing(t *testing.T, concurrency int, spacing time.Duration) {
	oldN, oldS := ProbeConcurrency, ProbeSpacing
	ProbeConcurrency, ProbeSpacing = concurrency, spacing
	t.Cleanup(func() { ProbeConcurrency, ProbeSpacing = oldN, oldS })
}

// probeOrder records which member's probe actually started, in order.
type probeOrder struct {
	access sync.Mutex
	order  []string
}

func (o *probeOrder) run(tag string, hold <-chan struct{}) func(context.Context) (uint16, error) {
	return func(context.Context) (uint16, error) {
		o.access.Lock()
		o.order = append(o.order, tag)
		o.access.Unlock()
		if hold != nil {
			<-hold
		}
		return 50, nil
	}
}

func (o *probeOrder) get() []string {
	o.access.Lock()
	defer o.access.Unlock()
	return append([]string(nil), o.order...)
}

func waitQueued(t *testing.T, s *HistoryStorage, want ProbeQueue) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		q := s.ProbeQueue()
		if q.Running == want.Running && q.Critical == want.Critical && q.Interactive == want.Interactive && q.Background == want.Background {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("probe queue is %+v, want %+v", q, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Probes never run more than ProbeConcurrency at a time and start at least
// ProbeSpacing apart, whoever starts them.
func TestOpenBoxProbeBudget(t *testing.T) {
	withPacing(t, 2, 30*time.Millisecond)
	s := NewHistoryStorage()
	var running, peak atomic.Int32
	var access sync.Mutex
	var starts []time.Time
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		tag := string(rune('a' + i))
		ctx := context.Background()
		if i%2 == 1 {
			ctx = WithPriority(ctx, PriorityInteractive)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.Probe(ctx, tag, "http://test", time.Minute, true, func(context.Context) (uint16, error) {
				access.Lock()
				starts = append(starts, time.Now())
				access.Unlock()
				n := running.Add(1)
				for {
					p := peak.Load()
					if n <= p || peak.CompareAndSwap(p, n) {
						break
					}
				}
				time.Sleep(60 * time.Millisecond)
				running.Add(-1)
				return 50, nil
			})
		}()
	}
	wg.Wait()
	if peak.Load() > 2 {
		t.Fatal("more probes in flight than the budget: ", peak.Load())
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i].Before(starts[j]) })
	for i := 1; i < len(starts); i++ {
		if gap := starts[i].Sub(starts[i-1]); gap < 25*time.Millisecond {
			t.Fatal("probes started too close together: ", gap)
		}
	}
	if q := s.ProbeQueue(); q.Running != 0 || q.Critical != 0 || q.Interactive != 0 || q.Background != 0 {
		t.Fatal("budget not released: ", q)
	}
}

// A freed slot goes to a waiting interactive probe before any background one;
// background probes keep their order.
func TestOpenBoxProbePriority(t *testing.T) {
	withPacing(t, 1, 0)
	s := NewHistoryStorage()
	o := &probeOrder{}
	hold := make(chan struct{})
	var wg sync.WaitGroup
	start := func(ctx context.Context, tag string, h <-chan struct{}) {
		wg.Add(1)
		go func() { defer wg.Done(); s.Probe(ctx, tag, "http://test", time.Minute, true, o.run(tag, h)) }()
	}
	start(context.Background(), "holder", hold)
	waitQueued(t, s, ProbeQueue{Running: 1})
	start(context.Background(), "bg1", nil)
	waitQueued(t, s, ProbeQueue{Running: 1, Background: 1})
	start(context.Background(), "bg2", nil)
	waitQueued(t, s, ProbeQueue{Running: 1, Background: 2})
	start(WithPriority(context.Background(), PriorityInteractive), "manual", nil)
	waitQueued(t, s, ProbeQueue{Running: 1, Interactive: 1, Background: 2})
	close(hold)
	wg.Wait()
	got := o.get()
	want := []string{"holder", "manual", "bg1", "bg2"}
	for i := range want {
		if i >= len(got) || got[i] != want[i] {
			t.Fatal("probe order ", got, " want ", want)
		}
	}
}

// An interactive caller joining a member that a background batch has queued moves
// that queued probe up, instead of waiting behind the whole background queue.
func TestOpenBoxProbeUpgrade(t *testing.T) {
	withPacing(t, 1, 0)
	s := NewHistoryStorage()
	o := &probeOrder{}
	hold := make(chan struct{})
	var wg sync.WaitGroup
	start := func(ctx context.Context, tag string, h <-chan struct{}) {
		wg.Add(1)
		go func() { defer wg.Done(); s.Probe(ctx, tag, "http://test", time.Minute, false, o.run(tag, h)) }()
	}
	start(context.Background(), "holder", hold)
	waitQueued(t, s, ProbeQueue{Running: 1})
	start(context.Background(), "bg1", nil)
	waitQueued(t, s, ProbeQueue{Running: 1, Background: 1})
	start(context.Background(), "shared", nil)
	waitQueued(t, s, ProbeQueue{Running: 1, Background: 2})
	joined := make(chan ProbeResult, 1)
	go func() {
		joined <- s.Probe(WithPriority(context.Background(), PriorityInteractive), "shared", "http://test", time.Minute, false, o.run("shared-dup", nil))
	}()
	waitQueued(t, s, ProbeQueue{Running: 1, Interactive: 1, Background: 1})
	close(hold)
	wg.Wait()
	r := <-joined
	if !r.Reused || r.Err != nil {
		t.Fatal("the interactive caller should share the queued probe's result", r)
	}
	got := o.get()
	want := []string{"holder", "shared", "bg1"}
	if len(got) != len(want) {
		t.Fatal("probe order ", got, " want ", want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatal("probe order ", got, " want ", want)
		}
	}
}

// A large background batch never holds every slot: one stays free, so a manual
// test or a recheck starts at once instead of waiting behind it.
func TestOpenBoxProbeBackgroundLeavesSlot(t *testing.T) {
	withPacing(t, 4, 0)
	s := NewHistoryStorage()
	hold := make(chan struct{})
	var wg sync.WaitGroup
	defer func() { close(hold); wg.Wait() }()
	o := &probeOrder{}
	for i := 0; i < 5; i++ {
		tag := string(rune('a' + i))
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.Probe(context.Background(), tag, "http://test", time.Minute, true, o.run(tag, hold))
		}()
	}
	waitQueued(t, s, ProbeQueue{Running: 3, Background: 2})
	done := make(chan ProbeResult, 1)
	go func() {
		done <- s.Probe(WithPriority(context.Background(), PriorityInteractive), "manual", "http://test", time.Minute, true, func(context.Context) (uint16, error) { return 30, nil })
	}()
	select {
	case r := <-done:
		if r.Err != nil {
			t.Fatal(r.Err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("a manual test waited behind a background batch")
	}
}

// Critical comes first, then interactive, then background; critical still takes a
// slot (it is inside the budget, not around it).
func TestOpenBoxProbeCriticalFirst(t *testing.T) {
	withPacing(t, 1, 0)
	s := NewHistoryStorage()
	o := &probeOrder{}
	hold := make(chan struct{})
	var wg sync.WaitGroup
	start := func(ctx context.Context, tag string, h <-chan struct{}) {
		wg.Add(1)
		go func() { defer wg.Done(); s.Probe(ctx, tag, "http://test", time.Minute, true, o.run(tag, h)) }()
	}
	start(WithPriority(context.Background(), PriorityInteractive), "holder", hold)
	waitQueued(t, s, ProbeQueue{Running: 1})
	start(context.Background(), "bg", nil)
	waitQueued(t, s, ProbeQueue{Running: 1, Background: 1})
	start(WithPriority(context.Background(), PriorityInteractive), "manual", nil)
	waitQueued(t, s, ProbeQueue{Running: 1, Interactive: 1, Background: 1})
	start(WithPriority(context.Background(), PriorityCritical), "recheck", nil)
	waitQueued(t, s, ProbeQueue{Running: 1, Critical: 1, Interactive: 1, Background: 1})
	close(hold)
	wg.Wait()
	got := o.get()
	want := []string{"holder", "recheck", "manual", "bg"}
	for i := range want {
		if i >= len(got) || got[i] != want[i] {
			t.Fatal("probe order ", got, " want ", want)
		}
	}
}

// A recheck of a member that a background batch has queued moves that queued probe
// to the front instead of waiting behind the background queue.
func TestOpenBoxProbeCriticalUpgrade(t *testing.T) {
	withPacing(t, 1, 0)
	s := NewHistoryStorage()
	o := &probeOrder{}
	hold := make(chan struct{})
	var wg sync.WaitGroup
	start := func(ctx context.Context, tag string, h <-chan struct{}) {
		wg.Add(1)
		go func() { defer wg.Done(); s.Probe(ctx, tag, "http://test", time.Minute, false, o.run(tag, h)) }()
	}
	start(context.Background(), "holder", hold)
	waitQueued(t, s, ProbeQueue{Running: 1})
	start(context.Background(), "bg1", nil)
	waitQueued(t, s, ProbeQueue{Running: 1, Background: 1})
	start(context.Background(), "node", nil)
	waitQueued(t, s, ProbeQueue{Running: 1, Background: 2})
	joined := make(chan ProbeResult, 1)
	go func() {
		joined <- s.Probe(WithPriority(context.Background(), PriorityCritical), "node", "http://test", time.Minute, true, o.run("node-dup", nil))
	}()
	waitQueued(t, s, ProbeQueue{Running: 1, Critical: 1, Background: 1})
	close(hold)
	wg.Wait()
	if r := <-joined; !r.Reused {
		t.Fatal("the recheck should share the queued probe", r)
	}
	got := o.get()
	want := []string{"holder", "node", "bg1"}
	if len(got) != len(want) {
		t.Fatal("probe order ", got, " want ", want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatal("probe order ", got, " want ", want)
		}
	}
}

// Waiting for a slot does not count against a probe's own timeout (the Clash API
// sets the timeout inside run), and a queued member whose request already ended
// still gets its turn instead of being recorded as failed.
func TestOpenBoxProbeQueueNotTimeout(t *testing.T) {
	withPacing(t, 1, 0)
	s := NewHistoryStorage()
	hold := make(chan struct{})
	go s.Probe(context.Background(), "holder", "http://test", time.Minute, true, (&probeOrder{}).run("holder", hold))
	waitQueued(t, s, ProbeQueue{Running: 1})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan ProbeResult, 1)
	go func() {
		done <- s.Probe(ctx, "queued", "http://test", time.Minute, true, func(ctx context.Context) (uint16, error) {
			probeCtx, cancelProbe := context.WithTimeout(context.WithoutCancel(ctx), 50*time.Millisecond)
			defer cancelProbe()
			select {
			case <-probeCtx.Done():
				return 0, probeCtx.Err()
			case <-time.After(10 * time.Millisecond):
				return 60, nil
			}
		})
	}()
	waitQueued(t, s, ProbeQueue{Running: 1, Background: 1})
	cancel()
	time.Sleep(150 * time.Millisecond)
	close(hold)
	select {
	case r := <-done:
		if r.Err != nil || r.Delay != 60 {
			t.Fatal("queueing counted against the probe or the cancelled request failed it", r)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("queued probe never ran")
	}
	if h := s.LoadURLTestHistory("queued"); h == nil || h.Delay != 60 {
		t.Fatal("queued probe result not recorded", h)
	}
}

// Each kernel (history storage) has its own budget.
func TestOpenBoxProbeBudgetPerStorage(t *testing.T) {
	withPacing(t, 1, time.Second)
	a, b := NewHistoryStorage(), NewHistoryStorage()
	hold := make(chan struct{})
	defer close(hold)
	go a.Probe(context.Background(), "x", "http://test", time.Minute, true, (&probeOrder{}).run("x", hold))
	waitQueued(t, a, ProbeQueue{Running: 1})
	done := make(chan struct{})
	go func() {
		b.Probe(context.Background(), "y", "http://test", time.Minute, true, func(context.Context) (uint16, error) { return 1, nil })
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("two kernels shared one probe budget")
	}
}
