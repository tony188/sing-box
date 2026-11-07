package urltest

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
)

// fakeGroup stands in for a URLTest group: the members it uses right now, and a
// count of how often the storage asked it to select again.
type fakeGroup struct {
	access    sync.Mutex
	selected  []string
	link      string
	reselects atomic.Int32
}

func (g *fakeGroup) SelectedTags() []string {
	g.access.Lock()
	defer g.access.Unlock()
	return append([]string(nil), g.selected...)
}

func (g *fakeGroup) ProbeLink() string {
	if g.link != "" {
		return g.link
	}
	return "http://test"
}

// Seed an already completed probe, with the same identity and history as Probe.
func seedProbeHistory(s *HistoryStorage, tag, link string, h *adapter.URLTestHistory) {
	s.probeAccess.Lock()
	if s.probes == nil {
		s.probes = make(map[probeKey]*probeEntry)
	}
	key := probeKey{tag, link}
	entry := s.probes[key]
	if entry == nil {
		entry = &probeEntry{done: make(chan struct{}), epoch: new(byte)}
		close(entry.done)
		s.probes[key] = entry
	}
	entry.history, entry.result = h, ProbeResult{Time: h.Time, Delay: h.Delay}
	s.StoreURLTestHistory(tag, h)
	s.probeAccess.Unlock()
}

func (g *fakeGroup) Reselect() { g.reselects.Add(1) }

func withConfirmDelay(t *testing.T, d time.Duration) {
	old := ConfirmDelay
	ConfirmDelay = d
	t.Cleanup(func() { ConfirmDelay = old })
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for ", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

var errDown = errors.New("down")

// The node in use fails once: the caller still sees the failure, but the record
// stays (the group does not switch) and one confirmation is scheduled; more
// failures while it is pending do not add probes.
func TestOpenBoxConfirmPendingSingleProbe(t *testing.T) {
	withConfirmDelay(t, 50*time.Millisecond)
	s := NewHistoryStorage()
	g := &fakeGroup{selected: []string{"node"}}
	defer s.RegisterGroup(g)()
	seedProbeHistory(s, "node", "http://test", &adapter.URLTestHistory{Time: time.Now().Add(-time.Minute), Delay: 80})
	var calls atomic.Int32
	fail := func(context.Context) (uint16, error) { calls.Add(1); return 0, errDown }

	r := s.Probe(context.Background(), "node", "http://test", time.Minute, true, fail)
	if r.Err == nil {
		t.Fatal("the failed probe should still report its failure to the caller")
	}
	if h := s.LoadURLTestHistory("node"); h == nil || h.Delay != 80 {
		t.Fatal("the node in use lost its record on the first failure", h)
	}
	if !s.Confirming("node", "http://test") {
		t.Fatal("no confirmation scheduled")
	}
	for i := 0; i < 5; i++ {
		if !s.Failed("node", "http://test", fail) {
			t.Fatal("a failure while pending should report the pending confirmation")
		}
	}
	waitFor(t, "confirmation", func() bool { return !s.Confirming("node", "http://test") })
	if calls.Load() != 2 {
		t.Fatal("expected the first probe and exactly one confirmation, got ", calls.Load())
	}
	if g.reselects.Load() == 0 {
		t.Fatal("groups were not asked to select after the confirmation")
	}
}

func TestOpenBoxConfirmInUsePassStoresNewResult(t *testing.T) {
	withConfirmDelay(t, 50*time.Millisecond)
	s := NewHistoryStorage()
	g := &fakeGroup{selected: []string{"node"}}
	defer s.RegisterGroup(g)()
	seedProbeHistory(s, "node", "http://test", &adapter.URLTestHistory{Time: time.Now().Add(-time.Minute), Delay: 80})
	var attempt atomic.Int32
	flaky := func(context.Context) (uint16, error) {
		if attempt.Add(1) == 1 {
			return 0, errDown
		}
		return 95, nil
	}
	s.Probe(context.Background(), "node", "http://test", time.Minute, true, flaky)
	waitFor(t, "confirmation", func() bool { return !s.Confirming("node", "http://test") })
	h := s.LoadURLTestHistory("node")
	if h == nil || h.Delay != 95 {
		t.Fatal("a passing confirmation should store its result", h)
	}
	// The cached probe result now reflects the confirmation, so a reuse within the
	// interval does not bring the old failure back.
	r := s.Probe(context.Background(), "node", "http://test", time.Minute, false, flaky)
	if !r.Reused || r.Err != nil || r.Delay != 95 {
		t.Fatal("cache still holds the failure the confirmation replaced", r)
	}
}

// Fails again on the confirmation: the record goes and the groups select again.
func TestOpenBoxConfirmInUseFailTwiceDrops(t *testing.T) {
	withConfirmDelay(t, 50*time.Millisecond)
	s := NewHistoryStorage()
	g := &fakeGroup{selected: []string{"node"}}
	defer s.RegisterGroup(g)()
	seedProbeHistory(s, "node", "http://test", &adapter.URLTestHistory{Time: time.Now().Add(-time.Minute), Delay: 80})
	var calls atomic.Int32
	fail := func(context.Context) (uint16, error) { calls.Add(1); return 0, errDown }
	s.Probe(context.Background(), "node", "http://test", time.Minute, true, fail)
	if s.LoadURLTestHistory("node") == nil {
		t.Fatal("dropped before the confirmation")
	}
	waitFor(t, "confirmation", func() bool { return !s.Confirming("node", "http://test") })
	if s.LoadURLTestHistory("node") != nil {
		t.Fatal("a node failing twice in a row kept its record")
	}
	if calls.Load() != 2 || g.reselects.Load() == 0 {
		t.Fatal("expected one confirmation probe and a reselection", calls.Load(), g.reselects.Load())
	}
}

// A failed member nobody uses loses its record at once, as upstream does: nothing
// switches because of it, and a large batch must not fire a second burst of probes.
func TestOpenBoxConfirmNotInUseDropsAtOnce(t *testing.T) {
	withConfirmDelay(t, 50*time.Millisecond)
	s := NewHistoryStorage()
	g := &fakeGroup{selected: []string{"other"}}
	defer s.RegisterGroup(g)()
	seedProbeHistory(s, "node", "http://test", &adapter.URLTestHistory{Time: time.Now(), Delay: 80})
	var calls atomic.Int32
	fail := func(context.Context) (uint16, error) { calls.Add(1); return 0, errDown }
	s.Probe(context.Background(), "node", "http://test", time.Minute, true, fail)
	if s.LoadURLTestHistory("node") != nil || s.Confirming("node", "http://test") {
		t.Fatal("a member nobody uses was protected")
	}
	time.Sleep(120 * time.Millisecond)
	if calls.Load() != 1 {
		t.Fatal("a member nobody uses was probed again", calls.Load())
	}
}

// A current selection without history must still get a bounded confirmation.
func TestOpenBoxConfirmNoHistory(t *testing.T) {
 withConfirmDelay(t,20*time.Millisecond)
 s:=NewHistoryStorage();defer s.Close()
 g:=&fakeGroup{selected:[]string{"node"}};defer s.RegisterGroup(g)()
 if !s.Failed("node","http://test",func(context.Context)(uint16,error){return 90,nil}) {t.Fatal("missing confirmation for an unmeasured current selection")}
 waitFor(t,"missing-history confirmation",func()bool{return !s.Confirming("node","http://test")})
 if g.reselects.Load()!=1||s.LoadProbeHistory("node","http://test")==nil{t.Fatal("confirmation did not publish success and reselect")}
}

// A pass recorded by someone else while the confirmation is pending wins over a
// failing confirmation.
func TestOpenBoxConfirmNewerPassWins(t *testing.T) {
	withConfirmDelay(t, 80*time.Millisecond)
	s := NewHistoryStorage()
	g := &fakeGroup{selected: []string{"node"}}
	defer s.RegisterGroup(g)()
	seedProbeHistory(s, "node", "http://test", &adapter.URLTestHistory{Time: time.Now().Add(-time.Minute), Delay: 80})
	s.Failed("node", "http://test", func(context.Context) (uint16, error) { return 0, errDown })
	time.Sleep(20 * time.Millisecond)
	seedProbeHistory(s, "node", "http://test", &adapter.URLTestHistory{Time: time.Now(), Delay: 70})
	waitFor(t, "confirmation", func() bool { return !s.Confirming("node", "http://test") })
	if h := s.LoadURLTestHistory("node"); h == nil || h.Delay != 70 {
		t.Fatal("a newer pass was deleted by an older failure's confirmation", h)
	}
}

// Closing the storage (kernel stopping) cancels pending confirmations.
func TestOpenBoxConfirmCloseCancels(t *testing.T) {
	withConfirmDelay(t, 80*time.Millisecond)
	s := NewHistoryStorage()
	g := &fakeGroup{selected: []string{"node"}}
	s.RegisterGroup(g)
	seedProbeHistory(s, "node", "http://test", &adapter.URLTestHistory{Time: time.Now(), Delay: 80})
	var calls atomic.Int32
	s.Failed("node", "http://test", func(context.Context) (uint16, error) { calls.Add(1); return 0, errDown })
	_ = s.Close()
	time.Sleep(160 * time.Millisecond)
	if calls.Load() != 0 {
		t.Fatal("a confirmation ran after the storage closed")
	}
}

// An unregistered group (closed, or replaced by a running kernel) no longer protects
// its old selection.
func TestOpenBoxConfirmUnregister(t *testing.T) {
	withConfirmDelay(t, 50*time.Millisecond)
	s := NewHistoryStorage()
	g := &fakeGroup{selected: []string{"node"}}
	unregister := s.RegisterGroup(g)
	unregister()
	seedProbeHistory(s, "node", "http://test", &adapter.URLTestHistory{Time: time.Now(), Delay: 80})
	if s.Failed("node", "http://test", func(context.Context) (uint16, error) { return 0, errDown }) {
		t.Fatal("an unregistered group still protected its selection")
	}
	if s.LoadURLTestHistory("node") != nil {
		t.Fatal("record kept without any group using it")
	}
}

// The recheck runs with a live context even though the request that failed is
// long gone; the run function sets its own timeout.
func TestOpenBoxConfirmLiveContext(t *testing.T) {
	withConfirmDelay(t, 30*time.Millisecond)
	s := NewHistoryStorage()
	g := &fakeGroup{selected: []string{"node"}}
	defer s.RegisterGroup(g)()
	seedProbeHistory(s, "node", "http://test", &adapter.URLTestHistory{Time: time.Now(), Delay: 80})
	var attempt atomic.Int32
	run := func(ctx context.Context) (uint16, error) {
		if attempt.Add(1) == 1 {
			<-ctx.Done()
			return 0, ctx.Err()
		}
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		return 90, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	s.Probe(ctx, "node", "http://test", time.Minute, true, run)
	cancel()
	waitFor(t, "confirmation", func() bool { return !s.Confirming("node", "http://test") })
	if h := s.LoadURLTestHistory("node"); h == nil || h.Delay != 90 {
		t.Fatal("the recheck did not run with a live context", h)
	}
}

// The panel's failover recheck and the kernel's own recheck both come about ten
// seconds after the same failure. Whichever runs first probes; the other takes that
// result ("newer than the failure"), so the member is probed once, not twice.
func TestOpenBoxConfirmSharedWithLaterRecheck(t *testing.T) {
	withConfirmDelay(t, 40*time.Millisecond)
	s := NewHistoryStorage()
	g := &fakeGroup{selected: []string{"node"}}
	defer s.RegisterGroup(g)()
	seedProbeHistory(s, "node", "http://test", &adapter.URLTestHistory{Time: time.Now().Add(-time.Minute), Delay: 80})
	var calls atomic.Int32
	fail := func(context.Context) (uint16, error) { calls.Add(1); return 0, errDown }
	failed := s.Probe(context.Background(), "node", "http://test", time.Minute, true, fail)
	waitFor(t, "kernel recheck", func() bool { return !s.Confirming("node", "http://test") })
	// the panel's recheck arrives after the kernel's finished
	r := s.Probe(WithNewerThan(WithPriority(context.Background(), PriorityCritical), failed.Time), "node", "http://test", time.Minute, true, fail)
	if !r.Reused || r.Err == nil {
		t.Fatal("the later recheck should take the kernel's recheck result", r)
	}
	if calls.Load() != 2 {
		t.Fatal("member probed ", calls.Load(), " times, want the failure plus one recheck")
	}
}

func TestOpenBoxConfirmSharedWithEarlierRecheck(t *testing.T) {
	withConfirmDelay(t, 120*time.Millisecond)
	s := NewHistoryStorage()
	g := &fakeGroup{selected: []string{"node"}}
	defer s.RegisterGroup(g)()
	seedProbeHistory(s, "node", "http://test", &adapter.URLTestHistory{Time: time.Now().Add(-time.Minute), Delay: 80})
	var calls atomic.Int32
	fail := func(context.Context) (uint16, error) { calls.Add(1); return 0, errDown }
	failed := s.Probe(context.Background(), "node", "http://test", time.Minute, true, fail)
	// the panel's recheck arrives before the kernel's timer fires
	time.Sleep(20 * time.Millisecond)
	s.Probe(WithNewerThan(WithPriority(context.Background(), PriorityCritical), failed.Time), "node", "http://test", time.Minute, true, fail)
	waitFor(t, "kernel recheck", func() bool { return !s.Confirming("node", "http://test") })
	if calls.Load() != 2 {
		t.Fatal("member probed ", calls.Load(), " times, want the failure plus one recheck")
	}
	if s.LoadURLTestHistory("node") != nil {
		t.Fatal("failed twice in a row (failure, then the shared recheck) but kept its record")
	}
}

func TestOpenBoxConfirmJoinsRecheckInFlight(t *testing.T) {
	withConfirmDelay(t, 40*time.Millisecond)
	s := NewHistoryStorage()
	g := &fakeGroup{selected: []string{"node"}}
	defer s.RegisterGroup(g)()
	seedProbeHistory(s, "node", "http://test", &adapter.URLTestHistory{Time: time.Now().Add(-time.Minute), Delay: 80})
	var calls atomic.Int32
	slowFail := func(context.Context) (uint16, error) {
		calls.Add(1)
		time.Sleep(120 * time.Millisecond)
		return 0, errDown
	}
	failed := s.Probe(context.Background(), "node", "http://test", time.Minute, true, slowFail)
	// the kernel's recheck starts 40 ms later and takes 120 ms; the panel's arrives in between
	time.Sleep(90 * time.Millisecond)
	r := s.Probe(WithNewerThan(WithPriority(context.Background(), PriorityCritical), failed.Time), "node", "http://test", time.Minute, true, slowFail)
	if !r.Reused {
		t.Fatal("the recheck in flight should be joined", r)
	}
	waitFor(t, "kernel recheck", func() bool { return !s.Confirming("node", "http://test") })
	if calls.Load() != 2 {
		t.Fatal("member probed ", calls.Load(), " times, want the failure plus one recheck")
	}
}

// recheckSince is what the panel's and the App's failover recheck really sends: the
// failure it saw as unix milliseconds (all the Clash API's result time gives it),
// turned back into a time the way the Clash API does. The failure itself completed
// inside that millisecond, so it must not count as newer than itself (tcp21).
func recheckSince(failure time.Time) context.Context {
	return WithNewerThan(WithPriority(context.Background(), PriorityCritical), time.UnixMilli(failure.UnixMilli()))
}

// failOffMillisecond: a storage whose group uses `selected`, "node" with a passing
// record, failed once. Starts over until the failure completed off a millisecond
// boundary (nearly always the first try), as real ones do: a caller's millisecond
// `since` is then earlier than the failure itself.
func failOffMillisecond(t *testing.T, selected string, run func(context.Context) (uint16, error)) (*HistoryStorage, ProbeResult) {
	t.Helper()
	for attempt := 0; attempt < 50; attempt++ {
		s := NewHistoryStorage()
		unregister := s.RegisterGroup(&fakeGroup{selected: []string{selected}})
		seedProbeHistory(s, "node", "http://test", &adapter.URLTestHistory{Time: time.Now().Add(-time.Minute), Delay: 80})
		failed := s.Probe(context.Background(), "node", "http://test", time.Minute, true, run)
		if failed.Time.Nanosecond()%int(time.Millisecond) != 0 {
			t.Cleanup(func() { unregister(); _ = s.Close() })
			return s, failed
		}
		unregister()
		_ = s.Close()
	}
	t.Fatal("every failure completed on a millisecond boundary")
	return nil, ProbeResult{}
}

// A member no URL test group uses gets no recheck from the kernel: the failover
// recheck is its only second test and must really probe it. tcp14–tcp20 handed the
// failure back as the recheck, so the lane switched without testing the node again.
func TestOpenBoxConfirmRecheckSinceMillisecondNotInUse(t *testing.T) {
	withPacing(t, 4, time.Millisecond)
	withConfirmDelay(t, 40*time.Millisecond)
	var calls atomic.Int32
	fail := func(context.Context) (uint16, error) { calls.Add(1); return 0, errDown }
	s, failed := failOffMillisecond(t, "other", fail)
	before := calls.Load()
	r := s.Probe(recheckSince(failed.Time), "node", "http://test", time.Minute, true, fail)
	if r.Reused || calls.Load() != before+1 {
		t.Fatal("the recheck got back the failure it was rechecking instead of probing the member", r)
	}
}

// The member in use: arriving before the kernel's own recheck, the failover recheck
// probes, and the kernel's recheck takes that result — one probe, not none.
func TestOpenBoxConfirmRecheckSinceMillisecondBeforeKernel(t *testing.T) {
	withPacing(t, 4, time.Millisecond)
	withConfirmDelay(t, 200*time.Millisecond)
	var calls atomic.Int32
	fail := func(context.Context) (uint16, error) { calls.Add(1); return 0, errDown }
	s, failed := failOffMillisecond(t, "node", fail)
	before := calls.Load()
	if !s.Confirming("node", "http://test") {
		t.Fatal("the kernel's recheck should still be pending")
	}
	r := s.Probe(recheckSince(failed.Time), "node", "http://test", time.Minute, true, fail)
	if r.Reused {
		t.Fatal("the recheck ahead of the kernel's got back the failure it was rechecking", r)
	}
	waitFor(t, "kernel recheck", func() bool { return !s.Confirming("node", "http://test") })
	if calls.Load() != before+1 {
		t.Fatal("member probed ", calls.Load()-before, " times after the failure, want one shared recheck")
	}
	if s.LoadURLTestHistory("node") != nil {
		t.Fatal("failed twice in a row (the failure, then the shared recheck) but kept its record")
	}
}

// Arriving after the kernel's recheck, the failover recheck takes that result.
func TestOpenBoxConfirmRecheckSinceMillisecondAfterKernel(t *testing.T) {
	withPacing(t, 4, time.Millisecond)
	withConfirmDelay(t, 40*time.Millisecond)
	var calls atomic.Int32
	fail := func(context.Context) (uint16, error) { calls.Add(1); return 0, errDown }
	s, failed := failOffMillisecond(t, "node", fail)
	before := calls.Load()
	waitFor(t, "kernel recheck", func() bool { return !s.Confirming("node", "http://test") })
	r := s.Probe(recheckSince(failed.Time), "node", "http://test", time.Minute, true, fail)
	if !r.Reused || r.Err == nil || calls.Load() != before+1 {
		t.Fatal("the later recheck should take the kernel's recheck result", r, calls.Load()-before)
	}
}
