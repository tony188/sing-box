package urltest

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
)

func TestOpenBoxProbeIsolationConfirmation(t *testing.T) {
	if ConfirmDelay != 10*time.Second {
		t.Fatal("production confirmation must remain ten seconds")
	}
	withConfirmDelay(t, 60*time.Millisecond)
	withPacing(t, 4, time.Millisecond)
	for _, other := range []string{"http://other", "http://test expected=204"} {
		t.Run(other, func(t *testing.T) {
			s := NewHistoryStorage()
			defer s.Close()
			defer s.RegisterGroup(&fakeGroup{selected: []string{"node"}})()
			seedProbeHistory(s, "node", "http://test", &adapter.URLTestHistory{Time: time.Now().Add(-time.Minute), Delay: 80})
			var failures atomic.Int32
			fail := func(context.Context) (uint16, error) { failures.Add(1); return 0, errDown }
			s.Probe(context.Background(), "node", "http://test", time.Minute, true, fail)
			if s.LoadProbeHistory("node", "http://test") == nil {
				t.Fatal("first failure lost the protected pass")
			}
			s.Probe(context.Background(), "node", other, time.Minute, true, func(context.Context) (uint16, error) { return 10, nil })
			waitFor(t, "scoped confirmation", func() bool { return !s.Confirming("node", "http://test") })
			if failures.Load() != 2 {
				t.Fatal("other condition replaced the second check", failures.Load())
			}
			if s.LoadProbeHistory("node", "http://test") != nil {
				t.Fatal("another condition saved the failing node")
			}
			if h := s.LoadProbeHistory("node", other); h == nil || h.Delay != 10 {
				t.Fatal("failed condition damaged other condition", h)
			}
			if h := s.LoadURLTestHistory("node"); h == nil || h.Delay != 10 {
				t.Fatal("unrelated display history was deleted", h)
			}
		})
	}
}

func TestOpenBoxProbeIsolationInUseAndConcurrentChecks(t *testing.T) {
	withConfirmDelay(t, 60*time.Millisecond)
	withPacing(t, 4, time.Millisecond)
	s := NewHistoryStorage()
	defer s.Close()
	defer s.RegisterGroup(&fakeGroup{selected: []string{"node"}, link: "http://a"})()
	for _, link := range []string{"http://a", "http://b"} {
		seedProbeHistory(s, "node", link, &adapter.URLTestHistory{Time: time.Now().Add(-time.Minute), Delay: 80})
	}
	var a, b atomic.Int32
	failA := func(context.Context) (uint16, error) { a.Add(1); return 0, errDown }
	failB := func(context.Context) (uint16, error) { b.Add(1); return 0, errDown }
	s.Probe(context.Background(), "node", "http://b", time.Minute, true, failB)
	if s.Confirming("node", "http://b") || s.LoadProbeHistory("node", "http://b") != nil {
		t.Fatal("used by A does not mean used by B")
	}
	defer s.RegisterGroup(&fakeGroup{selected: []string{"node"}, link: "http://b"})()
	seedProbeHistory(s, "node", "http://b", &adapter.URLTestHistory{Time: time.Now().Add(-time.Minute), Delay: 80})
	s.Probe(context.Background(), "node", "http://a", time.Minute, true, failA)
	s.Probe(context.Background(), "node", "http://b", time.Minute, true, failB)
	if !s.Confirming("node", "http://a") || !s.Confirming("node", "http://b") {
		t.Fatal("conditions must have independent pending checks")
	}
	waitFor(t, "both checks", func() bool { return !s.Confirming("node", "http://a") && !s.Confirming("node", "http://b") })
	if a.Load() != 2 || b.Load() != 3 {
		t.Fatal("each condition needs one confirmation", a.Load(), b.Load())
	}
}

func TestOpenBoxProbeIsolationForgetDuringProbe(t *testing.T) {
	withPacing(t, 4, time.Millisecond)
	s := NewHistoryStorage()
	defer s.Close()
	started, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		s.Probe(context.Background(), "node", "http://test", time.Minute, true, func(context.Context) (uint16, error) {
			close(started)
			<-release
			return 5, nil
		})
		close(finished)
	}()
	<-started
	s.DeleteURLTestHistory("node")
	close(release)
	<-finished
	if s.LoadProbeHistory("node", "http://test") != nil || s.LoadURLTestHistory("node") != nil {
		t.Fatal("old outbound republished its pass after replacement")
	}
	var calls int
	r := s.Probe(context.Background(), "node", "http://test", time.Hour, false, func(context.Context) (uint16, error) { calls++; return 90, nil })
	if r.Reused || calls != 1 || r.Delay != 90 {
		t.Fatal("replacement reused old probe", r)
	}
}

func TestOpenBoxProbeIsolationForgetCancelsPending(t *testing.T) {
	withConfirmDelay(t, 50*time.Millisecond)
	withPacing(t, 4, time.Millisecond)
	s := NewHistoryStorage()
	defer s.Close()
	defer s.RegisterGroup(&fakeGroup{selected: []string{"node"}})()
	seedProbeHistory(s, "node", "http://test", &adapter.URLTestHistory{Time: time.Now().Add(-time.Minute), Delay: 80})
	var calls atomic.Int32
	s.Failed("node", "http://test", func(context.Context) (uint16, error) { calls.Add(1); return 0, errDown })
	s.DeleteURLTestHistory("node")
	s.Probe(context.Background(), "node", "http://test", time.Minute, false, func(context.Context) (uint16, error) { return 15, nil })
	time.Sleep(100 * time.Millisecond)
	if calls.Load() != 0 {
		t.Fatal("old confirmation ran against replaced outbound")
	}
	if h := s.LoadProbeHistory("node", "http://test"); h == nil || h.Delay != 15 {
		t.Fatal("old confirmation erased replacement", h)
	}
}
