package urltest

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestOpenBoxProbeSnapshotReadOnlyAndGeneration(t *testing.T) {
	withPacing(t, 4, 0)
	s := NewHistoryStorage()
	defer s.Close()
	conditions := []ProbeCondition{{Tag: "n", URL: "http://a", Expected: "204"}, {Tag: "n", URL: "http://a", Expected: "403"}, {Tag: "unknown", URL: "http://b"}}
	empty := s.Snapshot(conditions)
	if len(s.probes) != 0 || len(empty.Results) != 3 || empty.Results[0].OK != nil {
		t.Fatal("snapshot created cache entries")
	}
	status, _ := ParseExpectedStatus("204")
	link := ProbeLink("http://a", status)
	pass := s.Probe(context.Background(), "n", link, time.Hour, true, func(ctx context.Context) (uint16, error) { captureHTTPStatus(ctx, 204); return 30, nil })
	failure, _ := ParseExpectedStatus("403")
	s.Probe(context.Background(), "n", ProbeLink("http://a", failure), time.Hour, true, func(ctx context.Context) (uint16, error) {
		captureHTTPStatus(ctx, 500)
		return 0, errors.New("unexpected HTTP status 500")
	})
	first := s.Snapshot(conditions)
	second := s.Snapshot(conditions)
	if first.Instance == "" || first.Results[0].At != pass.Time.UnixMilli() || second.Results[0].At != first.Results[0].At {
		t.Fatal("cache read refreshed completion time")
	}
	if first.Results[0].OK == nil || !*first.Results[0].OK || first.Results[0].Status != 204 {
		t.Fatal("pass missing", first)
	}
	if first.Results[1].OK == nil || *first.Results[1].OK || first.Results[1].Status != 500 || first.Results[2].OK != nil {
		t.Fatal("failed/unknown or expected-status isolation lost", first)
	}
	s.DeleteURLTestHistory("n")
	forgot := s.Snapshot(conditions)
	if forgot.Revision <= first.Revision || forgot.Results[0].OK != nil {
		t.Fatal("outbound replacement retained old result")
	}
	s.Probe(context.Background(), "n", link, time.Hour, true, func(context.Context) (uint16, error) { return 40, nil })
	if s.Snapshot(conditions).Results[0].Generation == first.Results[0].Generation {
		t.Fatal("replacement reused generation")
	}
}
func TestOpenBoxProbeSnapshotDuringInflight(t *testing.T) {
	withPacing(t, 4, 0)
	s := NewHistoryStorage()
	defer s.Close()
	original := s.Probe(context.Background(), "n", "http://a", time.Hour, true, func(context.Context) (uint16, error) { return 10, nil })
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		s.Probe(context.Background(), "n", "http://a", time.Hour, true, func(context.Context) (uint16, error) { close(started); <-release; return 20, nil })
		close(done)
	}()
	<-started
	r := s.Snapshot([]ProbeCondition{{Tag: "n", URL: "http://a"}}).Results[0]
	close(release)
	<-done
	if !r.Pending || r.OK == nil || !*r.OK || r.At != original.Time.UnixMilli() {
		t.Fatal("pending probe lost real previous completion", r)
	}
}
func TestOpenBoxProbeFairOwners(t *testing.T) {
	p := &probePacer{limit: 1}
	first := p.enqueueOwner(PriorityInteractive, "A")
	p.wait(first)
	tickets := []*pacerTicket{}
	for i := 0; i < 12; i++ {
		tickets = append(tickets, p.enqueueOwner(PriorityInteractive, "A"))
	}
	b := p.enqueueOwner(PriorityInteractive, "B")
	c := p.enqueueOwner(PriorityInteractive, "C")
	p.release(first)
	p.wait(b)
	p.release(b)
	p.wait(tickets[0])
	p.release(tickets[0])
	tickets = tickets[1:]
	p.wait(c)
	p.release(c)
	// All of B/C must start before the queued A batch drains.
	for _, ticket := range tickets {
		p.wait(ticket)
		p.release(ticket)
	}
	if p.running != 0 {
		t.Fatal("slot leaked")
	}
}
