package urltest

import (
	"context"
	"sync"
	"time"
)

// Open-Box: one kernel-wide budget for latency probes, with priorities
// (1.14.1-openbox-tcp14).
//
// A group check used to probe its members ten at a time and start the next one
// as soon as a slot freed. On a production router a 288-member group (checked
// hourly) started well over ten new connections a second to as many servers for
// the first half minute; the carrier-grade NAT in front of the router began
// dropping new connections, so most of the batch timed out and real traffic
// stalled (direct TCP connects to a healthy node server went from 0.13 s to
// 1-2 s, with timeouts). Manual tests from the panel ran next to it, uncoordinated.
//
// Every probe that goes through HistoryStorage.Probe now takes one of
// ProbeConcurrency slots and starts at least ProbeSpacing after the previous
// start, whoever asked for it: the line does not care who opened a connection.
// Three priorities decide who gets a freed slot:
//
//   - PriorityCritical: keeping traffic flowing (the recheck of a failed node in
//     use, a failover lane recheck). Served first.
//   - PriorityInteractive: a test someone is waiting for. Served next.
//   - PriorityBackground (the default): scheduled checks, a group's own interval
//     loop. Served last, and never holds more than ProbeConcurrency-1 slots, so a
//     large background batch always leaves one slot for the other two.
//
// A probe's own timeout starts once it has its slot, so queueing never counts as
// a failure; a member queued when its group request ends still gets its turn.
// When a caller of higher priority joins a probe that is still queued (a manual
// test, or a recheck, of a node a background batch is about to test), the queued
// probe moves up to that priority instead of the caller waiting behind it.
var (
	ProbeConcurrency = 4
	ProbeSpacing     = 250 * time.Millisecond
)

type ProbePriority int

const (
	PriorityBackground ProbePriority = iota
	PriorityInteractive
	PriorityCritical
)

// ParseProbePriority maps the Clash API query value; ok is false for anything else.
func ParseProbePriority(value string) (ProbePriority, bool) {
	switch value {
	case "background":
		return PriorityBackground, true
	case "interactive":
		return PriorityInteractive, true
	case "critical":
		return PriorityCritical, true
	}
	return PriorityBackground, false
}

type priorityKey struct{}

// WithPriority sets the priority probes started under ctx get.
func WithPriority(ctx context.Context, priority ProbePriority) context.Context {
	return context.WithValue(ctx, priorityKey{}, priority)
}

func PriorityOf(ctx context.Context) ProbePriority {
	if ctx == nil {
		return PriorityBackground
	}
	if p, ok := ctx.Value(priorityKey{}).(ProbePriority); ok {
		return p
	}
	return PriorityBackground
}

type pacerTicket struct {
	grant    chan time.Time
	priority ProbePriority
	owner    string
	queued   bool
	// background: the ticket holds one of the background slots (fixed at start)
	background bool
}

type probePacer struct {
	access            sync.Mutex
	limit             int
	spacing           time.Duration
	running           int
	runningBackground int
	next              time.Time
	interactiveBurst  int
	// queues[priority], served from critical down to background
	queues    [3][]*pacerTicket
	lastOwner [3]string
}

func (s *HistoryStorage) probePacer() *probePacer {
	s.pacerOnce.Do(func() {
		n := ProbeConcurrency
		if n < 1 {
			n = 1
		}
		s.pacer = &probePacer{limit: n, spacing: ProbeSpacing}
	})
	return s.pacer
}

func (p *probePacer) backgroundLimit() int {
	if p.limit <= 1 {
		return 1
	}
	return p.limit - 1
}

func (p *probePacer) canStartLocked(priority ProbePriority) bool {
	if p.running >= p.limit {
		return false
	}
	return priority != PriorityBackground || p.runningBackground < p.backgroundLimit()
}

func (p *probePacer) reserveStartLocked() time.Time {
	now := time.Now()
	start := p.next
	if start.Before(now) {
		start = now
	}
	p.next = start.Add(p.spacing)
	return start
}

func (p *probePacer) startLocked(ticket *pacerTicket) {
	p.running++
	p.lastOwner[ticket.priority] = ticket.owner
	if ticket.priority == PriorityInteractive {
		p.interactiveBurst++
	}
	if ticket.priority == PriorityBackground {
		p.interactiveBurst = 0
	}
	ticket.background = ticket.priority == PriorityBackground
	if ticket.background {
		p.runningBackground++
	}
	ticket.queued = false
	ticket.grant <- p.reserveStartLocked()
}

// dispatchLocked starts queued tickets while there is room, highest priority first.
func (p *probePacer) popLocked(priority ProbePriority) *pacerTicket {
	q := p.queues[priority]
	if len(q) == 0 {
		return nil
	}
	owners := []string{}
	seen := map[string]bool{}
	for _, t := range q {
		if !seen[t.owner] {
			seen[t.owner] = true
			owners = append(owners, t.owner)
		}
	}
	owner := owners[0]
	for i, name := range owners {
		if name == p.lastOwner[priority] {
			owner = owners[(i+1)%len(owners)]
			break
		}
	}
	for i, t := range q {
		if t.owner == owner {
			p.queues[priority] = append(q[:i:i], q[i+1:]...)
			p.lastOwner[priority] = owner
			return t
		}
	}
	return nil
}

func (p *probePacer) dispatchLocked() {
	for p.running < p.limit {
		priority := PriorityBackground
		if len(p.queues[PriorityCritical]) > 0 {
			priority = PriorityCritical
		} else if len(p.queues[PriorityBackground]) > 0 && p.runningBackground == 0 && p.interactiveBurst >= max(2, p.limit) {
			priority = PriorityBackground
		} else if len(p.queues[PriorityInteractive]) > 0 {
			priority = PriorityInteractive
		} else if len(p.queues[PriorityBackground]) == 0 || !p.canStartLocked(PriorityBackground) {
			break
		}
		p.startLocked(p.popLocked(priority))
	}
}

// enqueue takes a slot at once when there is room and nobody of the same or a
// higher priority is waiting, otherwise queues a ticket; either way the caller
// then waits on the ticket's grant.
func (p *probePacer) enqueue(priority ProbePriority) *pacerTicket {
	return p.enqueueOwner(priority, "")
}
func (p *probePacer) enqueueOwner(priority ProbePriority, owner string) *pacerTicket {
	ticket := &pacerTicket{grant: make(chan time.Time, 1), priority: priority, owner: owner}
	p.access.Lock()
	defer p.access.Unlock()
	waitingAhead := false
	for q := priority; q <= PriorityCritical; q++ {
		if len(p.queues[q]) > 0 {
			waitingAhead = true
			break
		}
	}
	if !waitingAhead && p.canStartLocked(priority) {
		p.startLocked(ticket)
		return ticket
	}
	ticket.queued = true
	p.queues[priority] = append(p.queues[priority], ticket)
	return ticket
}

// wait blocks until the ticket holds a slot and its start time has come.
func (p *probePacer) wait(ticket *pacerTicket) {
	start := <-ticket.grant
	if d := time.Until(start); d > 0 {
		time.Sleep(d)
	}
}

// release frees the ticket's slot and hands room to whoever waits.
func (p *probePacer) release(ticket *pacerTicket) {
	p.access.Lock()
	defer p.access.Unlock()
	p.running--
	if ticket.background {
		p.runningBackground--
	}
	p.dispatchLocked()
}

// upgrade moves a ticket still queued at a lower priority to the end of the queue
// for priority, and starts it if that priority has room.
func (p *probePacer) upgrade(ticket *pacerTicket, priority ProbePriority) {
	if ticket == nil {
		return
	}
	p.access.Lock()
	defer p.access.Unlock()
	if !ticket.queued || ticket.priority >= priority {
		return
	}
	from := p.queues[ticket.priority]
	for i, t := range from {
		if t == ticket {
			p.queues[ticket.priority] = append(from[:i:i], from[i+1:]...)
			break
		}
	}
	ticket.priority = priority
	p.queues[priority] = append(p.queues[priority], ticket)
	p.dispatchLocked()
}

// ProbeQueue reports the probes running now and those waiting per priority
// (Clash API GET /openbox/probes).
type ProbeQueue struct {
	Running     int `json:"running"`
	Critical    int `json:"critical"`
	Interactive int `json:"interactive"`
	Background  int `json:"background"`
	Limit       int `json:"limit"`
}

func (s *HistoryStorage) ProbeQueue() ProbeQueue {
	if s == nil {
		return ProbeQueue{}
	}
	p := s.probePacer()
	p.access.Lock()
	defer p.access.Unlock()
	return ProbeQueue{
		Running:     p.running,
		Critical:    len(p.queues[PriorityCritical]),
		Interactive: len(p.queues[PriorityInteractive]),
		Background:  len(p.queues[PriorityBackground]),
		Limit:       p.limit,
	}
}
