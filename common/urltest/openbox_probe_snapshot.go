package urltest

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync/atomic"
	"time"
)

var probeIdentity atomic.Uint64

func newProbeIdentity() uint64 { return probeIdentity.Add(1) }
func newProbeInstance() string {
	return fmt.Sprintf("%x-%x", time.Now().UnixNano(), newProbeIdentity())
}

type httpStatusKey struct{}

func captureHTTPStatus(ctx context.Context, status int) {
	if p, ok := ctx.Value(httpStatusKey{}).(*int); ok {
		*p = status
	}
}

type ProbeCondition struct {
	Tag      string `json:"tag"`
	URL      string `json:"url"`
	Expected string `json:"expected"`
}

// Confirmation is retained with its probe entry, never in a separate unbounded log.
type Confirmation struct {
	FailedAt     int64    `json:"failedAt"`
	DueAt        int64    `json:"dueAt"`
	StartedAt    int64    `json:"startedAt,omitempty"`
	FinishedAt   int64    `json:"finishedAt,omitempty"`
	ReselectedAt int64    `json:"reselectedAt,omitempty"`
	Stage        string   `json:"stage"`
	Outcome      string   `json:"outcome,omitempty"`
	Reused       bool     `json:"reused,omitempty"`
	Before       []string `json:"before,omitempty"`
	After        []string `json:"after,omitempty"`
}

type ProbeSample struct {
	ProbeCondition
	Generation   uint64        `json:"generation"`
	At           int64         `json:"at"`
	OK           *bool         `json:"ok"`
	Delay        uint16        `json:"delay,omitempty"`
	Status       int           `json:"status,omitempty"`
	Error        string        `json:"error,omitempty"`
	Pending      bool          `json:"pending"`
	Confirmation *Confirmation `json:"confirmation,omitempty"`
}

type ProbeSnapshot struct {
	Schema   int           `json:"schema"`
	Revision uint64        `json:"revision"`
	Instance string        `json:"instance"`
	Results  []ProbeSample `json:"results"`
}

// Snapshot is read-only: it neither creates entries nor refreshes sample times.
// Callers request their exact conditions, including conditions not yet measured.
func (s *HistoryStorage) Snapshot(conditions []ProbeCondition) ProbeSnapshot {
	out := ProbeSnapshot{Schema: 1, Results: make([]ProbeSample, 0, len(conditions))}
	if s == nil {
		return out
	}
	s.probeAccess.Lock()
	defer s.probeAccess.Unlock()
	out.Instance = s.probeInstance
	out.Revision = s.probeRevision
	r := s.confirmState()
	r.access.Lock()
	defer r.access.Unlock()
	for _, condition := range conditions {
		expected, err := ParseExpectedStatus(condition.Expected)
		if err != nil {
			continue
		} // HTTP handler rejects invalid policies before calling us.
		condition.Expected = expected.String()
		key := probeKey{condition.Tag, probeLinkDefault(ProbeLink(condition.URL, expected))}
		item := ProbeSample{ProbeCondition: condition}
		if entry := s.probes[key]; entry != nil {
			item.Generation = entry.generation
			result := entry.result
			if errors.Is(result.Err, context.Canceled) {
				result = entry.previous
			}
			select {
			case <-entry.done:
			default:
				item.Pending = true
				result = entry.previous
			}
			if !result.Time.IsZero() {
				item.At = result.Time.UnixMilli()
				item.Delay = result.Delay
				item.Status = result.Status
				ok := result.Err == nil && result.Delay > 0
				item.OK = &ok
				if result.Err != nil {
					item.Error = result.Err.Error()
					if len(item.Error) > 512 {
						item.Error = item.Error[:512]
					}
				}
			}
		}
		if c, ok := r.events[key]; ok {
			copy := c
			copy.Before = append([]string(nil), c.Before...)
			copy.After = append([]string(nil), c.After...)
			item.Confirmation = &copy
		}
		out.Results = append(out.Results, item)
	}
	return out
}

func (s *HistoryStorage) selectedFor(link string) []string {
	r := s.confirmState()
	r.access.Lock()
	groups := make([]ConfirmGroup, 0, len(r.groups))
	for _, g := range r.groups {
		groups = append(groups, g)
	}
	r.access.Unlock()
	seen := map[string]bool{}
	var tags []string
	for _, g := range groups {
		if probeLinkDefault(g.ProbeLink()) == link {
			for _, tag := range g.SelectedTags() {
				if !seen[tag] {
					seen[tag] = true
					tags = append(tags, tag)
				}
			}
		}
	}
	sort.Strings(tags)
	return tags
}

func (s *HistoryStorage) markConfirmation(key probeKey, cancel chan struct{}, stage string) {
	r := s.confirmState()
	r.access.Lock()
	defer r.access.Unlock()
	if r.confirming[key] != cancel {
		return
	}
	c := r.events[key]
	c.Stage = stage
	if stage == "probing" {
		c.StartedAt = time.Now().UnixMilli()
	}
	r.events[key] = c
}
func (s *HistoryStorage) finishConfirmation(key probeKey, cancel chan struct{}, result ProbeResult, before, after []string) {
	r := s.confirmState()
	r.access.Lock()
	defer r.access.Unlock()
	if r.confirming[key] != cancel {
		return
	}
	c := r.events[key]
	c.Stage = "done"
	c.FinishedAt = time.Now().UnixMilli()
	c.ReselectedAt = c.FinishedAt
	c.Outcome = "passed"
	if result.Err != nil || result.Delay == 0 {
		c.Outcome = "failed"
	}
	c.Reused = result.Reused
	c.Before = before
	c.After = after
	r.events[key] = c
}
