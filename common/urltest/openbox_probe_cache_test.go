package urltest

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestOpenBoxProbeCacheBoundedAcrossURLs(t *testing.T) {
	withPacing(t, 4, 0)
	s := NewHistoryStorage()
	defer s.Close()
	defer s.RegisterGroup(&fakeGroup{selected: []string{"node"}, link: "http://test/0"})()
	for i := 0; i < 4300; i++ {
		s.Probe(context.Background(), "node", fmt.Sprintf("http://test/%d", i), time.Minute, false, func(context.Context) (uint16, error) { return 5, nil })
	}
	if len(s.probes) > 4096 {
		t.Fatalf("finished probe cache retained %d distinct URL entries", len(s.probes))
	}
	if s.LoadProbeHistory("node", "http://test/4299") == nil {
		t.Fatal("newest probe was evicted")
	}
	if s.LoadProbeHistory("node", "http://test/0") == nil {
		t.Fatal("active group's cached pass was evicted")
	}
}
