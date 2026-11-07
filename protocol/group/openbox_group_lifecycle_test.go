package group

import (
	"context"
	"errors"
	"testing"
)

// Startup checks run before Touch creates a ticker. Retiring such a group must
// still stop its batch; Close cannot return early just because it was idle.
func TestOpenBoxURLTestClosedIdleStopsBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	g := &URLTestGroup{ctx: ctx, cancel: cancel, close: make(chan struct{})}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-g.close:
	default:
		t.Fatal("idle group's close notification was not delivered")
	}
	if result, err := g.urlTest(context.Background(), false, 0); !errors.Is(err, context.Canceled) || len(result) != 0 {
		t.Fatal("retired group accepted a new round", result, err)
	}
	// No manager or history is installed: a retired worker must return before
	// looking up a node or submitting another shared probe.
	b := &urlTestBatch{ctx: context.WithValue(context.Background(), probeGroupLifetimeKey{}, ctx)}
	b.probe(nil, "http://test/", g.expected, 0, false)
}
