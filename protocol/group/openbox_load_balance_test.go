package group

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	O "github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/urltest"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

type balanceTestLeaf struct {
	adapter.Outbound
	tag        string
	down       atomic.Bool
	peers      chan net.Conn
	dial       func(context.Context) (net.Conn, error)
	dialTarget func(context.Context, M.Socksaddr) (net.Conn, error)
	packet     func(context.Context) (net.PacketConn, error)
}

func (o *balanceTestLeaf) Tag() string       { return o.tag }
func (o *balanceTestLeaf) Type() string      { return "shadowsocks" }
func (o *balanceTestLeaf) Network() []string { return []string{N.NetworkTCP, N.NetworkUDP} }
func (o *balanceTestLeaf) DialContext(ctx context.Context, _ string, dst M.Socksaddr) (net.Conn, error) {
	if o.down.Load() {
		return nil, errors.New("refused")
	}
	if o.dial != nil {
		return o.dial(ctx)
	}
	if o.dialTarget != nil {
		return o.dialTarget(ctx, dst)
	}
	x, y := net.Pipe()
	o.peers <- y
	return x, nil
}
func (o *balanceTestLeaf) ListenPacket(ctx context.Context, _ M.Socksaddr) (net.PacketConn, error) {
	if o.down.Load() {
		return nil, errors.New("refused")
	}
	if o.packet != nil {
		return o.packet(ctx)
	}
	return net.ListenPacket("udp", "127.0.0.1:0")
}
func balanceFixture(t *testing.T, n int) (*LoadBalance, []*balanceTestLeaf) {
	t.Helper()
	s := &LoadBalance{Adapter: O.NewAdapter(LoadBalanceType, "balance", []string{N.NetworkTCP, N.NetworkUDP}, nil), interval: time.Minute, timeout: time.Second}
	leaves := make([]*balanceTestLeaf, n)
	for i := range leaves {
		leaves[i] = &balanceTestLeaf{tag: fmt.Sprintf("node-%d", i), peers: make(chan net.Conn, 256)}
		s.tags = append(s.tags, leaves[i].tag)
		s.members = append(s.members, balanceMember{outbound: leaves[i], state: balanceUnknown})
	}
	t.Cleanup(func() {
		s.Close()
		for _, leaf := range leaves {
			close(leaf.peers)
			for conn := range leaf.peers {
				conn.Close()
			}
		}
	})
	return s, leaves
}
func balanceDial(t *testing.T, s *LoadBalance) (net.Conn, string) {
	t.Helper()
	metadata := &adapter.InboundContext{OutboundChain: []adapter.Outbound{s}}
	c, err := s.DialContext(adapter.WithContext(context.Background(), metadata), N.NetworkTCP, M.ParseSocksaddr("same-target.example:443"))
	if err != nil {
		t.Fatal(err)
	}
	return c, metadata.OutboundChain[len(metadata.OutboundChain)-1].Tag()
}
func TestOpenBoxLoadBalanceSameTargetRotatesAndReserves(t *testing.T) {
	s, _ := balanceFixture(t, 10)
	counts := map[string]int{}
	var lock sync.Mutex
	var wg sync.WaitGroup
	connections := make(chan net.Conn, 100)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, tag := balanceDial(t, s)
			connections <- c
			lock.Lock()
			counts[tag]++
			lock.Unlock()
		}()
	}
	wg.Wait()
	for _, tag := range s.tags {
		if counts[tag] != 10 {
			t.Fatalf("unbalanced concurrent selection: %v", counts)
		}
	}
	// Read-only status / membership queries must not consume the rotation.
	before := s.cursor
	for i := 0; i < 100; i++ {
		s.Status()
		s.All()
	}
	if before != s.cursor {
		t.Fatal("status read selected a node")
	}
	close(connections)
	for c := range connections {
		c.Close()
		c.Close()
	}
	for _, status := range s.Status() {
		if status.Active != 0 {
			t.Fatalf("leaked reservation: %+v", status)
		}
	}
	seen := map[string]bool{}
	for i := 0; i < 10; i++ {
		c, tag := balanceDial(t, s)
		seen[tag] = true
		c.Close()
	}
	if len(seen) != 10 {
		t.Fatalf("same target was pinned: %v", seen)
	}
}
func TestOpenBoxLoadBalancePrefersIdle(t *testing.T) {
	s, _ := balanceFixture(t, 3)
	a, ta := balanceDial(t, s)
	defer a.Close()
	b, tb := balanceDial(t, s)
	b.Close()
	c, tc := balanceDial(t, s)
	defer c.Close()
	if ta == tb || ta == tc || tb == tc {
		t.Fatal("first rotation repeated a node")
	}
	d, td := balanceDial(t, s)
	defer d.Close()
	if td != tb {
		t.Fatalf("chose busy node %s instead of idle %s", td, tb)
	}
}
func TestOpenBoxLoadBalanceNineDownEverySurvivorAndNoDirect(t *testing.T) {
	for survivor := 0; survivor < 10; survivor++ {
		t.Run(fmt.Sprint(survivor), func(t *testing.T) {
			s, leaves := balanceFixture(t, 10)
			for i, l := range leaves {
				l.down.Store(i != survivor)
			}
			for request := 0; request < 20; request++ {
				c, tag := balanceDial(t, s)
				if tag != leaves[survivor].tag {
					t.Fatal(tag)
				}
				c.Close()
			}
			leaves[survivor].down.Store(true)
			if c, err := s.DialContext(context.Background(), N.NetworkTCP, M.ParseSocksaddr("same-target.example:443")); err == nil {
				c.Close()
				t.Fatal("all down must fail closed")
			}
		})
	}
}
func TestOpenBoxLoadBalanceClosePreservesExistingAndCancellation(t *testing.T) {
	s, leaves := balanceFixture(t, 2)
	c, tag := balanceDial(t, s)
	defer c.Close()
	var peer net.Conn
	for _, leaf := range leaves {
		if leaf.tag == tag {
			peer = <-leaf.peers
		}
	}
	defer peer.Close()
	s.Close()
	done := make(chan error, 1)
	go func() { _, err := c.Write([]byte("alive")); done <- err }()
	buf := make([]byte, 5)
	if _, err := io.ReadFull(peer, buf); err != nil || string(buf) != "alive" {
		t.Fatal(err, string(buf))
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := s.DialContext(context.Background(), N.NetworkTCP, M.ParseSocksaddr("example.org:443")); err == nil {
		t.Fatal("closed group dialled")
	}
	s2, _ := balanceFixture(t, 2)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s2.DialContext(ctx, N.NetworkTCP, M.ParseSocksaddr("example.org:443")); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	for _, m := range s2.Status() {
		if m.Active != 0 || m.Healthy != nil {
			t.Fatal("cancel changed health", m)
		}
	}
}
func TestOpenBoxLoadBalanceBoundsAndRecovery(t *testing.T) {
	s, leaves := balanceFixture(t, 10)
	for _, leaf := range leaves {
		leaf.dial = func(ctx context.Context) (net.Conn, error) { <-ctx.Done(); return nil, ctx.Err() }
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := s.DialContext(ctx, N.NetworkTCP, M.ParseSocksaddr("example.org:443")); err == nil {
		t.Fatal("black holes passed")
	}
	if time.Since(start) > time.Second {
		t.Fatal("unbounded dial")
	}
	for _, m := range s.Status() {
		if m.Active != 0 {
			t.Fatal("failed dial retained a reservation")
		}
	}
	// A new successful health result admits a recovered node without rebuilding.
	leaves[0].dial = nil
	s.access.Lock()
	s.members[0].failed[0] = time.Time{}
	s.members[0].checked = time.Now()
	s.members[0].state = balanceHealthy
	s.access.Unlock()
	c, tag := balanceDial(t, s)
	defer c.Close()
	if tag != leaves[0].tag {
		t.Fatal(tag)
	}
}

func TestOpenBoxLoadBalanceHealthySurvivorGetsRemainingBudget(t *testing.T) {
	s, leaves := balanceFixture(t, 64)
	for i := 1; i < len(s.members); i++ {
		s.members[i].checked = time.Now()
		s.members[i].state = balanceDown
	}
	leaves[0].dial = func(ctx context.Context) (net.Conn, error) {
		select {
		case <-time.After(20 * time.Millisecond):
			a, b := net.Pipe()
			leaves[0].peers <- b
			return a, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	c, err := s.DialContext(ctx, N.NetworkTCP, M.ParseSocksaddr("same-target.example:443"))
	if err != nil {
		t.Fatalf("excluded nodes consumed survivor dial budget: %v", err)
	}
	c.Close()
}

// Adding healthy nodes must not shorten each transport's handshake allowance.
// With 64 candidates the old division gave each only 7ms of a 500ms deadline,
// rejecting every healthy 80ms handshake and marking the whole pool as failed.
func TestOpenBoxLoadBalanceLargePoolKeepsHandshakeBudget(t *testing.T) {
	for _, count := range []int{2, 10, 58, 64} {
		for _, network := range []string{N.NetworkTCP, N.NetworkUDP} {
			t.Run(fmt.Sprintf("%s/%d", network, count), func(t *testing.T) {
				s, leaves := balanceFixture(t, count)
				for _, leaf := range leaves {
					leaf := leaf
					leaf.dial = func(ctx context.Context) (net.Conn, error) {
						select {
						case <-time.After(80 * time.Millisecond):
							a, b := net.Pipe()
							leaf.peers <- b
							return a, nil
						case <-ctx.Done():
							return nil, ctx.Err()
						}
					}
					leaf.packet = func(ctx context.Context) (net.PacketConn, error) {
						select {
						case <-time.After(80 * time.Millisecond):
							return net.ListenPacket("udp", "127.0.0.1:0")
						case <-ctx.Done():
							return nil, ctx.Err()
						}
					}
				}
				ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
				defer cancel()
				var conn io.Closer
				var err error
				if network == N.NetworkTCP {
					conn, err = s.DialContext(ctx, network, M.ParseSocksaddr("same-target.example:443"))
				} else {
					conn, err = s.ListenPacket(ctx, M.ParseSocksaddr("same-target.example:443"))
				}
				if err != nil {
					t.Fatalf("%d healthy members lost their handshake budget: %v", count, err)
				}
				conn.Close()
				for _, status := range s.Status() {
					if status.DialFailedAt != 0 || status.Active != 0 {
						t.Fatalf("healthy handshake polluted pool state: %+v", status)
					}
				}
			})
		}
	}
}

func TestOpenBoxLoadBalancePrefersConfirmedHealthyOverUnknown(t *testing.T) {
	s, _ := balanceFixture(t, 10)
	s.members[9].state = balanceHealthy
	s.members[9].active[0] = 3
	i, _, _ := s.reserve(N.NetworkTCP, nil)
	if i != 9 {
		t.Fatalf("untested idle node displaced a known healthy node: %d", i)
	}
	s.release(i, N.NetworkTCP, false)
}

func TestOpenBoxLoadBalanceHedgesStalledTransport(t *testing.T) {
	for _, network := range []string{N.NetworkTCP, N.NetworkUDP} {
		t.Run(network, func(t *testing.T) {
			s, leaves := balanceFixture(t, 2)
			cancelled := make(chan struct{})
			leaves[0].dial = func(ctx context.Context) (net.Conn, error) {
				<-ctx.Done()
				close(cancelled)
				return nil, ctx.Err()
			}
			leaves[0].packet = func(ctx context.Context) (net.PacketConn, error) {
				<-ctx.Done()
				close(cancelled)
				return nil, ctx.Err()
			}
			ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
			defer cancel()
			start := time.Now()
			var conn io.Closer
			var err error
			if network == N.NetworkTCP {
				conn, err = s.DialContext(ctx, network, M.ParseSocksaddr("same-target.example:443"))
			} else {
				conn, err = s.ListenPacket(ctx, M.ParseSocksaddr("same-target.example:443"))
			}
			if err != nil {
				t.Fatalf("stalled first member blocked healthy second: %v", err)
			}
			conn.Close()
			if time.Since(start) > 650*time.Millisecond {
				t.Fatal("waited for the stalled member's full deadline")
			}
			select {
			case <-cancelled:
			case <-time.After(time.Second):
				t.Fatal("losing attempt was not cancelled")
			}
			for _, status := range s.Status() {
				if status.Active != 0 || status.DialFailedAt != 0 {
					t.Fatalf("hedging leaked or penalized cancellation: %+v", status)
				}
			}
		})
	}
}

func TestOpenBoxLoadBalanceBoundsSpeculativeConcurrency(t *testing.T) {
	s, leaves := balanceFixture(t, 64)
	var running, peak atomic.Int32
	var done sync.WaitGroup
	for _, leaf := range leaves {
		leaf.dial = func(ctx context.Context) (net.Conn, error) {
			done.Add(1)
			defer done.Done()
			active := running.Add(1)
			defer running.Add(-1)
			for previous := peak.Load(); active > previous && !peak.CompareAndSwap(previous, active); previous = peak.Load() {
			}
			<-ctx.Done()
			return nil, ctx.Err()
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancel()
	if conn, err := s.DialContext(ctx, N.NetworkTCP, M.ParseSocksaddr("same-target.example:443")); err == nil {
		conn.Close()
		t.Fatal("blackhole succeeded")
	}
	done.Wait()
	if peak.Load() < 2 || peak.Load() > 4 || running.Load() != 0 {
		t.Fatalf("unexpected speculative concurrency: peak=%d running=%d", peak.Load(), running.Load())
	}
	for _, status := range s.Status() {
		if status.Active != 0 || status.DialFailedAt != 0 {
			t.Fatalf("cancelled speculative dial changed member state: %+v", status)
		}
	}
}

func TestOpenBoxLoadBalanceClosesSuccessfulLosingAttempt(t *testing.T) {
	s, leaves := balanceFixture(t, 2)
	ready := make(chan struct{})
	for i, leaf := range leaves {
		i, leaf := i, leaf
		leaf.dial = func(context.Context) (net.Conn, error) {
			if i == 0 {
				<-ready
			} else {
				close(ready)
			}
			a, b := net.Pipe()
			leaf.peers <- b
			return a, nil
		}
	}
	c, _ := balanceDial(t, s)
	c.Close()
	for _, leaf := range leaves {
		select {
		case peer := <-leaf.peers:
			peer.SetReadDeadline(time.Now().Add(time.Second))
			if _, err := peer.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
				t.Fatalf("successful losing transport was not closed: %v", err)
			}
			peer.Close()
		case <-time.After(time.Second):
			t.Fatal("expected both overlapping handshakes")
		}
	}
	for _, status := range s.Status() {
		if status.Active != 0 || status.DialFailedAt != 0 {
			t.Fatalf("successful losing attempt leaked state: %+v", status)
		}
	}
}
func TestOpenBoxLoadBalanceUDPAssociationAndClose(t *testing.T) {
	s, _ := balanceFixture(t, 10)
	var connections []net.PacketConn
	for i := 0; i < 10; i++ {
		c, err := s.ListenPacket(context.Background(), M.ParseSocksaddr("example.org:443"))
		if err != nil {
			t.Fatal(err)
		}
		connections = append(connections, c)
	}
	for _, m := range s.Status() {
		if m.Active != 1 {
			t.Fatal("UDP sessions concentrated", m)
		}
	}
	for _, c := range connections {
		c.Close()
		c.Close()
	}
	for _, m := range s.Status() {
		if m.Active != 0 {
			t.Fatal("UDP reservation leak", m)
		}
	}
}

func TestOpenBoxLoadBalanceUDPExcludesKnownDownMembers(t *testing.T) {
	s, _ := balanceFixture(t, 10)
	for i := range s.members {
		s.members[i].checked = time.Now()
		s.members[i].state = balanceDown
		if i == 9 {
			s.members[i].state = balanceHealthy
		}
	}
	c, err := s.ListenPacket(context.Background(), M.ParseSocksaddr("example.org:443"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for i, status := range s.Status() {
		if (i == 9 && status.Active != 1) || (i != 9 && status.Active != 0) {
			t.Fatal("UDP selected an unhealthy node", status)
		}
	}
}

// Compile-time check that packet routing never flattens balancing into one node.
func TestOpenBoxLoadBalanceNotFlattened(t *testing.T) {
	s, _ := balanceFixture(t, 1)
	if _, ok := any(s).(adapter.OutboundGroup); ok {
		t.Fatal("router will bypass the balancing dialer")
	}
}

func TestOpenBoxLoadBalanceTargetFailureDoesNotDisablePool(t *testing.T) {
	s, leaves := balanceFixture(t, 3)
	for _, leaf := range leaves {
		leaf.down.Store(true)
	}
	_, _ = s.DialContext(context.Background(), N.NetworkTCP, M.ParseSocksaddr("broken-target.example:443"))
	// The next target can be reached even during the temporary dial cooldown.
	for _, leaf := range leaves {
		leaf.down.Store(false)
	}
	c, _ := balanceDial(t, s)
	defer c.Close()
}

func TestOpenBoxLoadBalanceHealthStateAndFreshRecovery(t *testing.T) {
	s, _ := balanceFixture(t, 2)
	base := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	sample := func(offset int, ok bool) urltest.ProbeResult {
		r := urltest.ProbeResult{Time: base.Add(time.Duration(offset) * time.Second), Delay: 12}
		if !ok {
			r.Err = errors.New("unavailable")
			r.Delay = 0
		}
		return r
	}
	s.finishCheck(0, sample(0, true), base)
	if s.members[0].state != balanceHealthy || s.members[0].nextCheck.Sub(base) != time.Minute {
		t.Fatal(s.Status())
	}
	s.finishCheck(0, sample(1, false), base.Add(time.Second))
	if s.members[0].state != balanceHealthy || s.members[0].failures != 1 || s.members[0].nextCheck.Sub(base.Add(time.Second)) != 5*time.Second {
		t.Fatal(s.Status())
	}
	s.finishCheck(0, sample(2, false), base.Add(2*time.Second))
	if s.members[0].state != balanceDown {
		t.Fatal(s.Status())
	}
	// Even after the old interval has elapsed, a confirmed failure is excluded.
	s.members[0].checked = base.Add(-24 * time.Hour)
	i, _, _ := s.reserve(N.NetworkTCP, nil)
	if i != 1 {
		t.Fatal("old failure expired into the pool", s.Status())
	}
	s.release(i, N.NetworkTCP, false)
	s.finishCheck(0, sample(3, true), base.Add(3*time.Second))
	if s.members[0].state != balanceRecovering || s.members[0].successes != 1 || *s.Status()[0].Healthy {
		t.Fatal(s.Status())
	}
	cached := sample(3, true)
	cached.Reused = true
	s.finishCheck(0, cached, base.Add(9*time.Second))
	if s.members[0].state != balanceRecovering || s.members[0].successes != 1 {
		t.Fatal("cached success counted twice", s.Status())
	}
	s.finishCheck(0, sample(4, false), base.Add(10*time.Second))
	if s.members[0].state != balanceDown || s.members[0].successes != 0 {
		t.Fatal("flapping member admitted", s.Status())
	}
	s.finishCheck(0, sample(5, true), base.Add(11*time.Second))
	s.finishCheck(0, sample(6, true), base.Add(12*time.Second))
	if s.members[0].state != balanceHealthy || !*s.Status()[0].Healthy {
		t.Fatal(s.Status())
	}
}

func TestOpenBoxLoadBalanceDownNeverFallsBackUntilRecovery(t *testing.T) {
	s, _ := balanceFixture(t, 1)
	s.members[0].state = balanceDown
	s.members[0].checked = time.Now().Add(-48 * time.Hour)
	s.members[0].failed[0] = time.Now().Add(-48 * time.Hour)
	for _, network := range []string{N.NetworkTCP, N.NetworkUDP} {
		if _, member, _ := s.reserve(network, nil); member != nil {
			t.Fatal("confirmed down member selected", s.Status())
		}
	}
	s.members[0].state = balanceRecovering
	if _, member, _ := s.reserve(N.NetworkTCP, nil); member != nil {
		t.Fatal("unconfirmed recovery selected")
	}
}

func TestOpenBoxLoadBalanceStalePassAndCancellationDoNotConfirmRecovery(t *testing.T) {
	s, _ := balanceFixture(t, 1)
	base := time.Now().Add(-time.Minute).Truncate(time.Millisecond)
	s.members[0].state = balanceDown
	s.members[0].failed[0] = base.Add(2 * time.Second)
	s.finishCheck(0, urltest.ProbeResult{Time: base.Add(time.Second), Delay: 5}, base.Add(3*time.Second))
	if s.members[0].state != balanceDown || !s.members[0].checked.IsZero() {
		t.Fatal("pre-failure pass readmitted member", s.Status())
	}
	s.finishCheck(0, urltest.ProbeResult{Time: base.Add(4 * time.Second), Err: context.Canceled}, base.Add(4*time.Second))
	if s.members[0].failures != 0 || s.members[0].successes != 0 || !s.members[0].checked.IsZero() {
		t.Fatal("cancellation changed health", s.Status())
	}
	s.finishCheck(0, urltest.ProbeResult{Time: base.Add(5 * time.Second), Delay: 5}, base.Add(5*time.Second))
	if s.members[0].state != balanceRecovering || s.members[0].failed[0].IsZero() {
		t.Fatal("cleared dial failure before confirmed recovery", s.Status())
	}
	s.finishCheck(0, urltest.ProbeResult{Time: base.Add(6 * time.Second), Delay: 5}, base.Add(6*time.Second))
	if s.members[0].state != balanceHealthy || !s.members[0].failed[0].IsZero() {
		t.Fatal(s.Status())
	}
}

func TestOpenBoxLoadBalanceIndependentCheckSchedule(t *testing.T) {
	s, _ := balanceFixture(t, 3)
	now := time.Now().Truncate(time.Millisecond)
	s.members[0].checking = true // A slow in-flight probe never reserves another slot.
	s.members[1].nextCheck = now.Add(30 * time.Second)
	s.members[2].state = balanceDown
	s.members[2].nextCheck = now.Add(-time.Second)
	i, _ := s.reserveCheck(now)
	if i != 2 || !s.members[2].checking {
		t.Fatal("due recovery blocked by another member", i)
	}
	i, wait := s.reserveCheck(now)
	if i != -1 || wait != 30*time.Second {
		t.Fatal("duplicate probe or busy timer", i, wait)
	}
	// A repeated cached failure has no extra fall vote.
	r := urltest.ProbeResult{Time: now, Err: errors.New("down")}
	s.finishCheck(2, r, now)
	s.finishCheck(2, r, now.Add(time.Second))
	if s.members[2].failures != 1 || s.members[2].nextCheck.Sub(now) != 5*time.Second {
		t.Fatal(s.Status())
	}
	if s.checkInterval(&balanceMember{state: balanceHealthy}) != time.Minute || s.checkInterval(&balanceMember{state: balanceRecovering}) != 5*time.Second {
		t.Fatal("normal and recovery intervals were mixed")
	}
}
