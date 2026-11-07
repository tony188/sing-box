package group

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
	"github.com/sagernet/sing/common/bufio"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
)

func TestOpenBoxLoadBalanceTargetIsolationAndRecovery(t *testing.T) {
	s, _ := balanceFixture(t, 2)
	dst := M.ParseSocksaddr("cdn.example:443")
	now := time.Now().Truncate(time.Millisecond)
	s.recordTargetFailure(0, dst, now, true)
	// The generic URL probe stays successful, but it cannot readmit this target.
	s.finishCheck(0, urltest.ProbeResult{Time: now.Add(time.Second), Delay: 10}, now.Add(time.Second))
	i, _, _ := s.reserve(N.NetworkTCP, nil, dst)
	if i != 1 {
		t.Fatal("generic success cleared target failure", i)
	}
	s.release(i, N.NetworkTCP, false)
	// Other targets and ports retain the node and ordinary rotation.
	for _, other := range []string{"other.example:443", "cdn.example:80"} {
		s.cursor = 0
		i, _, _ = s.reserve(N.NetworkTCP, nil, M.ParseSocksaddr(other))
		if i != 0 {
			t.Fatal("failure escaped destination scope", other, i)
		}
		s.release(i, N.NetworkTCP, false)
	}
	check, _ := s.reserveTargetCheck(now.Add(time.Second))
	if check == nil {
		t.Fatal("missing target check")
	}
	pass := urltest.ProbeResult{Time: now.Add(2 * time.Second), Delay: 20}
	s.finishTargetCheck(check, pass, now.Add(2*time.Second))
	i, _, _ = s.reserve(N.NetworkTCP, map[int]bool{1: true}, dst)
	if i != -1 {
		t.Fatal("single recovery pass readmitted target")
	}
	// Re-reading the same pass must not count as the second success.
	check, _ = s.reserveTargetCheck(now.Add(8 * time.Second))
	s.finishTargetCheck(check, pass, now.Add(8*time.Second))
	check, _ = s.reserveTargetCheck(now.Add(14 * time.Second))
	if check == nil {
		t.Fatal("recovery not scheduled")
	}
	s.finishTargetCheck(check, urltest.ProbeResult{Time: now.Add(15 * time.Second), Delay: 20}, now.Add(15*time.Second))
	s.cursor = 0
	i, _, _ = s.reserve(N.NetworkTCP, nil, dst)
	if i != 0 {
		t.Fatal("confirmed recovery not readmitted")
	}
	s.release(i, N.NetworkTCP, false)
}

func TestOpenBoxLoadBalanceTargetStaleProbeAndCapacity(t *testing.T) {
	s, _ := balanceFixture(t, 1)
	dst := M.ParseSocksaddr("cdn.example:443")
	now := time.Now().Truncate(time.Millisecond)
	s.recordTargetFailure(0, dst, now, true)
	check, _ := s.reserveTargetCheck(now)
	s.recordTargetFailure(0, dst, now.Add(time.Second), true)
	s.finishTargetCheck(check, urltest.ProbeResult{Time: now.Add(2 * time.Second), Delay: 5}, now.Add(2*time.Second))
	// A newer business failure invalidates a probe which started before it.
	check, _ = s.reserveTargetCheck(now.Add(3 * time.Second))
	s.finishTargetCheck(check, urltest.ProbeResult{Time: now.Add(4 * time.Second), Err: context.Canceled}, now.Add(4*time.Second))
	for j := 0; j < balanceTargetLimit+10; j++ {
		s.recordTargetFailure(0, M.ParseSocksaddr(fmt.Sprintf("cdn-%d.example:443", j)), now, true)
	}
	if len(s.targets) > balanceTargetLimit {
		t.Fatal("unbounded destination state", len(s.targets))
	}
	if i, _, _ := s.reserve(N.NetworkTCP, nil, dst); i != -1 {
		t.Fatal("capacity evicted an unconfirmed failure")
	}
	// Age alone never readmits a fault, even when idle probes are suspended.
	_, wait := s.reserveTargetCheck(now.Add(24 * time.Hour))
	if wait <= 0 {
		t.Fatal("idle target scheduler spins")
	}
}

func TestOpenBoxLoadBalanceTargetCopyErrorClassification(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		download               bool
		err                    error
		closed, canceled, want bool
	}{
		{"remote read reset", true, &net.OpError{Op: "read", Err: syscall.ECONNRESET}, false, false, true},
		{"remote write reset", false, &net.OpError{Op: "write", Err: syscall.EPIPE}, false, false, true},
		{"client write reset", true, &net.OpError{Op: "write", Err: syscall.ECONNRESET}, false, false, false},
		{"client read reset", false, &net.OpError{Op: "read", Err: syscall.ECONNRESET}, false, false, false},
		{"normal EOF", true, io.EOF, false, false, false},
		{"normal close", true, net.ErrClosed, false, false, false},
		{"local close", true, &net.OpError{Op: "read", Err: syscall.ECONNRESET}, true, false, false},
		{"caller canceled", true, &net.OpError{Op: "read", Err: syscall.ECONNRESET}, false, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := balanceFixture(t, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c, _ := balanceDial(t, s)
			b := c.(*balanceConn)
			b.ctx = ctx
			if tc.closed {
				c.Close()
			}
			if tc.canceled {
				cancel()
			}
			b.ReportCopyResult(tc.download, 32, tc.err)
			if (len(s.targets) > 0) != tc.want {
				t.Fatal("wrong fault attribution", len(s.targets))
			}
			c.Close()
		})
	}
}

func TestOpenBoxLoadBalanceTargetAllDownAndNoPoisonOnSuccess(t *testing.T) {
	s, _ := balanceFixture(t, 10)
	dst := M.ParseSocksaddr("same-target.example:443")
	for j := 0; j < 9; j++ {
		s.recordTargetFailure(j, dst, time.Now(), true)
	}
	for j := 0; j < 20; j++ {
		c, tag := balanceDial(t, s)
		if tag != "node-9" {
			t.Fatal(tag)
		}
		c.Close()
	}
	s.recordTargetFailure(9, dst, time.Now(), true)
	if c, err := s.DialContext(context.Background(), N.NetworkTCP, dst); err == nil {
		c.Close()
		t.Fatal("target failure fell back")
	}
	if _, _, err := net.SplitHostPort(dst.String()); err != nil {
		t.Fatal(err)
	}
}

func TestOpenBoxLoadBalanceLatencyPreferenceKeepsLoadFirst(t *testing.T) {
	s, _ := balanceFixture(t, 3)
	now := time.Now()
	for i, d := range []uint16{300, 50, 60} {
		s.finishCheck(i, urltest.ProbeResult{Time: now, Delay: d}, now)
	}
	// The slow idle node is skipped while both low-latency nodes are also idle.
	s.cursor = 0
	a, ta := balanceDial(t, s)
	defer a.Close()
	b, tb := balanceDial(t, s)
	defer b.Close()
	if ta != "node-1" || tb != "node-2" {
		t.Fatal("latency tie window did not rotate", ta, tb)
	}
	// The third simultaneous connection uses the idle slower node, rather than
	// concentrating another connection on a low-latency but already-busy node.
	c, tc := balanceDial(t, s)
	defer c.Close()
	if tc != "node-0" {
		t.Fatal("latency overrode real-time load", tc)
	}
	for i := range s.members {
		s.members[i].checked = now.Add(-time.Hour)
	}
	s.cursor = 0
	d, td := balanceDial(t, s)
	defer d.Close()
	if td != "node-0" {
		t.Fatal("stale latency affected selection", td)
	}
}

func TestOpenBoxLoadBalanceLatencySmoothing(t *testing.T) {
	s, _ := balanceFixture(t, 1)
	now := time.Now()
	s.finishCheck(0, urltest.ProbeResult{Time: now, Delay: 100}, now)
	s.finishCheck(0, urltest.ProbeResult{Time: now.Add(time.Second), Delay: 300}, now.Add(time.Second))
	if s.members[0].delay != 150 {
		t.Fatal("single spike replaced smoothed delay", s.members[0].delay)
	}
}

func TestOpenBoxLoadBalanceTargetProbeThroughRealHTTP(t *testing.T) {
	s, leaves := balanceFixture(t, 2)
	s.history = urltest.NewHistoryStorage()
	t.Cleanup(func() { s.history.Close() })
	s.timeout = 80 * time.Millisecond
	var bad atomic.Bool
	bad.Store(true)
	var heads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "HEAD" || r.URL.Path != "/" && r.URL.Path != "/generate_204" || r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
			t.Error("unsafe probe", r.Method, r.URL)
		}
		heads.Add(1)
		w.WriteHeader(204)
	}))
	defer server.Close()
	address := strings.TrimPrefix(server.URL, "http://")
	for i, leaf := range leaves {
		i, leaf := i, leaf
		leaf.dialTarget = func(ctx context.Context, dst M.Socksaddr) (net.Conn, error) {
			if i == 0 && dst.Fqdn == "cdn.example" && bad.Load() {
				a, b := net.Pipe()
				leaf.peers <- b
				go io.Copy(io.Discard, b)
				return a, nil
			}
			return (&net.Dialer{}).DialContext(ctx, "tcp", address)
		}
	}
	dst := M.ParseSocksaddr("cdn.example:80")
	// A successful ordinary URL probe must coexist with failure at the CDN.
	delay, err := urltest.URLTestExpected(context.Background(), "http://www.gstatic.com/generate_204", urltest.ExpectedStatus{}, leaves[0])
	if err != nil {
		t.Fatal(err)
	}
	s.finishCheck(0, urltest.ProbeResult{Time: time.Now(), Delay: delay}, time.Now())
	s.recordTargetFailure(0, dst, time.Now(), false)
	check, _ := s.reserveTargetCheck(time.Now())
	s.checkTarget(context.Background(), check)
	if i, _, _ := s.reserve(N.NetworkTCP, map[int]bool{1: true}, dst); i != -1 {
		t.Fatal("target HTTP timeout did not exclude node")
	}
	if s.members[0].state != balanceHealthy {
		t.Fatal("target failure changed generic health")
	}
	if h := s.history.LoadURLTestHistory(leaves[0].Tag()); h != nil {
		t.Fatal("target probe overwrote display latency", h)
	}
	bad.Store(false)
	for pass := 0; pass < 2; pass++ {
		check, _ = s.reserveTargetCheck(time.Now().Add(6 * time.Second))
		if check == nil {
			t.Fatal("missing recovery")
		}
		s.checkTarget(context.Background(), check)
		if pass == 0 {
			if i, _, _ := s.reserve(N.NetworkTCP, map[int]bool{1: true}, dst); i != -1 {
				t.Fatal("recovered too soon")
			}
		}
	}
	if heads.Load() != 3 {
		t.Fatal("unexpected probe/replay count", heads.Load())
	}
	if len(s.targets) != 0 {
		t.Fatal("fresh successful recovery not admitted")
	}
}

func TestOpenBoxLoadBalanceFirstResponseWatchSurvivesUnwrap(t *testing.T) {
	s, leaves := balanceFixture(t, 1)
	s.timeout = 20 * time.Millisecond
	c, _ := balanceDial(t, s)
	defer c.Close()
	peer := <-leaves[0].peers
	defer peer.Close()
	go io.Copy(io.Discard, peer)
	// The first write is observed, then this copy upgrades to the raw transport.
	if _, err := bufio.Copy(c, strings.NewReader("request")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		s.access.Lock()
		n := len(s.targets)
		s.access.Unlock()
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("copy fast path bypassed first-response observation")
		}
		time.Sleep(time.Millisecond)
	}
	// Suspicion alone must not exclude a normally idle application or close it.
	i, _, _ := s.reserve(N.NetworkTCP, nil, M.ParseSocksaddr("same-target.example:443"))
	if i != 0 {
		t.Fatal("watchdog imposed a new application timeout")
	}
	s.release(i, N.NetworkTCP, false)
	if _, err := c.Write([]byte("still open")); err != nil {
		t.Fatal("watchdog closed live transport", err)
	}
	go peer.Write([]byte("response"))
	if _, err := io.ReadFull(c, make([]byte, 8)); err != nil {
		t.Fatal(err)
	}
	r, rc := N.UnwrapCountReader(c, nil)
	w, wc := N.UnwrapCountWriter(c, nil)
	if r == c || w == c || len(rc) != 0 || len(wc) != 0 || N.NeedHandshakeForRead(c) || N.NeedHandshakeForWrite(c) {
		t.Fatal("bulk copy retained observation overhead")
	}
}

func BenchmarkOpenBoxLoadBalanceReserve(b *testing.B) {
	s := &LoadBalance{interval: time.Minute}
	for i := 0; i < 64; i++ {
		s.members = append(s.members, balanceMember{outbound: &balanceTestLeaf{tag: fmt.Sprint(i)}, state: balanceHealthy, delay: uint32(50 + i), checked: time.Now()})
	}
	dst := M.ParseSocksaddr("cdn.example:443")
	b.ReportAllocs()
	b.ResetTimer()
	for j := 0; j < b.N; j++ {
		i, _, _ := s.reserve(N.NetworkTCP, nil, dst)
		s.release(i, N.NetworkTCP, false)
	}
}

type balanceTestRoots struct {
	adapter.CertificateStore
	pool *x509.CertPool
}

func (r balanceTestRoots) Pool() *x509.CertPool { return r.pool }
func TestOpenBoxLoadBalanceTargetHTTPSProbeAndHTTPStatus(t *testing.T) {
	s, leaves := balanceFixture(t, 1)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(403) }))
	defer server.Close()
	leaves[0].dialTarget = func(ctx context.Context, _ M.Socksaddr) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", server.Listener.Addr().String())
	}
	pool := x509.NewCertPool()
	pool.AddCert(server.Certificate())
	ctx := service.ContextWith[adapter.CertificateStore](context.Background(), balanceTestRoots{pool: pool})
	dst := M.ParseSocksaddr("example.com:443")
	if _, err := balanceProbeTarget(ctx, leaves[0], dst); err != nil {
		t.Fatal("HTTP rejection mistaken for unreachable target", err)
	}
	if _, err := balanceProbeTarget(context.Background(), leaves[0], dst); !errors.Is(err, context.Canceled) {
		t.Fatal("certificate verification changed health or was bypassed", err)
	}
	pinned := M.ParseSocksaddr("203.0.113.8:443")
	leaves[0].dialTarget = func(ctx context.Context, got M.Socksaddr) (net.Conn, error) {
		if got != pinned {
			t.Error("resolved target IP lost during recovery", got)
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp", server.Listener.Addr().String())
	}
	if _, err := balanceProbeTarget(ctx, leaves[0], dst, pinned); err != nil {
		t.Fatal("resolved target lost HTTP Host/TLS SNI", err)
	}
	_ = s
}

func TestOpenBoxLoadBalanceResolvedDestinationKeepsApplicationHost(t *testing.T) {
	ip := M.ParseSocksaddr("203.0.113.8:443")
	for _, m := range []*adapter.InboundContext{
		{Destination: M.ParseSocksaddr("cdn.example:443")},
		{Destination: ip, Domain: "cdn.example"},
	} {
		if got := balanceApplicationDestination(adapter.WithContext(context.Background(), m), ip); got != M.ParseSocksaddr("cdn.example:443") {
			t.Fatal("lost original host", got)
		}
	}
	if got := balanceApplicationDestination(context.Background(), ip); got != ip {
		t.Fatal("invented host", got)
	}
}

func TestOpenBoxLoadBalanceUDPZeroLengthDatagramPreserved(t *testing.T) {
	s, leaves := balanceFixture(t, 1)
	listener, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	leaves[0].dial = func(context.Context) (net.Conn, error) { return net.Dial("udp", listener.LocalAddr().String()) }
	c, err := s.DialContext(context.Background(), N.NetworkUDP, M.ParseSocksaddr(listener.LocalAddr().String()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err = c.Write(nil); err != nil {
		t.Fatal(err)
	}
	listener.SetReadDeadline(time.Now().Add(time.Second))
	if n, _, err := listener.ReadFrom(make([]byte, 1)); err != nil || n != 0 {
		t.Fatal("empty UDP datagram dropped", n, err)
	}
}

func TestOpenBoxLoadBalanceRecoveryVotesResetOnNewStreamFailure(t *testing.T) {
	s,_:=balanceFixture(t,1);dst:=M.ParseSocksaddr("cdn.example:443");now:=time.Now()
	s.recordTargetFailure(0,dst,now,true)
	check,_:=s.reserveTargetCheck(now)
	s.finishTargetCheck(check,urltest.ProbeResult{Time:now.Add(time.Second),Delay:5},now.Add(time.Second))
	s.recordTargetFailure(0,dst,now.Add(2*time.Second),false)
	check,_=s.reserveTargetCheck(now.Add(7*time.Second))
	s.finishTargetCheck(check,urltest.ProbeResult{Time:now.Add(8*time.Second),Delay:5},now.Add(8*time.Second))
	if i,_,_:=s.reserve(N.NetworkTCP,nil,dst);i!=-1 {t.Fatal("recovery reused a pass from before the latest stream failure")}
}
