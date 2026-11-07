package urltest

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/sagernet/sing/service"
)

// Redirect only the socket in this fixture, so HTTP Host/path and the requested
// destination still expose an unexpected protocol or fallback URL.
type openBoxHTTPDialer struct {
	address     string
	mu          sync.Mutex
	destination M.Socksaddr
}

func TestOpenBoxHTTPFallbackMeasuresOnlySuccessfulRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			time.Sleep(180 * time.Millisecond)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	delay, err := URLTest(context.Background(), server.URL, &openBoxHTTPDialer{address: server.Listener.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	if delay >= 150 {
		t.Fatalf("GET latency includes rejected HEAD request: %dms", delay)
	}
}

func (d *openBoxHTTPDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	d.mu.Lock()
	d.destination = destination
	d.mu.Unlock()
	return (&net.Dialer{}).DialContext(ctx, network, d.address)
}

func (*openBoxHTTPDialer) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, errors.New("unexpected UDP probe")
}

type openBoxHTTPCertificates struct {
	adapter.CertificateStore
	pool *x509.CertPool
}

func (c openBoxHTTPCertificates) Pool() *x509.CertPool { return c.pool }

func TestOpenBoxHTTPProbe(t *testing.T) {
	for _, secure := range []bool{false, true} {
		t.Run(map[bool]string{false: "HTTP", true: "HTTPS"}[secure], func(t *testing.T) {
			requests := make(chan *http.Request, 1)
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests <- r
				w.WriteHeader(http.StatusNoContent)
			})
			var server *httptest.Server
			ctx := context.Background()
			if secure {
				server = httptest.NewTLSServer(handler)
				pool := x509.NewCertPool()
				pool.AddCert(server.Certificate())
				ctx = service.ContextWith[adapter.CertificateStore](ctx, openBoxHTTPCertificates{pool: pool})
			} else {
				server = httptest.NewServer(handler)
			}
			defer server.Close()
			dialer := &openBoxHTTPDialer{address: server.Listener.Addr().String()}
			delay, err := URLTest(ctx, server.URL+"/custom-204?source=clash", dialer)
			if err != nil || delay == 0 {
				t.Fatalf("successful probe: delay=%d err=%v", delay, err)
			}
			request := <-requests
			if request.Method != http.MethodHead || request.URL.RequestURI() != "/custom-204?source=clash" {
				t.Fatalf("unexpected request: %s %s", request.Method, request.URL)
			}
		})
	}
}

func TestOpenBoxHTTPDefaultAndRedirect(t *testing.T) {
	requests := make(chan *http.Request, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r
		w.Header().Set("Location", "https://must-not-follow.invalid/")
		w.WriteHeader(http.StatusFound)
	}))
	defer server.Close()
	dialer := &openBoxHTTPDialer{address: server.Listener.Addr().String()}
	delay, err := URLTest(context.Background(), "", dialer)
	if err != nil || delay == 0 {
		t.Fatalf("default HTTP probe: delay=%d err=%v", delay, err)
	}
	request := <-requests
	if request.Host != "www.gstatic.com" || request.URL.Path != "/generate_204" || dialer.destination.Port != 80 {
		t.Fatalf("wrong default: host=%s path=%s destination=%s", request.Host, request.URL.Path, dialer.destination)
	}
	if len(requests) != 0 {
		t.Fatal("probe followed redirect")
	}
}

func TestOpenBoxHTTPTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	delay, err := URLTest(ctx, server.URL, &openBoxHTTPDialer{address: server.Listener.Addr().String()})
	if err == nil || delay != 0 {
		t.Fatalf("timeout: delay=%d err=%v", delay, err)
	}
}

// #586: a site may implement GET while rejecting HEAD. Only method errors
// permit one retry; status policy and redirect behavior stay unchanged.
func TestOpenBoxHTTPMethodFallback(t *testing.T) {
	for _, tc := range []struct {
		head, get, calls int
		pass             bool
	}{
		{200, 200, 1, true}, {403, 200, 1, false}, {404, 200, 1, false},
		{405, 200, 2, true}, {501, 204, 2, true}, {405, 403, 2, false},
		{405, 405, 2, false}, {405, 302, 2, true},
	} {
		t.Run(fmt.Sprintf("HEAD%d_GET%d", tc.head, tc.get), func(t *testing.T) {
			var requests []string
			var mu sync.Mutex
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				requests = append(requests, r.Method+" "+r.URL.RequestURI())
				mu.Unlock()
				w.Header().Set("Connection", "close")
				code := tc.head
				if r.Method == http.MethodGet {
					code = tc.get
				}
				if code == 302 {
					w.Header().Set("Location", "/must-not-follow")
				}
				w.WriteHeader(code)
			}))
			defer server.Close()
			expected, _ := ParseExpectedStatus("200-399")
			delay, err := URLTestExpected(context.Background(), server.URL+"/check?source=test", expected, &openBoxHTTPDialer{address: server.Listener.Addr().String()})
			if tc.pass != (err == nil && delay > 0) {
				t.Fatalf("delay=%d err=%v", delay, err)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(requests) != tc.calls || requests[0] != "HEAD /check?source=test" || (tc.calls == 2 && requests[1] != "GET /check?source=test") {
				t.Fatalf("requests=%v", requests)
			}
			if !tc.pass {
				status := tc.head
				if tc.calls == 2 {
					status = tc.get
				}
				if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("unexpected status %d", status)) {
					t.Fatalf("wrong failure: %v", err)
				}
			}
		})
	}
}

func TestOpenBoxHTTPFallbackDoesNotReadBody(t *testing.T) {
	closed := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(405)
			return
		}
		w.Header().Set("Content-Length", "104857600")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		// Deliberately never send a body: the probe must close immediately.
		<-r.Context().Done()
		close(closed)
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	expected, _ := ParseExpectedStatus("200")
	if delay, err := URLTestExpected(ctx, server.URL, expected, &openBoxHTTPDialer{address: server.Listener.Addr().String()}); err != nil || delay == 0 {
		t.Fatalf("delay=%d err=%v", delay, err)
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("GET body not closed")
	}
}

func TestOpenBoxHTTPFallbackHonorsCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(405)
			return
		}
		<-r.Context().Done()
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	expected, _ := ParseExpectedStatus("200")
	if _, err := URLTestExpected(ctx, server.URL, expected, &openBoxHTTPDialer{address: server.Listener.Addr().String()}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want original deadline: %v", err)
	}
}
