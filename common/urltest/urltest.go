package urltest

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-mux"
	"github.com/sagernet/sing-snell"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/ntp"
	"github.com/sagernet/sing/common/observable"
)

type HistoryStorage struct {
 bandwidthOnce sync.Once
 bandwidth *BandwidthState
	probeInstance string
	probeRevision uint64
	probeClosed   bool
	probeAccess   sync.Mutex
	probes        map[probeKey]*probeEntry
	access        sync.RWMutex
	delayHistory  map[string]*adapter.URLTestHistory
	updateHooks   []*observable.Subscriber[struct{}]
	confirmOnce   sync.Once
	confirm       *confirmRegistry
	pacerOnce     sync.Once
	pacer         *probePacer
}

func NewHistoryStorage() *HistoryStorage {
	return &HistoryStorage{
		probeInstance: newProbeInstance(),
		delayHistory:  make(map[string]*adapter.URLTestHistory),
	}
}

func (s *HistoryStorage) AddUpdateHook(hook *observable.Subscriber[struct{}]) {
	s.access.Lock()
	defer s.access.Unlock()
	s.updateHooks = append(s.updateHooks, hook)
}

func (s *HistoryStorage) NotifyUpdated() {
	s.access.RLock()
	defer s.access.RUnlock()
	s.notifyUpdated()
}

func (s *HistoryStorage) LoadURLTestHistory(tag string) *adapter.URLTestHistory {
	if s == nil {
		return nil
	}
	s.access.RLock()
	defer s.access.RUnlock()
	return s.delayHistory[tag]
}

func (s *HistoryStorage) DeleteURLTestHistory(tag string) {
	// Hot replacement/removal invalidates every condition and any old in-flight result.
	s.probeAccess.Lock()
	s.probeRevision++
	for key, entry := range s.probes {
		if key.tag == tag {
			if entry.invalidated != nil {
				close(entry.invalidated)
			}
			delete(s.probes, key)
		}
	}
	s.cancelConfirm(tag)
	s.access.Lock()
	delete(s.delayHistory, tag)
	s.notifyUpdated()
	s.access.Unlock()
	s.probeAccess.Unlock()
}

func (s *HistoryStorage) StoreURLTestHistory(tag string, history *adapter.URLTestHistory) {
	s.access.Lock()
	s.delayHistory[tag] = history
	s.notifyUpdated()
	s.access.Unlock()
}

func (s *HistoryStorage) notifyUpdated() {
	for _, updateHook := range s.updateHooks {
		updateHook.Emit(struct{}{})
	}
}

func (s *HistoryStorage) Close() error {
	s.probeAccess.Lock()
	s.probeClosed = true
	for key, entry := range s.probes {
		if entry.invalidated != nil {
			close(entry.invalidated)
		}
		delete(s.probes, key)
	}
	s.probeAccess.Unlock()
	s.closeConfirm()
	s.access.Lock()
	defer s.access.Unlock()
	s.updateHooks = nil
	return nil
}

func URLTest(ctx context.Context, link string, detour N.Dialer) (uint16, error) {
	return URLTestExpected(ctx, link, ExpectedStatus{}, detour)
}

// URLTestExpected is URLTest with the statuses that count as a pass (Open-Box
// tcp19, expected.go): any other response fails the test, as a timeout would.
func URLTestExpected(ctx context.Context, link string, expected ExpectedStatus, detour N.Dialer) (uint16, error) {
	multiplexOutbound, isMultiplexOutbound := common.Cast[adapter.OutboundWithMultiplex](detour)
	if isMultiplexOutbound && multiplexOutbound.MultiplexEnabled() {
		warmContext := adapter.ContextWithKeepSession(ctx)
		warmContext = mux.ContextWithKeepSession(warmContext)
		warmContext = contextWithQUICKeepSession(warmContext)
		warmContext = snell.ContextWithKeepSession(warmContext)
		_, err := urlTest(warmContext, link, expected, detour)
		if err != nil {
			return 0, err
		}
	}
	return urlTest(ctx, link, expected, detour)
}

func urlTest(ctx context.Context, link string, expected ExpectedStatus, detour N.Dialer) (uint16, error) {
	// HEAD and a possible GET share one timeout, including dialing and TLS.
	ctx, cancel := context.WithTimeout(ctx, C.TCPTimeout)
	defer cancel()
	return urlTestMethod(ctx, link, expected, detour, http.MethodHead)
}

func urlTestMethod(ctx context.Context, link string, expected ExpectedStatus, detour N.Dialer, method string) (t uint16, err error) {
	if link == "" {
		link = "http://www.gstatic.com/generate_204"
	}
	linkURL, err := url.Parse(link)
	if err != nil {
		return
	}
	hostname := linkURL.Hostname()
	port := linkURL.Port()
	if port == "" {
		switch linkURL.Scheme {
		case "http":
			port = "80"
		case "https":
			port = "443"
		}
	}

	start := time.Now()
	instance, err := detour.DialContext(ctx, "tcp", M.ParseSocksaddrHostPortStr(hostname, port))
	if err != nil {
		return
	}
	defer instance.Close()
	if N.NeedHandshakeForWrite(instance) {
		start = time.Now()
	}
	req, err := http.NewRequest(method, link, nil)
	if err != nil {
		return
	}
	client := http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return instance, nil
			},
			TLSClientConfig: &tls.Config{
				Time:    ntp.TimeFuncFromContext(ctx),
				RootCAs: adapter.RootPoolFromContext(ctx),
			},
		},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Timeout: C.TCPTimeout,
	}
	defer client.CloseIdleConnections()
	resp, err := client.Do(req.WithContext(ctx))
	if err != nil {
		return
	}
	captureHTTPStatus(ctx, resp.StatusCode)
	resp.Body.Close()
	if method == http.MethodHead && (resp.StatusCode == http.StatusMethodNotAllowed || resp.StatusCode == http.StatusNotImplemented) {
		// Some sites only implement GET. Close HEAD's connection before retrying
		// through the same outbound. Do not follow redirects or read the body.
		instance.Close()
		return urlTestMethod(ctx, link, expected, detour, http.MethodGet)
	}
	if !expected.Match(resp.StatusCode) {
		err = E.New("unexpected status ", resp.StatusCode)
		return
	}
	// A successful sub-millisecond HTTP probe must not look unavailable to
	// Clash API clients, which reserve zero for failed tests.
	t = uint16(max(1, time.Since(start)/time.Millisecond))
	return
}
