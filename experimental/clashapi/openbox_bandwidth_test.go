package clashapi

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
	M "github.com/sagernet/sing/common/metadata"
	"io"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type bandwidthEndingReader struct {
	data *bytes.Reader
	err  error
}

func (r *bandwidthEndingReader) Read(p []byte) (int, error) {
	n, err := r.data.Read(p)
	if err == io.EOF || r.data.Len() == 0 {
		return n, r.err
	}
	return n, err
}

func TestOpenBoxBandwidthBodyNormalizesWrappedEOFOnly(t *testing.T) {
	for _, tc := range []struct {
		name string
		end  error
		ok   bool
	}{
		{"normal EOF", io.EOF, true},
		{"QUIC wrapped EOF", fmt.Errorf("QUIC: %w", io.EOF), true},
		{"truncated HTTP body", io.ErrUnexpectedEOF, false},
		{"reset", errors.New("connection reset by peer"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := bytes.Repeat([]byte{42}, 512000)
			n, err := readBandwidthBody(&bandwidthEndingReader{bytes.NewReader(data), tc.end}, 1000000)
			if n != int64(len(data)) || (err == nil) != tc.ok {
				t.Fatalf("bytes=%d error=%v", n, err)
			}
		})
	}
}

type bandwidthTestManager struct {
	adapter.OutboundManager
	list []adapter.Outbound
}

func (m *bandwidthTestManager) Outbounds() []adapter.Outbound { return m.list }
func (m *bandwidthTestManager) Outbound(tag string) (adapter.Outbound, bool) {
	for _, o := range m.list {
		if o.Tag() == tag {
			return o, true
		}
	}
	return nil, false
}

type bandwidthTestOutbound struct {
	adapter.Outbound
	enabled, healthy bool
}

func (o *bandwidthTestOutbound) Tag() string  { return "a" }
func (o *bandwidthTestOutbound) Type() string { return "shadowsocks" }
func (o *bandwidthTestOutbound) BandwidthMembers() []string {
	if o.enabled {
		return []string{"a"}
	}
	return nil
}
func (o *bandwidthTestOutbound) BandwidthHealthy(string) bool { return o.healthy }
func (o *bandwidthTestOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	panic("unexpected real download")
}
func TestOpenBoxBandwidthProbeRequiresEnabledHealthyGroup(t *testing.T) {
	for _, c := range []struct {
		enabled, healthy bool
		payload          string
		want             int
	}{
		{false, true, `{"tag":"a","maxBytes":64000000}`, 409}, {true, false, `{"tag":"a","maxBytes":64000000}`, 409},
		{true, true, `{"tag":"a","maxBytes":1024000001}`, 400}, {true, true, `{"tag":"a","maxBytes":0}`, 400},
	} {
		o := &bandwidthTestOutbound{enabled: c.enabled, healthy: c.healthy}
		s := &Server{outbound: &bandwidthTestManager{list: []adapter.Outbound{o}}, urlTestHistory: urltest.NewHistoryStorage()}
		w := httptest.NewRecorder()
		openboxBandwidthProbe(s)(w, httptest.NewRequest("POST", "/", strings.NewReader(c.payload)))
		if w.Code != c.want {
			t.Fatal(w.Code, w.Body.String())
		}
	}
}
func TestOpenBoxBandwidthUploadDoesNotCreateClientWeights(t *testing.T) {
	o := &bandwidthTestOutbound{}
	h := urltest.NewHistoryStorage()
	s := &Server{outbound: &bandwidthTestManager{list: []adapter.Outbound{o}}, urlTestHistory: h}
	payload := `{"samples":{"a":{"mbps":500,"at":1800000000000,"periodMs":7200000}},"aliases":{"a":"` + strings.Repeat("a", 64) + `"}}`
	w := httptest.NewRecorder()
	openboxBandwidthUpdate(s)(w, httptest.NewRequest("PUT", "/", strings.NewReader(payload)))
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	if _, ok := h.Bandwidth().Weights(map[string]uint32{"a": 10}, time.Now()); ok {
		t.Fatal("disabled client accepted a weight")
	}
}
func TestOpenBoxBandwidthReadCapIsPerWholeTask(t *testing.T) {
	body := bytes.NewReader(make([]byte, 2000000))
	n, err := readBandwidthBody(body, 1000000)
	if err != nil || n != 1000000 || body.Len() != 1000000 {
		t.Fatal(n, err, body.Len())
	}
	if _, err = io.ReadAll(body); err != nil {
		t.Fatal(err)
	}
}

func TestOpenBoxBandwidthMonitoringRequiresFreshConfirmedHealth(t *testing.T) {
	o := &bandwidthTestOutbound{}
	h := urltest.NewHistoryStorage()
	s := &Server{outbound: &bandwidthTestManager{list: []adapter.Outbound{o}}, urlTestHistory: h}
	w := httptest.NewRecorder()
	openboxBandwidthUpdate(s)(w, httptest.NewRequest("PUT", "/", strings.NewReader(`{"monitoring":[{"tag":"a","url":"http://test/","expected":"204"}]}`)))
	if w.Code != 200 || !strings.Contains(w.Body.String(), "monitoringSupported") {
		t.Fatal(w.Code, w.Body.String())
	}
	if bandwidthAllowed(s, "a", true) {
		t.Fatal("untested node admitted")
	}
	if !bandwidthAllowed(s, "a", false) {
		t.Fatal("monitor not registered")
	}
	expected, _ := urltest.ParseExpectedStatus("204")
	link := urltest.ProbeLink("http://test/", expected)
	h.Probe(context.Background(), "a", link, time.Hour, true, func(context.Context) (uint16, error) { return 20, nil })
	if !bandwidthAllowed(s, "a", true) {
		t.Fatal("healthy monitor rejected")
	}
	h.Probe(context.Background(), "a", link, time.Hour, true, func(context.Context) (uint16, error) { return 0, errors.New("down") })
	if bandwidthAllowed(s, "a", true) {
		t.Fatal("failed/recovering monitor admitted")
	}
	w = httptest.NewRecorder()
	openboxBandwidthUpdate(s)(w, httptest.NewRequest("PUT", "/", strings.NewReader(`{"monitoring":[]}`)))
	if bandwidthAllowed(s, "a", false) {
		t.Fatal("disabled monitor retained")
	}
	w = httptest.NewRecorder()
	openboxBandwidthUpdate(s)(w, httptest.NewRequest("PUT", "/", strings.NewReader(`{"monitoring":[{"tag":"missing","url":"http://test/"}]}`)))
	if w.Code != 400 {
		t.Fatal("nonexistent monitor accepted")
	}
}
