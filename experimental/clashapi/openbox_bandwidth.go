package clashapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-chi/render"
	"github.com/sagernet/sing-box/common/urltest"
	M "github.com/sagernet/sing/common/metadata"
	"io"
	"math"
	"mime"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"
)

type bandwidthGroup interface {
	BandwidthMembers() []string
	BandwidthHealthy(string) bool
}

func bandwidthAllowed(server *Server, tag string, health bool) bool {
	for _, o := range server.outbound.Outbounds() {
		if g, ok := o.(bandwidthGroup); ok && slices.Contains(g.BandwidthMembers(), tag) && (!health || g.BandwidthHealthy(tag)) {
			return true
		}
	}
	if condition, ok := server.urlTestHistory.Bandwidth().Monitoring(tag); ok {
		if !health {
			return true
		}
		sample := server.urlTestHistory.Snapshot([]urltest.ProbeCondition{condition}).Results[0]
		age := time.Now().UnixMilli() - sample.At
		return sample.OK != nil && *sample.OK && !sample.Pending && age >= 0 && age <= 240000 &&
			(sample.Confirmation == nil || sample.Confirmation.FailedAt <= sample.At)
	}
	return false
}
func openboxBandwidthUpdate(server *Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var q struct {
			Samples    map[string]urltest.BandwidthSample `json:"samples"`
			Aliases    map[string]string                  `json:"aliases"`
			Monitoring []urltest.ProbeCondition           `json:"monitoring"`
		}
		if json.NewDecoder(http.MaxBytesReader(w, r.Body, 2<<20)).Decode(&q) != nil || len(q.Samples) > 4096 || len(q.Aliases) > 4096 || len(q.Monitoring) > 4096 {
			http.Error(w, "invalid samples", 400)
			return
		}
		allowed := make(map[string]bool)
		for _, o := range server.outbound.Outbounds() {
			if g, ok := o.(bandwidthGroup); ok {
				for _, tag := range g.BandwidthMembers() {
					allowed[tag] = true
				}
			}
		}
		for _, c := range q.Monitoring {
			o, ok := server.outbound.Outbound(c.Tag)
			if !ok || slices.Contains([]string{"selector", "urltest", "loadbalance", "direct", "block"}, o.Type()) || len(c.URL) > 4096 || !(strings.HasPrefix(c.URL, "http://") || strings.HasPrefix(c.URL, "https://")) {
				http.Error(w, "invalid monitoring condition", 400)
				return
			}
			allowed[c.Tag] = true
			if _, err := urltest.ParseExpectedStatus(c.Expected); err != nil {
				http.Error(w, "invalid expected status", 400)
				return
			}
		}
		safe := map[string]urltest.BandwidthSample{}
		for tag, s := range q.Samples {
			if !allowed[tag] {
				continue
			}
			if s.Mbps <= 0 || s.Mbps > 1e7 || math.IsNaN(s.Mbps) || math.IsInf(s.Mbps, 0) || s.At <= 0 || s.At > time.Now().Add(time.Minute).UnixMilli() || s.PeriodMs < 3600000 || s.PeriodMs > 86400000 {
				http.Error(w, "invalid measurement", 400)
				return
			}
			safe[tag] = s
		}
		aliases := map[string]string{}
		for tag, id := range q.Aliases {
			if len(id) == 64 && allowed[tag] {
				aliases[tag] = id
			}
		}
		server.urlTestHistory.Bandwidth().SetMonitoring(q.Monitoring)
		server.urlTestHistory.Bandwidth().SetAliases(aliases)
		server.urlTestHistory.Bandwidth().Replace(safe)
		for _, o := range server.outbound.Outbounds() {
			if g, ok := o.(interface{ ReselectBandwidth() }); ok {
				g.ReselectBandwidth()
			}
		}
		render.JSON(w, r, map[string]any{"ok": true, "monitoringSupported": true})
	}
}

var bandwidthProbeSlot = make(chan struct{}, 1)

func openboxBandwidthProbe(server *Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var q struct {
			Tag      string   `json:"tag"`
			Tags     []string `json:"tags"`
			MaxBytes int64    `json:"maxBytes"`
		}
		if json.NewDecoder(http.MaxBytesReader(w, r.Body, 8192)).Decode(&q) != nil || q.MaxBytes < 1000000 || q.MaxBytes > 1024000000 {
			http.Error(w, "invalid probe", 400)
			return
		}
		if len(q.Tags) > 64 {
			http.Error(w, "too many aliases", 400)
			return
		}
		for _, tag := range q.Tags {
			if bandwidthAllowed(server, tag, true) {
				q.Tag = tag
				break
			}
		}
		if !bandwidthAllowed(server, q.Tag, true) {
			http.Error(w, "node is not healthy in an enabled bandwidth group", 409)
			return
		}
		outbound, ok := server.outbound.Outbound(q.Tag)
		if !ok || outbound.Type() == "urltest" || outbound.Type() == "selector" || outbound.Type() == "loadbalance" || outbound.Type() == "direct" || outbound.Type() == "block" {
			http.Error(w, "real proxy required", 400)
			return
		}
		select {
		case bandwidthProbeSlot <- struct{}{}:
			defer func() { <-bandwidthProbeSlot }()
		default:
			http.Error(w, "bandwidth probe busy", 409)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		transport := &http.Transport{DisableCompression: true, DisableKeepAlives: true, ForceAttemptHTTP2: false, DialContext: func(ctx context.Context, n, a string) (net.Conn, error) {
			return outbound.DialContext(ctx, n, M.ParseSocksaddr(a))
		}}
		defer transport.CloseIdleConnections()
		client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return fmt.Errorf("redirected bandwidth probe") }}
		// Fixed HTTPS endpoint: imported client profiles cannot schedule probes or
		// turn this authenticated API into an arbitrary URL fetcher.
		request, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://speed.cloudflare.com/__down?bytes="+strconv.FormatInt(q.MaxBytes, 10), nil)
		response, err := client.Do(request)
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		defer response.Body.Close()
		mediaType, _, _ := mime.ParseMediaType(response.Header.Get("Content-Type"))
		if response.StatusCode != 200 || mediaType != "application/octet-stream" {
			http.Error(w, "unexpected bandwidth response", 502)
			return
		}
		started := time.Now()
		timer := time.AfterFunc(5*time.Second, cancel)
		defer timer.Stop()
		n, readErr := readBandwidthBody(response.Body, q.MaxBytes)
		elapsed := time.Since(started).Seconds()
		if n < 256000 || elapsed < 0.05 || readErr != nil && ctx.Err() == nil {
			http.Error(w, "insufficient bandwidth sample", 502)
			return
		}
		render.JSON(w, r, map[string]any{"mbps": float64(n) * 8 / elapsed / 1e6, "bytes": n, "seconds": elapsed, "at": time.Now().UnixMilli(), "lowerBound": n >= q.MaxBytes})
	}
}

func readBandwidthBody(body io.Reader, limit int64) (int64, error) {
	n, err := io.CopyBuffer(io.Discard, io.LimitReader(body, limit), make([]byte, 32*1024))
	// QUIC streams may wrap a normal EOF. io.Copy only recognizes the exact
	// sentinel, so normalize it without hiding truncated bodies or resets.
	if errors.Is(err, io.EOF) {
		return n, nil
	}
	return n, err
}
