package clashapi

import (
	"context"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/protocol/group"
	"github.com/sagernet/sing/common"
	F "github.com/sagernet/sing/common/format"
	"github.com/sagernet/sing/common/json/badjson"
	N "github.com/sagernet/sing/common/network"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
)

func proxyRouter(server *Server, router adapter.Router) http.Handler {
	r := chi.NewRouter()
	r.Get("/", getProxies(server))

	r.Route("/{name}", func(r chi.Router) {
		r.Use(parseProxyName, findProxyByName(server))
		r.Get("/", getProxy(server))
		r.Get("/delay", getProxyDelay(server))
		r.Put("/", updateProxy)
	})
	return r
}

func parseProxyName(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := getEscapeParam(r, "name")
		ctx := context.WithValue(r.Context(), CtxKeyProxyName, name)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func findProxyByName(server *Server) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			name := r.Context().Value(CtxKeyProxyName).(string)
			proxy, exist := server.outbound.Outbound(name)
			if !exist {
				render.Status(r, http.StatusNotFound)
				render.JSON(w, r, ErrNotFound)
				return
			}
			ctx := context.WithValue(r.Context(), CtxKeyProxy, proxy)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func proxyInfo(server *Server, detour adapter.Outbound) *badjson.JSONObject {
	var info badjson.JSONObject
	var clashType string
	switch detour.Type() {
	case group.LoadBalanceType:
		clashType = "LoadBalance"
	case C.TypeBlock:
		clashType = "Reject"
	default:
		clashType = C.ProxyDisplayName(detour.Type())
	}
	info.Put("type", clashType)
	info.Put("name", detour.Tag())
	info.Put("udp", common.Contains(detour.Network(), N.NetworkUDP))
	delayHistory := server.urlTestHistory.LoadURLTestHistory(group.RealTag(detour, N.NetworkTCP))
	if delayHistory != nil {
		info.Put("history", []*adapter.URLTestHistory{delayHistory})
	} else {
		info.Put("history", []*adapter.URLTestHistory{})
	}
	if group, isGroup := detour.(adapter.OutboundGroup); isGroup {
		var now string
		if selected := group.Selected(N.NetworkTCP); selected != nil {
			now = selected.Tag()
		}
		info.Put("now", now)
		info.Put("all", group.All())
	}
	if balance, ok := detour.(*group.LoadBalance); ok {
		info.Put("all", balance.All())
		info.Put("now", "")
		info.Put("loadBalance", balance.Status())
	}
	return &info
}

func getProxies(server *Server) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		var proxyMap badjson.JSONObject
		outbounds := common.Filter(server.outbound.Outbounds(), func(detour adapter.Outbound) bool {
			return detour.Tag() != ""
		})
		outbounds = append(outbounds, common.Map(common.Filter(server.endpoint.Endpoints(), func(detour adapter.Endpoint) bool {
			return detour.Tag() != ""
		}), func(it adapter.Endpoint) adapter.Outbound {
			return it
		})...)

		allProxies := make([]string, 0, len(outbounds))

		for _, detour := range outbounds {
			switch detour.Type() {
			case C.TypeDirect, C.TypeBlock, C.TypeDNS:
				continue
			}
			allProxies = append(allProxies, detour.Tag())
		}

		defaultTag := server.outbound.Default().Tag()

		sort.SliceStable(allProxies, func(i, j int) bool {
			return allProxies[i] == defaultTag
		})

		// fix clash dashboard
		proxyMap.Put("GLOBAL", map[string]any{
			"type":    "Fallback",
			"name":    "GLOBAL",
			"udp":     true,
			"history": []*adapter.URLTestHistory{},
			"all":     allProxies,
			"now":     defaultTag,
		})

		for i, detour := range outbounds {
			var tag string
			if detour.Tag() == "" {
				tag = F.ToString(i)
			} else {
				tag = detour.Tag()
			}
			proxyMap.Put(tag, proxyInfo(server, detour))
		}
		var responseMap badjson.JSONObject
		responseMap.Put("proxies", &proxyMap)
		response, err := responseMap.MarshalJSON()
		if err != nil {
			render.Status(r, http.StatusInternalServerError)
			render.JSON(w, r, newError(err.Error()))
			return
		}
		w.Write(response)
	}
}

func getProxy(server *Server) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		proxy := r.Context().Value(CtxKeyProxy).(adapter.Outbound)
		info := proxyInfo(server, proxy)
		if link := r.URL.Query().Get("url"); link != "" {
			expected, err := urltest.ParseExpectedStatus(r.URL.Query().Get("expected"))
			if err != nil {
				render.Status(r, http.StatusBadRequest)
				render.JSON(w, r, ErrBadRequest)
				return
			}
			tag := group.RealTag(proxy, N.NetworkTCP)
			info.Put("probe", map[string]any{
				"tag": tag, "url": link, "expected": expected.String(),
				"history": server.urlTestHistory.LoadProbeHistory(tag, urltest.ProbeLink(link, expected)),
			})
		}
		response, err := info.MarshalJSON()
		if err != nil {
			render.Status(r, http.StatusInternalServerError)
			render.JSON(w, r, newError(err.Error()))
			return
		}
		w.Write(response)
	}
}

type UpdateProxyRequest struct {
	Name string `json:"name"`
}

func updateProxy(w http.ResponseWriter, r *http.Request) {
	req := UpdateProxyRequest{}
	if err := render.DecodeJSON(r.Body, &req); err != nil {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, ErrBadRequest)
		return
	}

	proxy := r.Context().Value(CtxKeyProxy).(adapter.Outbound)
	selector, ok := proxy.(*group.Selector)
	if !ok {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, newError("Must be a Selector"))
		return
	}

	if !selector.SelectOutbound(req.Name) {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, newError("Selector update error: not found"))
		return
	}

	render.NoContent(w, r)
}

func groupContains(outboundManager adapter.OutboundManager, outboundGroup adapter.OutboundGroup, tag string, visited map[string]bool) bool {
	for _, memberTag := range outboundGroup.All() {
		if memberTag == tag {
			return true
		}
		member, loaded := outboundManager.Outbound(memberTag)
		if !loaded {
			continue
		}
		if group.RealTag(member, N.NetworkTCP) == tag {
			return true
		}
		memberGroup, isGroup := member.(adapter.OutboundGroup)
		if !isGroup || visited[memberTag] {
			continue
		}
		visited[memberTag] = true
		if groupContains(outboundManager, memberGroup, tag, visited) {
			return true
		}
	}
	return false
}

func getProxyDelay(server *Server) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		url := query.Get("url")
		timeout, err := strconv.ParseInt(query.Get("timeout"), 10, 16)
		if err != nil {
			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, ErrBadRequest)
			return
		}
		// Open-Box (tcp19): `expected` = statuses that count as a pass, as mihomo's
		// expected-status ("200-399/204"); empty or "*" accepts any response.
		expected, err := urltest.ParseExpectedStatus(query.Get("expected"))
		if err != nil {
			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, ErrBadRequest)
			return
		}

		force := true
		if value := query.Get("force"); value != "" {
			force, err = strconv.ParseBool(value)
			if err != nil {
				render.Status(r, http.StatusBadRequest)
				render.JSON(w, r, ErrBadRequest)
				return
			}
		}
		interval := int64(0)
		if !force {
			interval, err = strconv.ParseInt(query.Get("interval"), 10, 64)
			if err != nil || interval <= 0 || interval > 86400000 {
				render.Status(r, http.StatusBadRequest)
				render.JSON(w, r, ErrBadRequest)
				return
			}
		}
		proxy := r.Context().Value(CtxKeyProxy).(adapter.Outbound)
		realTag := group.RealTag(proxy, N.NetworkTCP)
		// Freeze the chosen leaf for the duration of this probe.
		if leaf, loaded := server.outbound.Outbound(realTag); loaded {
			proxy = leaf
		}
		// Open-Box (tcp14): the probe waits for a slot in the kernel-wide probe budget
		// (common/urltest/pacer.go); `timeout` bounds the probe itself, not the wait.
		// `priority` = interactive (default: someone is waiting for it), background,
		// or critical (never queued: keeping traffic flowing).
		priority := urltest.PriorityInteractive
		if value := query.Get("priority"); value != "" {
			var ok bool
			priority, ok = urltest.ParseProbePriority(value)
			if !ok {
				render.Status(r, http.StatusBadRequest)
				render.JSON(w, r, ErrBadRequest)
				return
			}
		}
		probeTimeout := time.Millisecond * time.Duration(timeout)
		ctx := urltest.WithPriority(context.Background(), priority)
		// `since` (unix ms): any result for this member completed after it counts,
		// even with force, and one in flight is joined — a recheck of a failure seen at
		// `since` is shared with the kernel's own recheck instead of probing twice.
		if value := query.Get("since"); value != "" {
			since, err := strconv.ParseInt(value, 10, 64)
			if err != nil || since <= 0 {
				render.Status(r, http.StatusBadRequest)
				render.JSON(w, r, ErrBadRequest)
				return
			}
			ctx = urltest.WithNewerThan(ctx, time.UnixMilli(since))
		}
		result := server.urlTestHistory.Probe(ctx, realTag, urltest.ProbeLink(url, expected), time.Millisecond*time.Duration(interval), force, func(ctx context.Context) (uint16, error) {
			if current, loaded := server.outbound.Outbound(realTag); !loaded || current != proxy {
				return 0, context.Canceled
			}
			probeCtx, cancel := context.WithTimeout(ctx, probeTimeout)
			defer cancel()
			return urltest.URLTestExpected(probeCtx, url, expected, proxy)
		})
		delay, err := result.Delay, result.Err
		if errors.Is(err, context.Canceled) {
			render.Status(r, http.StatusConflict)
			render.JSON(w, r, render.M{"message": "Probe invalidated", "error": err.Error()})
			return
		}
		defer func() {
			for _, detour := range server.outbound.Outbounds() {
				urlTestGroup, isURLTestGroup := detour.(adapter.URLTestGroup)
				if !isURLTestGroup {
					continue
				}
				if !groupContains(server.outbound, urlTestGroup, realTag, map[string]bool{detour.Tag(): true}) {
					continue
				}
				urlTestGroup.PerformUpdateCheck()
			}
		}()

		// Open-Box (tcp14): the raw error goes back too, so the panel can say why a node
		// failed (refused / TLS / DNS / closed ...) without probing it a second time.
		errText := ""
		if err != nil {
			errText = err.Error()
		}
		if ctx.Err() != nil || errors.Is(err, context.DeadlineExceeded) {
			render.Status(r, http.StatusGatewayTimeout)
			render.JSON(w, r, render.M{"message": "Timeout", "error": errText, "time": result.Time, "reused": result.Reused})
			return
		}

		if err != nil || delay == 0 {
			render.Status(r, http.StatusServiceUnavailable)
			render.JSON(w, r, render.M{"message": "An error occurred in the delay test", "error": errText, "time": result.Time, "reused": result.Reused})
			return
		}

		render.JSON(w, r, render.M{
			"delay":  delay,
			"time":   result.Time,
			"reused": result.Reused,
		})
	}
}
