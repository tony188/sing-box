package clashapi

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/protocol/group"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/json/badjson"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
)

func groupRouter(server *Server) http.Handler {
	r := chi.NewRouter()
	r.Get("/", getGroups(server))
	r.Route("/{name}", func(r chi.Router) {
		r.Use(parseProxyName, findProxyByName(server))
		r.Get("/", getGroup(server))
		r.Get("/delay", getGroupDelay(server))
	})
	return r
}

func getGroups(server *Server) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		groups := common.Map(common.Filter(server.outbound.Outbounds(), func(it adapter.Outbound) bool {
			_, isGroup := it.(adapter.OutboundGroup)
			return isGroup
		}), func(it adapter.Outbound) *badjson.JSONObject {
			return proxyInfo(server, it)
		})
		render.JSON(w, r, render.M{
			"proxies": groups,
		})
	}
}

func getGroup(server *Server) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		proxy := r.Context().Value(CtxKeyProxy).(adapter.Outbound)
		if _, ok := proxy.(adapter.OutboundGroup); ok {
			render.JSON(w, r, proxyInfo(server, proxy))
			return
		}
		render.Status(r, http.StatusNotFound)
		render.JSON(w, r, ErrNotFound)
	}
}

func getGroupDelay(server *Server) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		proxy := r.Context().Value(CtxKeyProxy).(adapter.Outbound)
		outboundGroup, ok := proxy.(adapter.OutboundGroup)
		if !ok {
			render.Status(r, http.StatusNotFound)
			render.JSON(w, r, ErrNotFound)
			return
		}

		query := r.URL.Query()
		url := query.Get("url")
		timeout, err := strconv.ParseInt(query.Get("timeout"), 10, 32)
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

		if query.Get("reselect_only") == "true" {
			// A failover lane only needs to consume the shared history and update
			// its selected member, never to start another round of network probes.
			if g, ok := outboundGroup.(adapter.URLTestGroup); ok {
				g.PerformUpdateCheck()
				render.JSON(w, r, render.M{})
				return
			}
			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, ErrBadRequest)
			return
		}

		// `timeout` is each member's own probe timeout (the panel's latency test setting),
		// as in the Clash API where every proxy in a group is tested with that bound. The
		// request itself gets enough room for all members: the batch probes ten at a time,
		// so a group with many dead members needs ceil(n/10) waves of that timeout.
		memberTimeout := time.Millisecond * time.Duration(timeout)
		members := len(outboundGroup.All())
		waves := (members + 9) / 10
		if waves < 1 {
			waves = 1
		}
		ctx, cancel := context.WithTimeout(r.Context(), memberTimeout*time.Duration(waves)+5*time.Second)
		defer cancel()

		var result map[string]uint16
		if urlTestGroup, isURLTestGroup := outboundGroup.(interface {
			URLTestWithForce(context.Context, bool, time.Duration) (map[string]uint16, error)
		}); isURLTestGroup {
			result, err = urlTestGroup.URLTestWithForce(ctx, force, memberTimeout)
		} else {
			outbounds := common.FilterNotNil(common.Map(outboundGroup.All(), func(it string) adapter.Outbound {
				itOutbound, _ := server.outbound.Outbound(it)
				return itOutbound
			}))
			result = group.URLTestOutbounds(ctx, server.outbound, server.urlTestHistory, server.logger, outbounds, url, 0, force, memberTimeout)
		}

		if err != nil {
			render.Status(r, http.StatusGatewayTimeout)
			render.JSON(w, r, newError(err.Error()))
			return
		}

		render.JSON(w, r, result)
	}
}
