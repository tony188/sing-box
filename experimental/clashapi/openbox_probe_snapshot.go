package clashapi

import (
	"encoding/json"
	"github.com/go-chi/render"
	"github.com/sagernet/sing-box/common/urltest"
	"net/http"
)

func openboxProbeSnapshot(server *Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Conditions []urltest.ProbeCondition `json:"conditions"`
		}
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2<<20))
		if decoder.Decode(&request) != nil || len(request.Conditions) > 4096 {
			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, ErrBadRequest)
			return
		}
		for _, c := range request.Conditions {
			if c.Tag == "" || len(c.URL) > 8192 || len(c.Expected) > 256 {
				render.Status(r, http.StatusBadRequest)
				render.JSON(w, r, ErrBadRequest)
				return
			}
			if _, err := urltest.ParseExpectedStatus(c.Expected); err != nil {
				render.Status(r, http.StatusBadRequest)
				render.JSON(w, r, ErrBadRequest)
				return
			}
		}
		render.JSON(w, r, server.urlTestHistory.Snapshot(request.Conditions))
	}
}
