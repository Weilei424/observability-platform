package rpc

import (
	"context"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
)

type drainResponse struct {
	Drained bool `json:"drained"`
}

// MountDrain registers POST /drain on an ingester: stop taking writes, then
// flush its whole head — metrics, open chunks included, and logs — into the
// store, all within timeout. It answers 200 {"drained":true} only when
// everything reached the store and the head was empty afterwards, so the 200
// covers every write the ingester acknowledged, and 503 with the reason
// otherwise (the store unreachable or the deadline passed). Removing an
// ingester waits for this 200 before the querier stops reading it, so the
// removal never hides data that is left only in its WAL.
func MountDrain(r chi.Router, drain func(context.Context) error, timeout time.Duration) {
	r.Post("/drain", func(w http.ResponseWriter, req *http.Request) {
		ctx, cancel := context.WithTimeout(req.Context(), timeout)
		defer cancel()
		if err := drain(ctx); err != nil {
			writeError(w, http.StatusServiceUnavailable, "drain incomplete: "+err.Error())
			return
		}
		writeJSON(w, http.StatusOK, drainResponse{Drained: true})
	})
}
