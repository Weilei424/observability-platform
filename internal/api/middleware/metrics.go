package middleware

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/masonwheeler/observability-platform/internal/observability"
)

// Metrics records request count and duration for every request, labelled by chi
// route pattern.
func Metrics(m *observability.HTTPMetrics) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := chimiddleware.NewWrapResponseWriter(w, r.ProtoMajor)

			next.ServeHTTP(ww, r)

			// RoutePattern is only populated once routing has resolved, which
			// happens inside ServeHTTP — reading it before the call yields "".
			var route string
			if rctx := chi.RouteContext(r.Context()); rctx != nil {
				route = rctx.RoutePattern()
			}

			// A handler that returns without calling WriteHeader still sends 200;
			// the wrapper reports 0 for that, and "0" is not a status code.
			status := ww.Status()
			if status == 0 {
				status = http.StatusOK
			}

			m.Observe(route, r.Method, status, time.Since(start))
		})
	}
}
