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

			// Deferred so a panicking handler is still observed: a plain
			// post-call statement never runs once next.ServeHTTP panics, and the
			// request would vanish from telemetry entirely — no count, no
			// duration. This does not recover the panic; it still propagates
			// after this function returns, same as if this defer weren't here.
			defer func() {
				// RoutePattern is only populated once routing has resolved, which
				// happens inside ServeHTTP — reading it before the call yields "".
				var route string
				if rctx := chi.RouteContext(r.Context()); rctx != nil {
					route = rctx.RoutePattern()
				}

				// A handler that returns without calling WriteHeader still sends
				// 200; the wrapper reports 0 for that, and "0" is not a status
				// code. A handler that panics before calling WriteHeader is
				// indistinguishable from this case and is recorded the same way.
				status := ww.Status()
				if status == 0 {
					status = http.StatusOK
				}

				m.Observe(route, r.Method, status, time.Since(start))
			}()

			next.ServeHTTP(ww, r)
		})
	}
}
