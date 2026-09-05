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

			// completed is set only once next.ServeHTTP returns normally. A
			// panicking handler never reaches that statement, so the defer below
			// can tell "handler returned without writing a header" (completed,
			// status 0 -> 200) apart from "handler panicked" (not completed ->
			// 500) -- a distinction the wrapped writer's Status() cannot make on
			// its own, since both cases report 0.
			var completed bool

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

				status := ww.Status()
				switch {
				case !completed:
					// The handler panicked. Recording this as 200 would be worse
					// than not recording it at all: it hides the panic inside a
					// "success" series and a 5xx-based alert or dashboard
					// expression would never see it.
					status = http.StatusInternalServerError
				case status == 0:
					// A handler that returns without calling WriteHeader still
					// sends 200; the wrapper reports 0 for that, and "0" is not a
					// status code.
					status = http.StatusOK
				}

				m.Observe(route, r.Method, status, time.Since(start))
			}()

			next.ServeHTTP(ww, r)
			completed = true
		})
	}
}
