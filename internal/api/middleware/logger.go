package middleware

import (
	"log/slog"
	"net/http"
	"time"

	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/masonwheeler/observability-platform/internal/observability"
)

// Logger stores a request-scoped logger (carrying only "request_id") in the
// request context for handlers to derive their own component logger from, and
// separately emits one component=api access-log line per request.
//
// The context-stored logger is deliberately kept component-free. Handlers get
// their own component via observability.Component(observability.FromContext(ctx),
// "<subsystem>"); if this logger already carried "component" (as it did when
// callers passed it Component(log, "api")), that call would add a SECOND
// "component" key to every handler log line instead of being the request's
// only one -- slog does not deduplicate attributes added by separate With
// calls. Stamping "component" only on the access-log line itself, on a
// logger the handlers never see, avoids that entirely.
func Logger(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := chimiddleware.NewWrapResponseWriter(w, r.ProtoMajor)

			reqLog := log.With(slog.String("request_id", chimiddleware.GetReqID(r.Context())))
			r = r.WithContext(observability.ContextWithLogger(r.Context(), reqLog))

			// completed is set only once next.ServeHTTP returns normally, so the
			// deferred access-log line below can tell a panicking handler apart
			// from one that simply never called WriteHeader. Without this defer,
			// a panic skips the access-log line entirely and the request's
			// request_id is never logged anywhere; recording it as status 200
			// would additionally misreport a failure as a success. The panic
			// itself is still left to propagate -- this defer does not call
			// recover, so nothing here changes what happens to it above this
			// middleware.
			var completed bool
			defer func() {
				status := ww.Status()
				switch {
				case !completed:
					status = http.StatusInternalServerError
				case status == 0:
					status = http.StatusOK
				}

				observability.Component(reqLog, "api").Info("request",
					"method", r.Method,
					"path", r.URL.Path,
					"status", status,
					"duration", time.Since(start).String(),
				)
			}()

			next.ServeHTTP(ww, r)
			completed = true
		})
	}
}
