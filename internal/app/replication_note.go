package app

import (
	"context"
	"log/slog"
	"net/http"
	"sync"

	"github.com/masonwheeler/observability-platform/internal/observability"
)

// skipNote collects, for one HTTP request, the ingesters its reads skipped.
// A request can read the sources several times -- /api/v1/series reads once
// per match[] selector -- so the head merge records each skip here
// (MergeHeadsWith's OnSkip) and the request logs the note once, after its
// response, and only if that response succeeded.
type skipNote struct {
	mu      sync.Mutex
	skipped []string
}

type skipNoteKey struct{}

// recordSkip adds names to the request's note, if the read carries one.
func recordSkip(ctx context.Context, names []string) {
	if n, ok := ctx.Value(skipNoteKey{}).(*skipNote); ok {
		n.mu.Lock()
		n.skipped = append(n.skipped, names...)
		n.mu.Unlock()
	}
}

// replicationNotes gives each request a skip note and, once the handler has
// answered with a non-error status, logs one "read answered by replication"
// line with the distinct ingesters skipped, through the request's logger (so
// with its request ID). It runs inside the API server's request-ID and logger
// middleware (api.Deps.Middleware).
func replicationNotes(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := &skipNote{}
		ctx := context.WithValue(r.Context(), skipNoteKey{}, n)
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r.WithContext(ctx))
		n.mu.Lock()
		skipped := n.skipped
		n.mu.Unlock()
		if len(skipped) == 0 || sw.status >= http.StatusBadRequest {
			return
		}
		seen := map[string]bool{}
		var distinct []string
		for _, s := range skipped {
			if !seen[s] {
				seen[s] = true
				distinct = append(distinct, s)
			}
		}
		observability.Component(observability.FromContext(ctx), "querier").Warn(
			"read answered by replication", slog.Any("skipped", distinct))
	})
}

// statusWriter records the status a handler answered with.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
