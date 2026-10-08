package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/masonwheeler/observability-platform/internal/observability"
	"github.com/masonwheeler/observability-platform/internal/rpc"
)

// statusClientClosedConnection is Prometheus's own convention (see upstream's
// web/api/v1/api.go) for a request whose caller went away before the query
// finished — Grafana abandoning a dashboard refresh or zoom. It is not a
// server error: mapping it to 500 would inflate the self-observability
// dashboard's 5xx rate for something that never failed.
const statusClientClosedConnection = 499

// isUnavailable reports whether err means a peer could not answer. A query that
// fails that way is not a bad query; it is an outage, and says so with 503.
// All-in-one never produces such an error.
func isUnavailable(err error) bool { return errors.Is(err, rpc.ErrUnavailable) }

// isCanceled reports whether err means the caller's context was canceled
// (Grafana abandoning the request) rather than the query itself failing. It is
// checked only after isUnavailable: an rpc client's deadline error wraps BOTH
// rpc.ErrUnavailable and context.DeadlineExceeded, which is an outage (503),
// never a 499 — this matches only context.Canceled, so that combination never
// reaches here.
func isCanceled(err error) bool { return errors.Is(err, context.Canceled) }

// writePromEvalError answers a failed metric evaluation: 503 "unavailable" for
// a peer outage, 499 "canceled" (Prometheus's own statusClientClosedConnection
// convention) when the client went away, 500 "execution" for anything else.
func writePromEvalError(w http.ResponseWriter, err error) {
	switch {
	case isUnavailable(err):
		writePromError(w, http.StatusServiceUnavailable, "unavailable", err.Error())
	case isCanceled(err):
		writePromError(w, statusClientClosedConnection, "canceled", err.Error())
	default:
		writePromError(w, http.StatusInternalServerError, "execution", err.Error())
	}
}

// writeLokiEvalError answers a failed Loki evaluation: a plain-text 503 for a
// peer outage, a plain-text 499 when the client went away, the plain-text 500
// otherwise. It logs under logs_query at Error for a real failure, but only at
// Debug for a client cancel — a canceled request is not a failure, and logging
// it at Error would inflate the self-observability dashboard's error-rate
// panels for something that is not one.
func writeLokiEvalError(w http.ResponseWriter, r *http.Request, logMsg string, err error) {
	log := observability.Component(observability.FromContext(r.Context()), "logs_query")
	switch {
	case isUnavailable(err):
		log.Error(logMsg, "err", err)
		writeLokiError(w, http.StatusServiceUnavailable, "unavailable: "+err.Error())
	case isCanceled(err):
		log.Debug(logMsg, "err", err)
		writeLokiError(w, statusClientClosedConnection, "canceled: "+err.Error())
	default:
		log.Error(logMsg, "err", err)
		writeLokiError(w, http.StatusInternalServerError, "internal error")
	}
}

// writeRouteError answers a write the gateway could not route (spec section
// 5.3): an ingester outage is 503 in the write routes' {"error": ...} shape, a
// client that went away is 499 with no body, and anything else -- an ingester
// refusing a body the gateway validated -- is a protocol bug: 500, logged at
// ERROR. Routing failures are not counted as rejections; the per-ingester
// outcome counter records them.
func (s *Server) writeRouteError(w http.ResponseWriter, r *http.Request, component string, err error) {
	var qe *rpc.QuorumError
	if errors.As(err, &qe) && isUnavailable(err) {
		// A replicated write that could not reach quorum because of an outage:
		// say how far it fell short. A QuorumError with a protocol cause falls
		// through to the 500 branch below.
		observability.Component(observability.FromContext(r.Context()), component).Warn(
			"write quorum not met", slog.String("error", qe.Error()))
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": qe.Error()})
		return
	}
	switch {
	case isUnavailable(err):
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "ingester unavailable"})
	case isCanceled(err):
		w.WriteHeader(statusClientClosedConnection)
	default:
		observability.Component(observability.FromContext(r.Context()), component).Error(
			"write routing failed", slog.String("error", err.Error()))
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
	}
}
