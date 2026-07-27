package api

import (
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/masonwheeler/observability-platform/internal/logs"
)

// maxEpochSeconds is the largest whole-second epoch that still fits in an int64
// nanosecond count (~year 2262). Beyond it the ns conversion would silently wrap.
const maxEpochSeconds = math.MaxInt64 / int64(time.Second)

// parseLokiTime parses a Loki timestamp, mirroring Loki's own parseTimestamp
// (pkg/loghttp/params.go) so the same string means the same instant here:
//
//   - contains '.'        → float seconds       ("1700000000.5")
//   - integer, ≤10 digits → whole seconds       ("1700000000")
//   - integer, >10 digits → nanoseconds         ("1700000000000000000")
//   - otherwise           → RFC3339 / RFC3339Nano
//
// The digit-length rule is surprising, but it is the upstream contract and it is
// what a hand-written `curl ...&start=1700000000` depends on: without it a
// second-granularity epoch would be read as a few nanoseconds after 1970 and
// quietly return nothing.
func parseLokiTime(s string) (int64, error) {
	if strings.Contains(s, ".") {
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			sec, frac := math.Modf(f)
			if sec > float64(maxEpochSeconds) || sec < float64(-maxEpochSeconds) {
				return 0, fmt.Errorf("timestamp %q is outside the representable nanosecond range", s)
			}
			// Round to microseconds first, as Loki does, so float noise in the
			// fractional part does not leak into the nanosecond value.
			frac = math.Round(frac*1000) / 1000
			return time.Unix(int64(sec), int64(frac*float64(time.Second))).UnixNano(), nil
		}
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		if len(strings.TrimPrefix(s, "-")) > 10 {
			return n, nil // already nanoseconds
		}
		if n > maxEpochSeconds || n < -maxEpochSeconds {
			return 0, fmt.Errorf("timestamp %q is outside the representable nanosecond range", s)
		}
		return n * int64(time.Second), nil
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UnixNano(), nil
		}
	}
	return 0, fmt.Errorf("invalid timestamp %q: want a Unix epoch (seconds, float seconds, or nanoseconds) or RFC3339", s)
}

func parseLokiLimit(s string) (int, error) {
	if s == "" {
		return 100, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("invalid 'limit' parameter %q: want a positive integer", s)
	}
	return n, nil
}

func parseLokiDirection(s string) (logs.Direction, error) {
	switch s {
	case "", "backward":
		return logs.Backward, nil
	case "forward":
		return logs.Forward, nil
	default:
		return logs.Backward, fmt.Errorf("invalid 'direction' parameter %q: want 'forward' or 'backward'", s)
	}
}

// requireLogQuery guards handlers when no logs query engine is configured (e.g.
// metrics-only test wiring). Production always configures it.
func (s *Server) requireLogQuery(w http.ResponseWriter, r *http.Request) bool {
	if s.logQuery == nil {
		s.log.Error("logs query engine not configured", "component", "logs_query",
			"request_id", chimiddleware.GetReqID(r.Context()))
		writeLokiError(w, http.StatusInternalServerError, "logs query engine not configured")
		return false
	}
	return true
}

// parseLokiQueryParams performs the parameter parsing shared by the query_range
// and instant query handlers: the log-query-engine guard, form parsing, the
// required 'query' parameter, 'limit', and 'direction'. It deliberately does not
// parse the query expression — the two endpoints accept different expression
// kinds (see handleLokiQuery). On any failure it writes the plain-text error
// response itself and returns ok=false; callers must return immediately when ok
// is false. On success, r.Form is populated so callers can read their own
// remaining parameters (start/end/time) from it directly.
func (s *Server) parseLokiQueryParams(w http.ResponseWriter, r *http.Request) (queryStr string, limit int, dir logs.Direction, ok bool) {
	if !s.requireLogQuery(w, r) {
		return "", 0, 0, false
	}
	if err := r.ParseForm(); err != nil {
		writeLokiError(w, http.StatusBadRequest, "invalid request: "+err.Error())
		return "", 0, 0, false
	}
	q := r.Form
	queryStr = q.Get("query")
	if queryStr == "" {
		writeLokiError(w, http.StatusBadRequest, "missing required parameter 'query'")
		return "", 0, 0, false
	}
	limit, err := parseLokiLimit(q.Get("limit"))
	if err != nil {
		writeLokiError(w, http.StatusBadRequest, err.Error())
		return "", 0, 0, false
	}
	dir, err = parseLokiDirection(q.Get("direction"))
	if err != nil {
		writeLokiError(w, http.StatusBadRequest, err.Error())
		return "", 0, 0, false
	}
	return queryStr, limit, dir, true
}

// parseLokiLogSelector parses queryStr as a log query, writing the plain-text
// 400 itself on failure.
func parseLokiLogSelector(w http.ResponseWriter, queryStr string) (logs.LogSelector, bool) {
	sel, err := logs.ParseLogQL(queryStr)
	if err != nil {
		writeLokiError(w, http.StatusBadRequest, err.Error())
		return logs.LogSelector{}, false
	}
	return sel, true
}

// respondLokiStreams runs fetch and writes its result as a Loki streams
// envelope, or logs the error and writes a 500 on failure. logMsg distinguishes
// the query_range vs instant query call site in the log line.
func (s *Server) respondLokiStreams(w http.ResponseWriter, r *http.Request, logMsg string, fetch func() ([]logs.StreamResult, error)) {
	results, err := fetch()
	if err != nil {
		s.log.Error(logMsg, "component", "logs_query",
			"request_id", chimiddleware.GetReqID(r.Context()), "err", err)
		writeLokiError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeLokiStreams(w, toLokiStreamResults(results))
}

func (s *Server) handleLokiQueryRange(w http.ResponseWriter, r *http.Request) {
	queryStr, limit, dir, ok := s.parseLokiQueryParams(w, r)
	if !ok {
		return
	}
	sel, ok := parseLokiLogSelector(w, queryStr)
	if !ok {
		return
	}
	q := r.Form

	// `step` is for metric queries and Loki ignores it on a stream response, so
	// ignoring it here matches upstream. `interval` is different: it thins a
	// stream response, returning the entry at start and then the next one at or
	// after each interval boundary. Ignoring it would hand back *more* entries
	// than asked for and look like a working filter, so it is rejected outright
	// per the project guardrail that unsupported query features error explicitly.
	if q.Get("interval") != "" {
		writeLokiError(w, http.StatusBadRequest,
			"unsupported parameter 'interval': log entry sampling is not implemented; omit it to receive every matching entry")
		return
	}

	endNs := time.Now().UnixNano()
	var err error
	if raw := q.Get("end"); raw != "" {
		endNs, err = parseLokiTime(raw)
		if err != nil {
			writeLokiError(w, http.StatusBadRequest, "invalid 'end' parameter: "+err.Error())
			return
		}
	}
	startNs := endNs - int64(time.Hour)
	if raw := q.Get("start"); raw != "" {
		startNs, err = parseLokiTime(raw)
		if err != nil {
			writeLokiError(w, http.StatusBadRequest, "invalid 'start' parameter: "+err.Error())
			return
		}
	}
	if endNs < startNs {
		writeLokiError(w, http.StatusBadRequest, "invalid time range: 'end' must be >= 'start'")
		return
	}

	s.respondLokiStreams(w, r, "loki query_range failed", func() ([]logs.StreamResult, error) {
		return s.logQuery.QueryRange(r.Context(), sel, startNs, endNs, limit, dir)
	})
}

// handleLokiQuery serves the instant query endpoint. It accepts two expression
// kinds:
//
//   - a stream selector, evaluated as a log query over [0, time]. Upstream Loki
//     rejects log queries here with a 400 and directs them to query_range; we
//     accept them as a deliberate superset. Grafana never sends a log query to
//     this endpoint, so nothing depends on the stricter behavior.
//   - a constant metric expression such as vector(1)+vector(1), returning a
//     "vector" envelope. This is the Grafana Loki datasource health check.
func (s *Server) handleLokiQuery(w http.ResponseWriter, r *http.Request) {
	queryStr, limit, dir, ok := s.parseLokiQueryParams(w, r)
	if !ok {
		return
	}
	q := r.Form

	timeNs := time.Now().UnixNano()
	if raw := q.Get("time"); raw != "" {
		var err error
		timeNs, err = parseLokiTime(raw)
		if err != nil {
			writeLokiError(w, http.StatusBadRequest, "invalid 'time' parameter: "+err.Error())
			return
		}
	}

	// A query that does not open with a stream selector is a metric query; only
	// the constant subset is supported and everything else errors explicitly.
	if trimmed := strings.TrimSpace(queryStr); trimmed != "" && trimmed[0] != '{' {
		value, err := logs.ParseScalarQuery(trimmed)
		if err != nil {
			writeLokiError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeLokiVector(w, timeNs, value)
		return
	}

	sel, ok := parseLokiLogSelector(w, queryStr)
	if !ok {
		return
	}
	s.respondLokiStreams(w, r, "loki query failed", func() ([]logs.StreamResult, error) {
		return s.logQuery.QueryInstant(r.Context(), sel, timeNs, limit, dir)
	})
}

// handleLokiLabels returns all stream label names. start/end/query narrowing is
// accepted but ignored this phase (documented limitation).
func (s *Server) handleLokiLabels(w http.ResponseWriter, r *http.Request) {
	if !s.requireLogQuery(w, r) {
		return
	}
	writeLokiLabels(w, s.logQuery.LabelNames())
}

// handleLokiLabelValues returns all values for the {name} label.
func (s *Server) handleLokiLabelValues(w http.ResponseWriter, r *http.Request) {
	if !s.requireLogQuery(w, r) {
		return
	}
	name := chi.URLParam(r, "name")
	writeLokiLabels(w, s.logQuery.LabelValues(name))
}

func toLokiStreamResults(results []logs.StreamResult) []lokiStreamResult {
	out := make([]lokiStreamResult, 0, len(results))
	for _, rs := range results {
		values := make([][2]string, 0, len(rs.Entries))
		for _, e := range rs.Entries {
			values = append(values, [2]string{strconv.FormatInt(e.TimestampNs, 10), e.Line})
		}
		out = append(out, lokiStreamResult{Stream: rs.Labels, Values: values})
	}
	return out
}
