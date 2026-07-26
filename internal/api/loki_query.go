package api

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/masonwheeler/observability-platform/internal/logs"
)

// parseLokiTime parses a Loki timestamp: a nanosecond Unix epoch (integer) or
// RFC3339/RFC3339Nano. Loki does not use Prometheus float-seconds.
func parseLokiTime(s string) (int64, error) {
	if ns, err := strconv.ParseInt(s, 10, 64); err == nil {
		return ns, nil
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UnixNano(), nil
		}
	}
	return 0, fmt.Errorf("invalid timestamp %q: want a nanosecond epoch or RFC3339", s)
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
func (s *Server) requireLogQuery(w http.ResponseWriter) bool {
	if s.logQuery == nil {
		writeLokiError(w, http.StatusInternalServerError, "logs query engine not configured")
		return false
	}
	return true
}

func (s *Server) handleLokiQueryRange(w http.ResponseWriter, r *http.Request) {
	if !s.requireLogQuery(w) {
		return
	}
	if err := r.ParseForm(); err != nil {
		writeLokiError(w, http.StatusBadRequest, "invalid request: "+err.Error())
		return
	}
	q := r.Form
	queryStr := q.Get("query")
	if queryStr == "" {
		writeLokiError(w, http.StatusBadRequest, "missing required parameter 'query'")
		return
	}
	sel, err := logs.ParseLogQL(queryStr)
	if err != nil {
		writeLokiError(w, http.StatusBadRequest, err.Error())
		return
	}

	endNs := time.Now().UnixNano()
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
	limit, err := parseLokiLimit(q.Get("limit"))
	if err != nil {
		writeLokiError(w, http.StatusBadRequest, err.Error())
		return
	}
	dir, err := parseLokiDirection(q.Get("direction"))
	if err != nil {
		writeLokiError(w, http.StatusBadRequest, err.Error())
		return
	}

	results, err := s.logQuery.QueryRange(sel, startNs, endNs, limit, dir)
	if err != nil {
		s.log.Error("loki query_range failed", "component", "logs_query",
			"request_id", chimiddleware.GetReqID(r.Context()), "err", err)
		writeLokiError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeLokiStreams(w, toLokiStreamResults(results))
}

func (s *Server) handleLokiQuery(w http.ResponseWriter, r *http.Request) {
	if !s.requireLogQuery(w) {
		return
	}
	if err := r.ParseForm(); err != nil {
		writeLokiError(w, http.StatusBadRequest, "invalid request: "+err.Error())
		return
	}
	q := r.Form
	queryStr := q.Get("query")
	if queryStr == "" {
		writeLokiError(w, http.StatusBadRequest, "missing required parameter 'query'")
		return
	}
	sel, err := logs.ParseLogQL(queryStr)
	if err != nil {
		writeLokiError(w, http.StatusBadRequest, err.Error())
		return
	}
	timeNs := time.Now().UnixNano()
	if raw := q.Get("time"); raw != "" {
		timeNs, err = parseLokiTime(raw)
		if err != nil {
			writeLokiError(w, http.StatusBadRequest, "invalid 'time' parameter: "+err.Error())
			return
		}
	}
	limit, err := parseLokiLimit(q.Get("limit"))
	if err != nil {
		writeLokiError(w, http.StatusBadRequest, err.Error())
		return
	}
	dir, err := parseLokiDirection(q.Get("direction"))
	if err != nil {
		writeLokiError(w, http.StatusBadRequest, err.Error())
		return
	}
	results, err := s.logQuery.QueryInstant(sel, timeNs, limit, dir)
	if err != nil {
		s.log.Error("loki query failed", "component", "logs_query",
			"request_id", chimiddleware.GetReqID(r.Context()), "err", err)
		writeLokiError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeLokiStreams(w, toLokiStreamResults(results))
}

// handleLokiLabels returns all stream label names. start/end/query narrowing is
// accepted but ignored this phase (documented limitation).
func (s *Server) handleLokiLabels(w http.ResponseWriter, r *http.Request) {
	if !s.requireLogQuery(w) {
		return
	}
	writeLokiLabels(w, s.logQuery.LabelNames())
}

// handleLokiLabelValues returns all values for the {name} label.
func (s *Server) handleLokiLabelValues(w http.ResponseWriter, r *http.Request) {
	if !s.requireLogQuery(w) {
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
