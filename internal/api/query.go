package api

import (
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/masonwheeler/observability-platform/internal/metrics"
)

// parseTimeParam parses a Prometheus time parameter (time, start, end).
// Accepts Unix timestamp as float seconds or RFC3339/RFC3339Nano.
func parseTimeParam(name, s string) (int64, error) {
	if s == "" {
		return 0, fmt.Errorf("missing required parameter '%s'", name)
	}
	f, err := strconv.ParseFloat(s, 64)
	if err == nil {
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return 0, fmt.Errorf("invalid parameter '%s': %s", name, s)
		}
		return int64(math.Round(f * 1000)), nil
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UnixMilli(), nil
		}
	}
	return 0, fmt.Errorf("invalid parameter '%s': %s", name, s)
}

// parseDurationParam parses a Prometheus step parameter.
// Accepts float seconds or Prometheus duration strings (15s, 1m, 1h30m, etc.).
func parseDurationParam(name, s string) (int64, error) {
	if s == "" {
		return 0, fmt.Errorf("missing required parameter '%s'", name)
	}
	f, err := strconv.ParseFloat(s, 64)
	if err == nil {
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return 0, fmt.Errorf("invalid parameter '%s': %s", name, s)
		}
		return int64(math.Round(f * 1000)), nil
	}
	ms, err := metrics.ParsePromDuration(s)
	if err != nil {
		return 0, fmt.Errorf("invalid parameter '%s': %s", name, s)
	}
	return ms, nil
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writePromError(w, http.StatusBadRequest, "bad_data", "invalid request body")
		return
	}
	q := r.Form

	queryStr := q.Get("query")
	if queryStr == "" {
		writePromError(w, http.StatusBadRequest, "bad_data", "missing required parameter 'query'")
		return
	}

	var tMs int64
	if raw := q.Get("time"); raw == "" {
		tMs = time.Now().UnixMilli()
	} else {
		var err error
		tMs, err = parseTimeParam("time", raw)
		if err != nil {
			writePromError(w, http.StatusBadRequest, "bad_data", err.Error())
			return
		}
	}

	expr, err := metrics.ParseExpr(queryStr)
	if err != nil {
		writePromError(w, http.StatusBadRequest, "bad_data", "invalid query: "+err.Error())
		return
	}

	samples, err := s.engine.EvalInstant(expr, tMs)
	if err != nil {
		writePromError(w, http.StatusInternalServerError, "execution", err.Error())
		return
	}

	// Scalar expressions (e.g. "1+1" from the Grafana datasource health check) must use
	// resultType:"scalar" per the Prometheus HTTP API spec.
	if _, ok := expr.(metrics.ScalarExpr); ok && len(samples) == 1 {
		s := samples[0]
		writePromSuccess(w, promScalarData{
			ResultType: "scalar",
			Result:     [2]any{msToPromTimestamp(s.TimestampMs), formatPromValue(s.Value)},
		})
		return
	}

	result := make([]promSample, len(samples))
	for i, sample := range samples {
		result[i] = promSample{
			Metric: sample.Labels.Map(),
			Value:  [2]any{msToPromTimestamp(sample.TimestampMs), formatPromValue(sample.Value)},
		}
	}

	writePromSuccess(w, promVectorData{ResultType: "vector", Result: result})
}

func (s *Server) handleQueryRange(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writePromError(w, http.StatusBadRequest, "bad_data", "invalid request body")
		return
	}
	q := r.Form

	queryStr := q.Get("query")
	if queryStr == "" {
		writePromError(w, http.StatusBadRequest, "bad_data", "missing required parameter 'query'")
		return
	}

	startMs, err := parseTimeParam("start", q.Get("start"))
	if err != nil {
		writePromError(w, http.StatusBadRequest, "bad_data", err.Error())
		return
	}

	endMs, err := parseTimeParam("end", q.Get("end"))
	if err != nil {
		writePromError(w, http.StatusBadRequest, "bad_data", err.Error())
		return
	}

	stepMs, err := parseDurationParam("step", q.Get("step"))
	if err != nil {
		writePromError(w, http.StatusBadRequest, "bad_data", err.Error())
		return
	}

	if stepMs <= 0 {
		writePromError(w, http.StatusBadRequest, "bad_data", "invalid parameter 'step': must be greater than 0")
		return
	}
	if endMs < startMs {
		writePromError(w, http.StatusBadRequest, "bad_data", "invalid parameter 'end': must be >= start")
		return
	}

	expr, err := metrics.ParseExpr(queryStr)
	if err != nil {
		writePromError(w, http.StatusBadRequest, "bad_data", "invalid query: "+err.Error())
		return
	}

	series, err := s.engine.EvalRange(expr, startMs, endMs, stepMs)
	if err != nil {
		writePromError(w, http.StatusInternalServerError, "execution", err.Error())
		return
	}

	result := make([]promSeries, len(series))
	for i, rs := range series {
		values := make([][2]any, len(rs.Points))
		for j, pt := range rs.Points {
			values[j] = [2]any{msToPromTimestamp(pt.TimestampMs), formatPromValue(pt.Value)}
		}
		result[i] = promSeries{
			Metric: rs.Labels.Map(),
			Values: values,
		}
	}

	writePromSuccess(w, promMatrixData{ResultType: "matrix", Result: result})
}
