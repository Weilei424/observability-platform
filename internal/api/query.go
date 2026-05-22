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
	ms, err := parsePromDuration(s)
	if err != nil {
		return 0, fmt.Errorf("invalid parameter '%s': %s", name, s)
	}
	return ms, nil
}

// parsePromDuration parses a Prometheus duration string like "15s", "1m", "1h30m".
// Units: ms, s, m, h, d, w, y.
func parsePromDuration(s string) (int64, error) {
	if s == "" {
		return 0, fmt.Errorf("empty duration")
	}
	var total int64
	remaining := s
	for remaining != "" {
		i := 0
		for i < len(remaining) && remaining[i] >= '0' && remaining[i] <= '9' {
			i++
		}
		if i == 0 {
			return 0, fmt.Errorf("expected digits in %q", s)
		}
		n, err := strconv.ParseInt(remaining[:i], 10, 64)
		if err != nil {
			return 0, err
		}
		remaining = remaining[i:]
		if remaining == "" {
			return 0, fmt.Errorf("missing unit in %q", s)
		}
		var unit string
		if len(remaining) >= 2 && remaining[:2] == "ms" {
			unit = "ms"
		} else {
			unit = string(remaining[0])
		}
		remaining = remaining[len(unit):]
		var mult int64
		switch unit {
		case "ms":
			mult = 1
		case "s":
			mult = 1000
		case "m":
			mult = 60 * 1000
		case "h":
			mult = 3600 * 1000
		case "d":
			mult = 24 * 3600 * 1000
		case "w":
			mult = 7 * 24 * 3600 * 1000
		case "y":
			mult = 365 * 24 * 3600 * 1000
		default:
			return 0, fmt.Errorf("unknown unit %q in %q", unit, s)
		}
		total += n * mult
	}
	return total, nil
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

	sel, err := metrics.ParseSelector(queryStr)
	if err != nil {
		writePromError(w, http.StatusBadRequest, "bad_data", "invalid query: "+err.Error())
		return
	}

	samples, err := s.engine.InstantQuery(sel, tMs)
	if err != nil {
		writePromError(w, http.StatusInternalServerError, "execution", err.Error())
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

	sel, err := metrics.ParseSelector(queryStr)
	if err != nil {
		writePromError(w, http.StatusBadRequest, "bad_data", "invalid query: "+err.Error())
		return
	}

	series, err := s.engine.RangeQuery(sel, startMs, endMs, stepMs)
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
