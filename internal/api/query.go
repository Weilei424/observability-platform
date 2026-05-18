package api

import (
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/masonwheeler/observability-platform/internal/metrics"
)

type promResponse struct {
	Status    string `json:"status"`
	Data      any    `json:"data,omitempty"`
	ErrorType string `json:"errorType,omitempty"`
	Error     string `json:"error,omitempty"`
}

type promVectorData struct {
	ResultType string       `json:"resultType"`
	Result     []promSample `json:"result"`
}

type promMatrixData struct {
	ResultType string       `json:"resultType"`
	Result     []promSeries `json:"result"`
}

type promSample struct {
	Metric map[string]string `json:"metric"`
	Value  [2]any            `json:"value"`
}

type promSeries struct {
	Metric map[string]string `json:"metric"`
	Values [][2]any          `json:"values"`
}

func writePromError(w http.ResponseWriter, status int, errorType, errMsg string) {
	writeJSON(w, status, promResponse{
		Status:    "error",
		ErrorType: errorType,
		Error:     errMsg,
	})
}

func msToPromTimestamp(ms int64) float64 {
	return float64(ms) / 1000.0
}

func formatPromValue(v float64) string {
	switch {
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	case math.IsNaN(v):
		return "NaN"
	default:
		return strconv.FormatFloat(v, 'f', -1, 64)
	}
}

func parseFloatSeconds(name, s string) (int64, error) {
	if s == "" {
		return 0, fmt.Errorf("missing required parameter '%s'", name)
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid parameter '%s': %s", name, s)
	}
	return int64(math.Round(f * 1000)), nil
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

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
		tMs, err = parseFloatSeconds("time", raw)
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

	writeJSON(w, http.StatusOK, promResponse{
		Status: "success",
		Data:   promVectorData{ResultType: "vector", Result: result},
	})
}

func (s *Server) handleQueryRange(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	queryStr := q.Get("query")
	if queryStr == "" {
		writePromError(w, http.StatusBadRequest, "bad_data", "missing required parameter 'query'")
		return
	}

	startMs, err := parseFloatSeconds("start", q.Get("start"))
	if err != nil {
		writePromError(w, http.StatusBadRequest, "bad_data", err.Error())
		return
	}

	endMs, err := parseFloatSeconds("end", q.Get("end"))
	if err != nil {
		writePromError(w, http.StatusBadRequest, "bad_data", err.Error())
		return
	}

	stepMs, err := parseFloatSeconds("step", q.Get("step"))
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

	writeJSON(w, http.StatusOK, promResponse{
		Status: "success",
		Data:   promMatrixData{ResultType: "matrix", Result: result},
	})
}
