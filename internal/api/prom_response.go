package api

import (
	"encoding/json"
	"math"
	"net/http"
	"strconv"
)

type promResponse struct {
	Status    string   `json:"status"`
	Data      any      `json:"data,omitempty"`
	ErrorType string   `json:"errorType,omitempty"`
	Error     string   `json:"error,omitempty"`
	Warnings  []string `json:"warnings"` // serialization controlled by MarshalJSON, not this tag
}

type promVectorData struct {
	ResultType string       `json:"resultType"`
	Result     []promSample `json:"result"`
}

type promMatrixData struct {
	ResultType string       `json:"resultType"`
	Result     []promSeries `json:"result"`
}

// promScalarData represents a single scalar result (e.g. scalar(some_expr)).
type promScalarData struct {
	ResultType string `json:"resultType"`
	Result     [2]any `json:"result"`
}

type promSample struct {
	Metric map[string]string `json:"metric"`
	Value  [2]any            `json:"value"`
}

type promSeries struct {
	Metric map[string]string `json:"metric"`
	Values [][2]any          `json:"values"`
}

// MarshalJSON implements custom marshaling for promResponse to handle the Warnings field properly:
// - For error responses (Status=="error"), Warnings is omitted even if non-nil
// - For success responses, Warnings is always included (as [] if empty)
func (r promResponse) MarshalJSON() ([]byte, error) {
	m := map[string]any{
		"status": r.Status,
	}

	// Only include these fields if non-empty
	if r.Data != nil {
		m["data"] = r.Data
	}
	if r.ErrorType != "" {
		m["errorType"] = r.ErrorType
	}
	if r.Error != "" {
		m["error"] = r.Error
	}

	// Include warnings for success responses (always, even if empty slice)
	if r.Status == "success" {
		m["warnings"] = r.Warnings
	}

	return json.Marshal(m)
}

// writePromSuccess writes a Prometheus-compatible success envelope, always including "warnings":[].
func writePromSuccess(w http.ResponseWriter, data any) {
	writeJSON(w, http.StatusOK, promResponse{
		Status:   "success",
		Data:     data,
		Warnings: []string{},
	})
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
