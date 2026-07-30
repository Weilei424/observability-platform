package api

import (
	"net/http"
	"strconv"
)

type lokiResponse struct {
	Status string   `json:"status"`
	Data   lokiData `json:"data"`
}

type lokiData struct {
	ResultType string             `json:"resultType"`
	Result     []lokiStreamResult `json:"result"`
	Stats      map[string]any     `json:"stats"`
}

type lokiStreamResult struct {
	Stream map[string]string `json:"stream"`
	Values [][2]string       `json:"values"` // ["<tsNs decimal>", "<line>"]
}

type lokiLabelResponse struct {
	Status string   `json:"status"`
	Data   []string `json:"data"`
}

// lokiVectorResponse is the `resultType: "vector"` envelope. It exists only for
// the constant metric-query subset (see logs.ParseScalarQuery); log queries
// always produce the "streams" envelope above.
type lokiVectorResponse struct {
	Status string         `json:"status"`
	Data   lokiVectorData `json:"data"`
}

type lokiVectorData struct {
	ResultType string             `json:"resultType"`
	Result     []lokiVectorSample `json:"result"`
	Stats      map[string]any     `json:"stats"`
}

type lokiVectorSample struct {
	Metric map[string]string `json:"metric"`
	Value  [2]any            `json:"value"` // [<epoch seconds>, "<value>"]
}

// lokiScalarResponse is the `resultType: "scalar"` envelope, which upstream
// returns for a literal-only expression such as `1+1`. The result is a bare
// sample pair rather than a list of samples, and carries no labels.
type lokiScalarResponse struct {
	Status string         `json:"status"`
	Data   lokiScalarData `json:"data"`
}

type lokiScalarData struct {
	ResultType string         `json:"resultType"`
	Result     [2]any         `json:"result"` // [<epoch seconds>, "<value>"]
	Stats      map[string]any `json:"stats"`
}

// writeLokiStreams writes a Loki "streams" success envelope. A nil result
// serializes as [] (never null); stats is an empty object placeholder.
func writeLokiStreams(w http.ResponseWriter, result []lokiStreamResult) {
	if result == nil {
		result = []lokiStreamResult{}
	}
	writeJSON(w, http.StatusOK, lokiResponse{
		Status: "success",
		Data: lokiData{
			ResultType: "streams",
			Result:     result,
			Stats:      map[string]any{},
		},
	})
}

// writeLokiVector writes a Loki "vector" success envelope holding one label-less
// sample. Timestamps are epoch seconds and the value is a string, matching the
// Prometheus-style sample shape Loki uses. Formatting the value ourselves also
// keeps ±Inf serializable, which encoding/json would reject as a bare float.
func writeLokiVector(w http.ResponseWriter, tsNs int64, value float64) {
	writeJSON(w, http.StatusOK, lokiVectorResponse{
		Status: "success",
		Data: lokiVectorData{
			ResultType: "vector",
			Result: []lokiVectorSample{{
				Metric: map[string]string{},
				Value:  [2]any{float64(tsNs) / 1e9, strconv.FormatFloat(value, 'f', -1, 64)},
			}},
			Stats: map[string]any{},
		},
	})
}

// writeLokiScalar writes a Loki "scalar" success envelope. Same timestamp and
// value encoding as writeLokiVector; only the result shape and type differ.
func writeLokiScalar(w http.ResponseWriter, tsNs int64, value float64) {
	writeJSON(w, http.StatusOK, lokiScalarResponse{
		Status: "success",
		Data: lokiScalarData{
			ResultType: "scalar",
			Result:     [2]any{float64(tsNs) / 1e9, strconv.FormatFloat(value, 'f', -1, 64)},
			Stats:      map[string]any{},
		},
	})
}

// writeLokiLabels writes a Loki label-discovery success envelope.
func writeLokiLabels(w http.ResponseWriter, values []string) {
	if values == nil {
		values = []string{}
	}
	writeJSON(w, http.StatusOK, lokiLabelResponse{Status: "success", Data: values})
}

// writeLokiError writes a plain-text error body + status, matching Loki's query
// endpoints (intentionally distinct from the Prometheus JSON error envelope).
func writeLokiError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(msg))
}
