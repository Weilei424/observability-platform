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

// lokiVectorResponse is the `resultType: "vector"` envelope, used by the instant
// query endpoint for both kinds of metric expression it accepts: a real metric
// query evaluated over stored logs (one labeled sample per output series) and the
// constant subset behind Grafana's datasource health check (one label-less
// sample; see logs.ParseScalarQuery). Log queries always produce the "streams"
// envelope above, and range metric queries produce the "matrix" envelope below.
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

// lokiMatrixResponse is the `resultType: "matrix"` envelope returned by a metric
// query on query_range: one entry per output series, each with a label set and a
// list of [<epoch seconds>, "<value>"] pairs.
type lokiMatrixResponse struct {
	Status string         `json:"status"`
	Data   lokiMatrixData `json:"data"`
}

type lokiMatrixData struct {
	ResultType string             `json:"resultType"`
	Result     []lokiMatrixSeries `json:"result"`
	Stats      map[string]any     `json:"stats"`
}

type lokiMatrixSeries struct {
	Metric map[string]string `json:"metric"`
	Values [][2]any          `json:"values"` // [<epoch seconds>, "<value>"]
}

// lokiSampleValue formats one sample the way Loki does: the timestamp as epoch
// seconds (a JSON number, fractional when sub-second) and the value as a string.
// Formatting the value ourselves also keeps ±Inf and NaN serializable, which
// encoding/json would reject as bare floats.
func lokiSampleValue(tsNs int64, v float64) [2]any {
	return [2]any{float64(tsNs) / 1e9, strconv.FormatFloat(v, 'f', -1, 64)}
}

// writeLokiMatrix writes a Loki "matrix" success envelope. A nil result
// serializes as [] (never null); stats is an empty object placeholder.
func writeLokiMatrix(w http.ResponseWriter, result []lokiMatrixSeries) {
	if result == nil {
		result = []lokiMatrixSeries{}
	}
	writeJSON(w, http.StatusOK, lokiMatrixResponse{
		Status: "success",
		Data: lokiMatrixData{
			ResultType: "matrix",
			Result:     result,
			Stats:      map[string]any{},
		},
	})
}

// writeLokiVectorSamples writes a Loki "vector" success envelope holding any
// number of labeled samples. An instant metric query answers with this; the
// constant-expression shim answers with writeLokiVector below, which is the
// single label-less case.
func writeLokiVectorSamples(w http.ResponseWriter, result []lokiVectorSample) {
	if result == nil {
		result = []lokiVectorSample{}
	}
	writeJSON(w, http.StatusOK, lokiVectorResponse{
		Status: "success",
		Data: lokiVectorData{
			ResultType: "vector",
			Result:     result,
			Stats:      map[string]any{},
		},
	})
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
// Prometheus-style sample shape Loki uses.
func writeLokiVector(w http.ResponseWriter, tsNs int64, value float64) {
	writeLokiVectorSamples(w, []lokiVectorSample{{
		Metric: map[string]string{},
		Value:  lokiSampleValue(tsNs, value),
	}})
}

// writeLokiScalar writes a Loki "scalar" success envelope. Same timestamp and
// value encoding as writeLokiVector; only the result shape and type differ.
func writeLokiScalar(w http.ResponseWriter, tsNs int64, value float64) {
	writeJSON(w, http.StatusOK, lokiScalarResponse{
		Status: "success",
		Data: lokiScalarData{
			ResultType: "scalar",
			Result:     lokiSampleValue(tsNs, value),
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
