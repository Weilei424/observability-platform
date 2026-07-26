package api

import "net/http"

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
