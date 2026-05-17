package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/masonwheeler/observability-platform/internal/metrics"
)

type ingestRequest struct {
	Metrics []ingestEntry `json:"metrics"`
}

type ingestEntry struct {
	Name        string            `json:"name"`
	Labels      map[string]string `json:"labels"`
	TimestampMs *int64            `json:"timestamp_ms"`
	Value       *float64          `json:"value"`
}

type ingestErrorItem struct {
	Index   int    `json:"index"`
	Field   string `json:"field"`
	Message string `json:"message"`
}

func (s *Server) handleIngestMetrics(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	var req ingestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}

	if len(req.Metrics) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "metrics array is empty or missing"})
		return
	}

	type pending struct {
		labels      metrics.Labels
		timestampMs int64
		value       float64
	}

	var validationErrors []ingestErrorItem
	samples := make([]pending, 0, len(req.Metrics))

	for i, entry := range req.Metrics {
		var entryHasError bool

		if entry.TimestampMs == nil {
			validationErrors = append(validationErrors, ingestErrorItem{Index: i, Field: "timestamp_ms", Message: "required"})
			entryHasError = true
		}
		if entry.Value == nil {
			validationErrors = append(validationErrors, ingestErrorItem{Index: i, Field: "value", Message: "required"})
			entryHasError = true
		}

		labelMap := make(map[string]string, len(entry.Labels)+1)
		for k, v := range entry.Labels {
			labelMap[k] = v
		}
		labelMap["__name__"] = entry.Name

		labels, err := metrics.NewLabels(labelMap)
		if err != nil {
			var ve *metrics.ValidationError
			if errors.As(err, &ve) {
				validationErrors = append(validationErrors, ingestErrorItem{Index: i, Field: ve.Field, Message: ve.Message})
			} else {
				validationErrors = append(validationErrors, ingestErrorItem{Index: i, Field: "unknown", Message: err.Error()})
			}
			entryHasError = true
		}

		if entryHasError {
			continue
		}

		if err := metrics.ValidateSample(metrics.Sample{TimestampMs: *entry.TimestampMs, Value: *entry.Value}); err != nil {
			var ve *metrics.ValidationError
			if errors.As(err, &ve) {
				validationErrors = append(validationErrors, ingestErrorItem{Index: i, Field: ve.Field, Message: ve.Message})
			} else {
				validationErrors = append(validationErrors, ingestErrorItem{Index: i, Field: "unknown", Message: err.Error()})
			}
			continue
		}

		samples = append(samples, pending{labels: labels, timestampMs: *entry.TimestampMs, value: *entry.Value})
	}

	if len(validationErrors) > 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"errors": validationErrors})
		return
	}

	var appendErrors []error
	for _, ps := range samples {
		if err := s.ingester.Append(ps.labels, ps.timestampMs, ps.value); err != nil {
			s.log.Error("ingester append failed", "err", err)
			appendErrors = append(appendErrors, err)
		}
	}
	if len(appendErrors) > 0 {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
