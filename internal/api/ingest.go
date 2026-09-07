package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/observability"
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

	dec := json.NewDecoder(r.Body)
	var req ingestRequest
	if err := dec.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	// A well-formed request is exactly one JSON object; anything after it is malformed.
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "unexpected trailing data after JSON body"})
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
	// firstRejectField holds, per rejected entry index, the field of the FIRST
	// validation error recorded for that entry. One malformed entry (e.g. a
	// sample missing both timestamp_ms and value) can append several items to
	// validationErrors, but it is still exactly one rejected sample; counting
	// every item would inflate obs_samples_rejected_total to more than the
	// number of samples actually rejected. Each rejected entry is therefore
	// counted once, attributed to its first error.
	firstRejectField := make(map[int]string, len(req.Metrics))
	recordValidationError := func(i int, field, message string) {
		validationErrors = append(validationErrors, ingestErrorItem{Index: i, Field: field, Message: message})
		if _, ok := firstRejectField[i]; !ok {
			firstRejectField[i] = field
		}
	}
	samples := make([]pending, 0, len(req.Metrics))

	for i, entry := range req.Metrics {
		var entryHasError bool

		if entry.TimestampMs == nil {
			recordValidationError(i, "timestamp_ms", "required")
			entryHasError = true
		}
		if entry.Value == nil {
			recordValidationError(i, "value", "required")
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
				recordValidationError(i, ve.Field, ve.Message)
			} else {
				recordValidationError(i, "unknown", err.Error())
			}
			entryHasError = true
		}

		if entryHasError {
			continue
		}

		if err := metrics.ValidateSample(metrics.Sample{TimestampMs: *entry.TimestampMs, Value: *entry.Value}); err != nil {
			var ve *metrics.ValidationError
			if errors.As(err, &ve) {
				recordValidationError(i, ve.Field, ve.Message)
			} else {
				recordValidationError(i, "unknown", err.Error())
			}
			continue
		}

		samples = append(samples, pending{labels: labels, timestampMs: *entry.TimestampMs, value: *entry.Value})
	}

	if len(validationErrors) > 0 {
		for _, field := range firstRejectField {
			s.ingest.SamplesRejected.WithLabelValues(metricRejectReason(field)).Inc()
		}
		writeJSON(w, http.StatusBadRequest, map[string]any{"errors": validationErrors})
		return
	}

	var appendErrors []error
	var appended int
	log := observability.Component(observability.FromContext(r.Context()), "metrics_ingest")
	for _, ps := range samples {
		if err := s.ingester.Append(ps.labels, ps.timestampMs, ps.value); err != nil {
			log.Error("ingester append failed", "err", err)
			appendErrors = append(appendErrors, err)
			continue
		}
		appended++
	}
	// Count what actually landed, before the error branch: a partial append leaves
	// those samples in the store, and reporting zero would understate ingest.
	s.ingest.SamplesIngested.Add(float64(appended))
	s.ingest.SamplesRejected.WithLabelValues("append").Add(float64(len(appendErrors)))
	if len(appendErrors) > 0 {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
