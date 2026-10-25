package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/masonwheeler/observability-platform/internal/drain"
	mlabels "github.com/masonwheeler/observability-platform/internal/labels"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/observability"
	"github.com/masonwheeler/observability-platform/internal/tenant"
)

const (
	defaultMetricsPushBytes = 1 << 20
	defaultLogsPushBytes    = 4 << 20
)

// pushCap is the push-body cap for the request's tenant: its configured limit,
// or def when no limits are configured.
// maxNamedTenantLabels is the label cap before the tenant label is added: the
// stored form allows 255, and a named tenant's series carries one more.
const maxNamedTenantLabels = 254

func (s *Server) pushCap(r *http.Request, def int64, limit func(*tenant.Limits, string) int64) int64 {
	if s.tenants == nil {
		return def
	}
	return limit(s.tenants, tenant.From(r.Context()))
}

// rejectOversizePush answers 413 when err is a body-over-cap error, reporting
// whether it did. Only a cap from the tenant resolver counts as a per-tenant
// limit rejection; the fixed safety caps (no resolver) do not.
func (s *Server) rejectOversizePush(w http.ResponseWriter, r *http.Request, err error) bool {
	var mbe *http.MaxBytesError
	if !errors.As(err, &mbe) {
		return false
	}
	id := tenant.From(r.Context())
	if s.tenants != nil && s.onTenantReject != nil {
		s.onTenantReject(id, "push_bytes")
	}
	writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{
		"error": fmt.Sprintf("push body exceeds %d bytes for tenant %s", mbe.Limit, id)})
	return true
}

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
	r.Body = http.MaxBytesReader(w, r.Body, s.pushCap(r, defaultMetricsPushBytes, (*tenant.Limits).MaxMetricsPushBytes))

	dec := json.NewDecoder(r.Body)
	var req ingestRequest
	if err := dec.Decode(&req); err != nil {
		if s.rejectOversizePush(w, r, err) {
			return
		}
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	// A well-formed request is exactly one JSON object; anything after it is malformed.
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if s.rejectOversizePush(w, r, err) {
			return
		}
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "unexpected trailing data after JSON body"})
		return
	}

	if len(req.Metrics) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "metrics array is empty or missing"})
		return
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
	samples := make([]metrics.PendingSample, 0, len(req.Metrics))

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

		if len(labelMap) > maxNamedTenantLabels && tenant.From(r.Context()) != tenant.Default {
			recordValidationError(i, "labels", "too many labels: maximum is 254 for a named tenant (one label is reserved for the tenant)")
			continue
		}
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

		labels = mlabels.WithTenant(labels, tenant.From(r.Context()))
		samples = append(samples, metrics.PendingSample{Labels: labels, TimestampMs: *entry.TimestampMs, Value: *entry.Value})
	}

	if len(validationErrors) > 0 {
		for _, field := range firstRejectField {
			s.ingest.SamplesRejected.WithLabelValues(metricRejectReason(field)).Inc()
		}
		// samples holds the entries that passed validation on their own; they are
		// still discarded because the batch is rejected atomically. That is
		// collateral loss, not an invalidity of their own, so it is counted under
		// the distinct "batch" reason rather than folded into the reasons above.
		if len(samples) > 0 {
			s.ingest.SamplesRejected.WithLabelValues(observability.ReasonBatch).Add(float64(len(samples)))
		}
		writeJSON(w, http.StatusBadRequest, map[string]any{"errors": validationErrors})
		return
	}

	if s.writes != nil {
		if err := s.writes.PushSamples(r.Context(), samples); err != nil {
			s.writeRouteError(w, r, "metrics_ingest", err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}

	appendLocal := func() error {
		var appendErrors []error
		var appended int
		log := observability.Component(observability.FromContext(r.Context()), "metrics_ingest")
		for i, ps := range samples {
			if err := s.ingester.Append(ps.Labels, ps.TimestampMs, ps.Value); err != nil {
				if errors.Is(err, drain.ErrDraining) {
					// A draining ingester refuses the rest of the batch: 503, so the
					// client retries it against the ring, not a server error.
					s.ingest.SamplesIngested.Add(float64(appended))
					s.ingest.SamplesRejected.WithLabelValues(observability.ReasonAppend).Add(float64(len(appendErrors) + len(samples) - i))
					writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
					return nil
				}
				log.Error("ingester append failed", "err", err)
				appendErrors = append(appendErrors, err)
				continue
			}
			appended++
		}
		// Count what actually landed, before the error branch: a partial append leaves
		// those samples in the store, and reporting zero would understate ingest.
		s.ingest.SamplesIngested.Add(float64(appended))
		s.ingest.SamplesRejected.WithLabelValues(observability.ReasonAppend).Add(float64(len(appendErrors)))
		if len(appendErrors) > 0 {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
			return nil
		}

		w.WriteHeader(http.StatusNoContent)
		return nil

	}
	if s.seriesGate == nil {
		_ = appendLocal()
		return
	}
	keys := map[string][]uint64{}
	for _, ps := range samples {
		t := mlabels.TenantOf(ps.Labels)
		keys[t] = append(keys[t], ps.Labels.Hash())
	}
	if err := s.seriesGate.Admit(keys, appendLocal); err != nil {
		s.writeLimitError(w, r, "metrics_ingest", err)
	}
}
