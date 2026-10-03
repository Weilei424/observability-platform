package rpc

import (
	"context"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/observability"
)

// MountReads registers the read routes over the given sources. The ingester
// mounts them over its heads, the store over its blocks and chunks; the
// querier's clients call them.
func MountReads(r chi.Router, m metrics.Source, l logs.Source) {
	r.Post("/metrics/select", func(w http.ResponseWriter, req *http.Request) { metricsSelect(w, req, m) })
	r.Get("/metrics/labels", labelNamesRoute(m.SelectLabelNames, "metrics"))
	r.Get("/metrics/label-values", labelValuesRoute(m.SelectLabelValues, "metrics"))
	r.Post("/logs/select", func(w http.ResponseWriter, req *http.Request) { logsSelect(w, req, l) })
	r.Get("/logs/labels", labelNamesRoute(l.SelectLabelNames, "logs"))
	r.Get("/logs/label-values", labelValuesRoute(l.SelectLabelValues, "logs"))
}

// labelNamesRoute answers a labels route over sel, shared by metrics and logs:
// they differ only in which Source method is bound as sel and the component
// name (what) that names the failure in the log line and its error message.
func labelNamesRoute(sel func(context.Context) ([]string, error), what string) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		names, err := sel(req.Context())
		if err != nil {
			internalError(w, req, what+" label names failed", err)
			return
		}
		writeJSON(w, http.StatusOK, namesResponse{Names: nonNil(names)})
	}
}

// labelValuesRoute answers a label-values route over sel, shared by metrics
// and logs as labelNamesRoute is above.
func labelValuesRoute(sel func(context.Context, string) ([]string, error), what string) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		name, ok := labelName(w, req)
		if !ok {
			return
		}
		values, err := sel(req.Context(), name)
		if err != nil {
			internalError(w, req, what+" label values failed", err)
			return
		}
		writeJSON(w, http.StatusOK, valuesResponse{Values: nonNil(values)})
	}
}

func metricsSelect(w http.ResponseWriter, r *http.Request, src metrics.Source) {
	body, ok := readBody(w, r, selectBodyLimit)
	if !ok {
		return
	}
	var req metricsSelectRequest
	if !decodeStrict(w, body, &req) {
		return
	}
	sel, err := selectorFromWire(req.Matchers)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	p := metrics.SelectParams{Selector: sel, MinT: req.MinMs, MaxT: req.MaxMs, Anchor: req.Anchor, SeriesOnly: req.SeriesOnly, AnyTime: req.AnyTime}
	if err := p.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	series, err := src.Select(r.Context(), p)
	if err != nil {
		internalError(w, r, "metrics select failed", err)
		return
	}
	writeJSON(w, http.StatusOK, metricsSelectResponse{Series: seriesToWire(series)})
}

func logsSelect(w http.ResponseWriter, r *http.Request, src logs.Source) {
	body, ok := readBody(w, r, selectBodyLimit)
	if !ok {
		return
	}
	var req logsSelectRequest
	if !decodeStrict(w, body, &req) {
		return
	}
	matchers, err := pairsFromWire(req.Matchers)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	streams, err := src.SelectStreams(r.Context(), matchers, req.MinNs, req.MaxNs)
	if err != nil {
		internalError(w, r, "logs select failed", err)
		return
	}
	writeJSON(w, http.StatusOK, logsSelectResponse{Streams: streamsToWire(streams)})
}

func labelName(w http.ResponseWriter, r *http.Request) (string, bool) {
	name := r.URL.Query().Get("name")
	if name == "" {
		writeError(w, http.StatusBadRequest, "missing required parameter 'name'")
		return "", false
	}
	return name, true
}

// statusClientClosedRequest is the de-facto (nginx) status for a caller that
// cancelled its own request; nobody is left to read it.
const statusClientClosedRequest = 499

// internalError logs a failure under the rpc component and answers 500 with
// its cause, which the calling client reports as ErrUnavailable. A failure
// caused by the caller cancelling its own request is not a server fault: it is
// logged at Debug and answered 499.
func internalError(w http.ResponseWriter, r *http.Request, msg string, err error) {
	log := observability.Component(observability.FromContext(r.Context()), "rpc")
	if errors.Is(err, context.Canceled) || r.Context().Err() != nil {
		log.Debug(msg+" (caller cancelled)", "err", err)
		writeError(w, statusClientClosedRequest, err.Error())
		return
	}
	log.Error(msg, "err", err)
	writeError(w, http.StatusInternalServerError, err.Error())
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// MountWrites registers the ingester's push routes: the gateway sends each
// ingester the samples and lines the ring assigns it, already validated. The
// labels are validated again — a protocol bug must not write invalid data —
// and a malformed body is a 400. ingest counts what lands; rejections were
// counted by the gateway, except append failures, counted here.
func MountWrites(r chi.Router, m metrics.Ingester, l logs.Ingester, ingest *observability.IngestMetrics) {
	r.Post("/metrics/push", func(w http.ResponseWriter, req *http.Request) { metricsPush(w, req, m, ingest) })
	r.Post("/logs/push", func(w http.ResponseWriter, req *http.Request) { logsPush(w, req, l, ingest) })
}

func metricsPush(w http.ResponseWriter, r *http.Request, ing metrics.Ingester, im *observability.IngestMetrics) {
	body, ok := readBody(w, r, FlushBodyLimit)
	if !ok {
		return
	}
	var req metricsPushRequest
	if !decodeStrict(w, body, &req) {
		return
	}
	var batch []metrics.PendingSample
	for _, s := range req.Series {
		labels, err := metrics.NewLabels(s.Labels)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid series labels: "+err.Error())
			return
		}
		for _, smp := range s.Samples {
			batch = append(batch, metrics.PendingSample{Labels: labels, TimestampMs: smp.T, Value: smp.V})
		}
	}
	for i, p := range batch {
		if err := ing.Append(p.Labels, p.TimestampMs, p.Value); err != nil {
			im.SamplesIngested.Add(float64(i))
			im.SamplesRejected.WithLabelValues(observability.ReasonAppend).Add(float64(len(batch) - i))
			internalError(w, r, "metrics push append failed", err)
			return
		}
	}
	im.SamplesIngested.Add(float64(len(batch)))
	w.WriteHeader(http.StatusNoContent)
}

func logsPush(w http.ResponseWriter, r *http.Request, ing logs.Ingester, im *observability.IngestMetrics) {
	body, ok := readBody(w, r, FlushBodyLimit)
	if !ok {
		return
	}
	var req logsPushRequest
	if !decodeStrict(w, body, &req) {
		return
	}
	var batch []logs.PendingEntry
	for _, s := range req.Streams {
		labels, err := logs.NewStreamLabels(s.Labels)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid stream labels: "+err.Error())
			return
		}
		for _, e := range s.Entries {
			batch = append(batch, logs.PendingEntry{Labels: labels, TimestampNs: e.T, Line: e.Line})
		}
	}
	for i, e := range batch {
		if err := ing.Append(e.Labels, e.TimestampNs, e.Line); err != nil {
			im.LogLinesIngested.Add(float64(i))
			im.LogLinesRejected.WithLabelValues(observability.ReasonAppend).Add(float64(len(batch) - i))
			internalError(w, r, "logs push append failed", err)
			return
		}
	}
	im.LogLinesIngested.Add(float64(len(batch)))
	w.WriteHeader(http.StatusNoContent)
}
