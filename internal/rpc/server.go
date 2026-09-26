package rpc

import (
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
	r.Get("/metrics/labels", func(w http.ResponseWriter, req *http.Request) {
		names, err := m.SelectLabelNames(req.Context())
		if err != nil {
			internalError(w, req, "metrics label names failed", err)
			return
		}
		writeJSON(w, http.StatusOK, namesResponse{Names: nonNil(names)})
	})
	r.Get("/metrics/label-values", func(w http.ResponseWriter, req *http.Request) {
		name, ok := labelName(w, req)
		if !ok {
			return
		}
		values, err := m.SelectLabelValues(req.Context(), name)
		if err != nil {
			internalError(w, req, "metrics label values failed", err)
			return
		}
		writeJSON(w, http.StatusOK, valuesResponse{Values: nonNil(values)})
	})
	r.Post("/logs/select", func(w http.ResponseWriter, req *http.Request) { logsSelect(w, req, l) })
	r.Get("/logs/labels", func(w http.ResponseWriter, req *http.Request) {
		names, err := l.SelectLabelNames(req.Context())
		if err != nil {
			internalError(w, req, "logs label names failed", err)
			return
		}
		writeJSON(w, http.StatusOK, namesResponse{Names: nonNil(names)})
	})
	r.Get("/logs/label-values", func(w http.ResponseWriter, req *http.Request) {
		name, ok := labelName(w, req)
		if !ok {
			return
		}
		values, err := l.SelectLabelValues(req.Context(), name)
		if err != nil {
			internalError(w, req, "logs label values failed", err)
			return
		}
		writeJSON(w, http.StatusOK, valuesResponse{Values: nonNil(values)})
	})
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

// internalError logs a failure under the rpc component and answers 500 with
// its cause, which the calling client reports as ErrUnavailable.
func internalError(w http.ResponseWriter, r *http.Request, msg string, err error) {
	observability.Component(observability.FromContext(r.Context()), "rpc").Error(msg, "err", err)
	writeError(w, http.StatusInternalServerError, err.Error())
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}
