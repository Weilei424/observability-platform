package api

import (
	"fmt"
	"math"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/masonwheeler/observability-platform/internal/metrics"
)

// parseOptionalTimeRange parses and cross-validates optional start/end params,
// returning the time component of a metrics.MetadataFilter. Either or both may
// be absent. A missing bound is widened to the min/max representable timestamp
// (Prometheus semantics) when the other bound is present. Returns an error if a
// present param is malformed or if both are present and end < start.
func parseOptionalTimeRange(startRaw, endRaw string) (startMs, endMs int64, hasTime bool, err error) {
	var hasStart, hasEnd bool
	if startRaw != "" {
		ms, perr := parseTimeParam("start", startRaw)
		if perr != nil {
			return 0, 0, false, perr
		}
		startMs = ms
		hasStart = true
	}
	if endRaw != "" {
		ms, perr := parseTimeParam("end", endRaw)
		if perr != nil {
			return 0, 0, false, perr
		}
		endMs = ms
		hasEnd = true
	}
	if hasStart && hasEnd && endMs < startMs {
		return 0, 0, false, fmt.Errorf("invalid parameter 'end': must be >= start")
	}
	if !hasStart && hasEnd {
		startMs = math.MinInt64
	}
	if hasStart && !hasEnd {
		endMs = math.MaxInt64
	}
	return startMs, endMs, hasStart || hasEnd, nil
}

// parseMatchSelectors parses each match[] expression into a Selector. A nil
// result with nil error means no match[] params were supplied.
func parseMatchSelectors(matchStrings []string) ([]metrics.Selector, error) {
	if len(matchStrings) == 0 {
		return nil, nil
	}
	selectors := make([]metrics.Selector, 0, len(matchStrings))
	for _, ms := range matchStrings {
		sel, err := metrics.ParseSelector(ms)
		if err != nil {
			return nil, fmt.Errorf("invalid match[] selector: %s", err.Error())
		}
		selectors = append(selectors, sel)
	}
	return selectors, nil
}

func (s *Server) handleLabels(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writePromError(w, http.StatusBadRequest, "bad_data", "invalid request body")
		return
	}
	filter, ok := s.metadataFilter(w, r)
	if !ok {
		return
	}
	names := s.engine.LabelNames(filter)
	writePromSuccess(w, names)
}

func (s *Server) handleLabelValues(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writePromError(w, http.StatusBadRequest, "bad_data", "invalid request body")
		return
	}
	name := chi.URLParam(r, "name")
	filter, ok := s.metadataFilter(w, r)
	if !ok {
		return
	}
	values := s.engine.LabelValues(name, filter)
	writePromSuccess(w, values)
}

func (s *Server) handleSeries(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writePromError(w, http.StatusBadRequest, "bad_data", "invalid request body")
		return
	}
	if len(r.Form["match[]"]) == 0 {
		writePromError(w, http.StatusBadRequest, "bad_data", "at least one match[] parameter is required")
		return
	}
	filter, ok := s.metadataFilter(w, r)
	if !ok {
		return
	}
	seriesList := s.engine.Series(filter)
	result := make([]map[string]string, len(seriesList))
	for i, labels := range seriesList {
		result[i] = labels.Map()
	}
	writePromSuccess(w, result)
}

// metadataFilter builds a metrics.MetadataFilter from the request's match[],
// start, and end params. On any validation error it writes a 400 response and
// returns ok=false; callers must stop on a false result.
func (s *Server) metadataFilter(w http.ResponseWriter, r *http.Request) (metrics.MetadataFilter, bool) {
	selectors, err := parseMatchSelectors(r.Form["match[]"])
	if err != nil {
		writePromError(w, http.StatusBadRequest, "bad_data", err.Error())
		return metrics.MetadataFilter{}, false
	}
	startMs, endMs, hasTime, err := parseOptionalTimeRange(r.Form.Get("start"), r.Form.Get("end"))
	if err != nil {
		writePromError(w, http.StatusBadRequest, "bad_data", err.Error())
		return metrics.MetadataFilter{}, false
	}
	return metrics.MetadataFilter{
		Selectors: selectors,
		StartMs:   startMs,
		EndMs:     endMs,
		HasTime:   hasTime,
	}, true
}
