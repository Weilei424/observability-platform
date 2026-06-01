package api

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/masonwheeler/observability-platform/internal/metrics"
)

// validateOptionalTimeRange parses and cross-validates optional start/end params.
// Either or both may be absent. Returns an error if a present param is malformed
// or if both are present and end < start.
func validateOptionalTimeRange(startRaw, endRaw string) error {
	var startMs, endMs int64
	var hasStart, hasEnd bool

	if startRaw != "" {
		ms, err := parseTimeParam("start", startRaw)
		if err != nil {
			return err
		}
		startMs = ms
		hasStart = true
	}
	if endRaw != "" {
		ms, err := parseTimeParam("end", endRaw)
		if err != nil {
			return err
		}
		endMs = ms
		hasEnd = true
	}
	if hasStart && hasEnd && endMs < startMs {
		return fmt.Errorf("invalid parameter 'end': must be >= start")
	}
	return nil
}

func (s *Server) handleLabels(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writePromError(w, http.StatusBadRequest, "bad_data", "invalid request body")
		return
	}
	if err := validateOptionalTimeRange(r.Form.Get("start"), r.Form.Get("end")); err != nil {
		writePromError(w, http.StatusBadRequest, "bad_data", err.Error())
		return
	}
	names := s.engine.LabelNames()
	writePromSuccess(w, names)
}

func (s *Server) handleLabelValues(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writePromError(w, http.StatusBadRequest, "bad_data", "invalid request body")
		return
	}
	name := chi.URLParam(r, "name")
	if err := validateOptionalTimeRange(r.Form.Get("start"), r.Form.Get("end")); err != nil {
		writePromError(w, http.StatusBadRequest, "bad_data", err.Error())
		return
	}
	values := s.engine.LabelValues(name)
	writePromSuccess(w, values)
}

func (s *Server) handleSeries(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writePromError(w, http.StatusBadRequest, "bad_data", "invalid request body")
		return
	}
	matchStrings := r.Form["match[]"]
	if len(matchStrings) == 0 {
		writePromError(w, http.StatusBadRequest, "bad_data", "at least one match[] parameter is required")
		return
	}
	if err := validateOptionalTimeRange(r.Form.Get("start"), r.Form.Get("end")); err != nil {
		writePromError(w, http.StatusBadRequest, "bad_data", err.Error())
		return
	}
	selectors := make([]metrics.Selector, 0, len(matchStrings))
	for _, ms := range matchStrings {
		sel, err := metrics.ParseSelector(ms)
		if err != nil {
			writePromError(w, http.StatusBadRequest, "bad_data", "invalid match[] selector: "+err.Error())
			return
		}
		selectors = append(selectors, sel)
	}
	seriesList := s.engine.Series(selectors)
	result := make([]map[string]string, len(seriesList))
	for i, labels := range seriesList {
		result[i] = labels.Map()
	}
	writePromSuccess(w, result)
}
