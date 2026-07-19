package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/masonwheeler/observability-platform/internal/logs"
)

type lokiPushRequest struct {
	Streams []lokiStream `json:"streams"`
}

type lokiStream struct {
	Stream map[string]string `json:"stream"`
	Values [][]string        `json:"values"` // each entry: ["<unix_nano>", "<line>"]
}

// handleLokiPush accepts a Loki-style JSON push payload. It validates every entry
// first; on any error it returns 400 with the full error list and buffers nothing.
// Otherwise it appends each accepted entry (WAL-before-buffer) and returns 204.
func (s *Server) handleLokiPush(w http.ResponseWriter, r *http.Request) {
	if ct := r.Header.Get("Content-Type"); ct != "" && !strings.HasPrefix(ct, "application/json") {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "unsupported content-type: only application/json is supported"})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4<<20)

	var req lokiPushRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	if len(req.Streams) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "streams array is empty or missing"})
		return
	}

	type pending struct {
		labels logs.StreamLabels
		tsNs   int64
		line   string
	}
	var validationErrors []ingestErrorItem
	entries := make([]pending, 0, len(req.Streams))

	for i, st := range req.Streams {
		sl, err := logs.NewStreamLabels(st.Stream)
		if err != nil {
			var ve *logs.ValidationError
			if errors.As(err, &ve) {
				validationErrors = append(validationErrors, ingestErrorItem{Index: i, Field: ve.Field, Message: ve.Message})
			} else {
				validationErrors = append(validationErrors, ingestErrorItem{Index: i, Field: "stream", Message: err.Error()})
			}
			continue
		}
		for _, v := range st.Values {
			if len(v) != 2 {
				validationErrors = append(validationErrors, ingestErrorItem{Index: i, Field: "values", Message: "each value must be a [timestamp, line] pair; structured metadata is not supported"})
				continue
			}
			tsNs, perr := strconv.ParseInt(v[0], 10, 64)
			if perr != nil {
				validationErrors = append(validationErrors, ingestErrorItem{Index: i, Field: "timestamp", Message: "invalid nanosecond timestamp: " + v[0]})
				continue
			}
			if verr := logs.ValidateEntry(logs.LogEntry{TimestampNs: tsNs, Line: v[1]}); verr != nil {
				var ve *logs.ValidationError
				if errors.As(verr, &ve) {
					validationErrors = append(validationErrors, ingestErrorItem{Index: i, Field: ve.Field, Message: ve.Message})
				} else {
					validationErrors = append(validationErrors, ingestErrorItem{Index: i, Field: "unknown", Message: verr.Error()})
				}
				continue
			}
			entries = append(entries, pending{labels: sl, tsNs: tsNs, line: v[1]})
		}
	}

	if len(validationErrors) > 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"errors": validationErrors})
		return
	}

	for _, e := range entries {
		if err := s.logIngester.Append(e.labels, e.tsNs, e.line); err != nil {
			s.log.Error("log ingester append failed", "err", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
			return
		}
	}
	w.WriteHeader(http.StatusNoContent)
}
