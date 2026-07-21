package api

import (
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strconv"

	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/masonwheeler/observability-platform/internal/logs"
)

type lokiPushRequest struct {
	Streams []lokiStream `json:"streams"`
}

type lokiStream struct {
	// Stream values are *string (not string) so a JSON null label value is
	// rejected rather than silently coerced to "": Unmarshal("null") is a no-op
	// for a string, which would leave {"service":null} as {"service":""}.
	Stream map[string]*string `json:"stream"`
	// Values holds each entry as raw JSON elements so that a canonical Loki
	// structured-metadata entry (["<ts>", "<line>", {..}]) decodes successfully
	// and is then rejected through the explicit length check below, rather than
	// failing generic JSON decoding.
	Values [][]json.RawMessage `json:"values"` // each entry: ["<unix_nano>", "<line>"]
}

// handleLokiPush accepts a Loki-style JSON push payload. It validates every entry
// first; on any error it returns 400 with the full error list and buffers nothing.
// Otherwise it appends each accepted entry (WAL-before-buffer) and returns 204.
func (s *Server) handleLokiPush(w http.ResponseWriter, r *http.Request) {
	// Content-Type, if present, must parse to exactly application/json. Parsing the
	// media type (rather than a prefix check) rejects look-alikes such as
	// application/jsonp and protobuf bodies.
	if ct := r.Header.Get("Content-Type"); ct != "" {
		mt, _, err := mime.ParseMediaType(ct)
		if err != nil || mt != "application/json" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "unsupported content-type: only application/json is supported"})
			return
		}
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4<<20)

	dec := json.NewDecoder(r.Body)
	var req lokiPushRequest
	if err := dec.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	// Reject a body with more than one top-level JSON value (e.g. "{...}{...}"):
	// a well-formed push is exactly one object, so anything after it is malformed.
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "unexpected trailing data after JSON body"})
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
		streamLabels := make(map[string]string, len(st.Stream))
		var nullLabel bool
		for k, vp := range st.Stream {
			if vp == nil {
				validationErrors = append(validationErrors, ingestErrorItem{Index: i, Field: k, Message: "label value must be a string, not null"})
				nullLabel = true
				break
			}
			streamLabels[k] = *vp
		}
		if nullLabel {
			continue
		}
		sl, err := logs.NewStreamLabels(streamLabels)
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
			// Decode into *string (not string) so a JSON null is rejected rather
			// than silently coerced to an empty value: Unmarshal("null") is a no-op
			// for a string, but leaves a *string nil.
			var tsPtr, linePtr *string
			if err := json.Unmarshal(v[0], &tsPtr); err != nil || tsPtr == nil {
				validationErrors = append(validationErrors, ingestErrorItem{Index: i, Field: "values", Message: "timestamp must be a string"})
				continue
			}
			if err := json.Unmarshal(v[1], &linePtr); err != nil || linePtr == nil {
				validationErrors = append(validationErrors, ingestErrorItem{Index: i, Field: "values", Message: "line must be a string"})
				continue
			}
			line := *linePtr
			tsNs, perr := strconv.ParseInt(*tsPtr, 10, 64)
			if perr != nil {
				validationErrors = append(validationErrors, ingestErrorItem{Index: i, Field: "timestamp", Message: "invalid nanosecond timestamp: " + *tsPtr})
				continue
			}
			if verr := logs.ValidateEntry(logs.LogEntry{TimestampNs: tsNs, Line: line}); verr != nil {
				var ve *logs.ValidationError
				if errors.As(verr, &ve) {
					validationErrors = append(validationErrors, ingestErrorItem{Index: i, Field: ve.Field, Message: ve.Message})
				} else {
					validationErrors = append(validationErrors, ingestErrorItem{Index: i, Field: "unknown", Message: verr.Error()})
				}
				continue
			}
			entries = append(entries, pending{labels: sl, tsNs: tsNs, line: line})
		}
	}

	if len(validationErrors) > 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"errors": validationErrors})
		return
	}

	for _, e := range entries {
		if err := s.logIngester.Append(e.labels, e.tsNs, e.line); err != nil {
			s.log.Error("log ingester append failed",
				"component", "logs_push",
				"request_id", chimiddleware.GetReqID(r.Context()),
				"err", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
			return
		}
	}
	w.WriteHeader(http.StatusNoContent)
}
