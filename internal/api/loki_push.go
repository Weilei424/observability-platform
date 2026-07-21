package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strconv"
	"unicode/utf8"

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

	// Read the whole body so its UTF-8 validity can be checked BEFORE JSON
	// decoding. encoding/json silently replaces malformed UTF-8 in strings with
	// U+FFFD, which would let raw invalid bytes through label validation and alter
	// stream identity. Validating the raw bytes first upholds the "invalid input
	// returns 400; never silently misparse" contract. (Legitimate U+FFFD arriving
	// as a � escape is unaffected — only raw invalid bytes are rejected.)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		// Exceeding the MaxBytesReader cap surfaces here as a read error.
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "request body too large or unreadable"})
		return
	}
	if !utf8.Valid(body) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "request body is not valid UTF-8"})
		return
	}
	// utf8.Valid only covers raw bytes; a JSON "\uD800" escape is pure ASCII yet
	// encodes an unpaired surrogate that encoding/json would silently replace with
	// U+FFFD, again altering stream identity. Reject unpaired surrogate escapes up
	// front (valid pairs and a legitimate "�" escape are left untouched).
	if hasUnpairedSurrogateEscape(body) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "request body contains an unpaired unicode surrogate escape"})
		return
	}

	dec := json.NewDecoder(bytes.NewReader(body))
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

// hasUnpairedSurrogateEscape reports whether the JSON body contains a "\uXXXX"
// escape encoding an unpaired UTF-16 surrogate — a high surrogate not immediately
// followed by a low-surrogate escape, or a low surrogate with no preceding high.
// encoding/json would silently turn such an escape into U+FFFD; rejecting it here
// preserves exact string (and thus stream) identity. Only surrogate pairing is
// checked — other malformations are left for the JSON decoder to reject. Escapes
// are only inspected inside string literals, and "\\" is consumed as a literal
// backslash so a payload like "\\uD800" is not misread as an escape.
func hasUnpairedSurrogateEscape(b []byte) bool {
	inString := false
	for i := 0; i < len(b); {
		c := b[i]
		if !inString {
			if c == '"' {
				inString = true
			}
			i++
			continue
		}
		if c == '"' {
			inString = false
			i++
			continue
		}
		if c != '\\' {
			i++
			continue
		}
		// Escape sequence: consume the backslash and its payload.
		if i+1 >= len(b) {
			return false // truncated; the JSON decoder will reject it
		}
		if b[i+1] != 'u' {
			i += 2 // \", \\, \n, ... — two bytes, keeps \\ from starting an escape
			continue
		}
		hi, ok := parseHex4(b, i+2)
		if !ok {
			return false // malformed \u; decoder will reject
		}
		if hi >= 0xDC00 && hi <= 0xDFFF {
			return true // low surrogate with no preceding high
		}
		if hi >= 0xD800 && hi <= 0xDBFF {
			// High surrogate: the very next thing must be a low-surrogate escape.
			if i+12 > len(b) || b[i+6] != '\\' || b[i+7] != 'u' {
				return true
			}
			lo, ok := parseHex4(b, i+8)
			if !ok || lo < 0xDC00 || lo > 0xDFFF {
				return true
			}
			i += 12
			continue
		}
		i += 6 // ordinary BMP escape
	}
	return false
}

// parseHex4 parses exactly four hex digits of b starting at off into a rune,
// reporting false if fewer than four remain or any digit is non-hex.
func parseHex4(b []byte, off int) (rune, bool) {
	if off+4 > len(b) {
		return 0, false
	}
	var v rune
	for j := 0; j < 4; j++ {
		d := b[off+j]
		switch {
		case d >= '0' && d <= '9':
			v = v<<4 | rune(d-'0')
		case d >= 'a' && d <= 'f':
			v = v<<4 | rune(d-'a'+10)
		case d >= 'A' && d <= 'F':
			v = v<<4 | rune(d-'A'+10)
		default:
			return 0, false
		}
	}
	return v, true
}
