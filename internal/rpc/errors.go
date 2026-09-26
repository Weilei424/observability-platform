package rpc

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// ErrUnavailable marks a peer that could not answer: a failed connection, a
// timeout, or a 5xx. The query handlers answer it with 503 "unavailable" — an
// outage, not a bad query. Test with errors.Is; the wrapped message names the
// peer, the route, and the cause.
var ErrUnavailable = errors.New("peer unavailable")

const (
	// selectBodyLimit bounds a select or maintenance request: a few matchers.
	selectBodyLimit = 1 << 20
	// FlushBodyLimit bounds a flush request. The ingester sends at most 16 MiB
	// of chunk bytes or log lines per request, which leaves ample margin even
	// after base64 and JSON.
	FlushBodyLimit = 64 << 20
)

type errorBody struct {
	Error string `json:"error"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	// Same rule as marshalJSON in codec.go: no HTML escaping. A response
	// containing a wireEntry has already assembled its line via marshalJSON, so
	// this encoder must not escape either — an outer SetEscapeHTML(true) here
	// would re-escape a nested MarshalJSON's already-literal '<'/'>'/'&' bytes
	// on the way out, which a downstream reader would then have to un-escape.
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorBody{Error: msg})
}

// readBody reads at most limit bytes of r's body. It answers 413 when the body
// is larger and 400 when it cannot be read; ok is false when it has answered.
func readBody(w http.ResponseWriter, r *http.Request, limit int64) ([]byte, bool) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("request body exceeds %d bytes", limit))
			return nil, false
		}
		writeError(w, http.StatusBadRequest, "unreadable request body: "+err.Error())
		return nil, false
	}
	return body, true
}

// decodeStrict unmarshals body into v, refusing unknown fields: both sides of
// this API ship in one binary, so an unknown field is a version mismatch, not a
// field to ignore. It answers 400 itself when it fails.
func decodeStrict(w http.ResponseWriter, body []byte, v any) bool {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request: "+err.Error())
		return false
	}
	return true
}

// errorMessage extracts {"error": ...} from an answer body, falling back to the
// first 200 bytes of whatever it is.
func errorMessage(raw []byte) string {
	var e errorBody
	if json.Unmarshal(raw, &e) == nil && e.Error != "" {
		return e.Error
	}
	s := strings.TrimSpace(string(raw))
	if len(s) > 200 {
		s = s[:200] + "…"
	}
	return s
}
