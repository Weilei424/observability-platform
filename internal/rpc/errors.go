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

// decodeStrict unmarshals body into v, refusing unknown fields and anything
// after the JSON value: both sides of this API ship in one binary, so an
// unknown field or trailing data is a version mismatch, not something to
// ignore. It answers 400 itself when it fails.
func decodeStrict(w http.ResponseWriter, body []byte, v any) bool {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request: "+err.Error())
		return false
	}
	// dec.Decode reads exactly one JSON value off the stream and stops; on its
	// own it would silently discard a second value or trailing garbage after
	// it, contradicting "Strict". A second Decode must land on exactly io.EOF
	// for the body to be considered fully consumed: trailing whitespace or a
	// newline reads as EOF, but a second value or any other trailing byte does
	// not. This is deliberately not dec.More(): More peeks for a '}' or ']' as
	// its "nothing more" signal, which is right inside an enclosing array or
	// object but means a body like `{"a":1}}` — a stray extra '}' with nothing
	// else after it — is not caught; a second Decode is caught by a
	// SyntaxError instead, since '}' cannot start a JSON value on its own.
	var extra json.RawMessage
	if err := dec.Decode(&extra); err != io.EOF {
		writeError(w, http.StatusBadRequest, "invalid request: trailing data after JSON value")
		return false
	}
	return true
}

// errorMessage extracts {"error": ...} from an answer body, falling back to the
// first 200 bytes of whatever it is. The fallback cuts by byte count, which can
// land inside a multi-byte rune; ToValidUTF8 drops that dangling partial rune
// instead of handing back a string that isn't valid UTF-8.
func errorMessage(raw []byte) string {
	var e errorBody
	if json.Unmarshal(raw, &e) == nil && e.Error != "" {
		return e.Error
	}
	s := strings.TrimSpace(string(raw))
	if len(s) > 200 {
		s = strings.ToValidUTF8(s[:200], "") + "…"
	}
	return s
}
