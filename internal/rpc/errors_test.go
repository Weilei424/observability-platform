package rpc

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"unicode/utf8"
)

// errReader is an io.Reader that always fails with a fixed, non-size-related
// error, used to distinguish readBody's "body too large" path from its
// generic "unreadable" path.
type errReader struct{ err error }

func (r errReader) Read([]byte) (int, error) { return 0, r.err }

func TestReadBodyAcceptsExactlyTheLimit(t *testing.T) {
	body := bytes.Repeat([]byte("x"), 10)
	req := &http.Request{Body: io.NopCloser(bytes.NewReader(body))}
	rec := httptest.NewRecorder()
	got, ok := readBody(rec, req, 10)
	if !ok {
		t.Fatalf("expected ok=true at exactly the limit, got status %d, body %q", rec.Code, rec.Body.String())
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("got %q, want %q", got, body)
	}
}

func TestReadBodyRejectsOneByteOverTheLimit(t *testing.T) {
	body := bytes.Repeat([]byte("x"), 11)
	req := &http.Request{Body: io.NopCloser(bytes.NewReader(body))}
	rec := httptest.NewRecorder()
	_, ok := readBody(rec, req, 10)
	if ok {
		t.Fatal("expected ok=false one byte over the limit")
	}
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusRequestEntityTooLarge)
	}
	msg := errorMessage(rec.Body.Bytes())
	if !strings.Contains(msg, "exceeds") || !strings.Contains(msg, "10") {
		t.Fatalf("body = %q, want a message reporting the body too large against limit 10", msg)
	}
}

func TestReadBodyDistinguishesANonSizeReadError(t *testing.T) {
	boom := errors.New("boom")
	req := &http.Request{Body: io.NopCloser(errReader{err: boom})}
	rec := httptest.NewRecorder()
	_, ok := readBody(rec, req, 1024)
	if ok {
		t.Fatal("expected ok=false on a read error")
	}
	// Must be a plain 400, not the 413 the size-limit path answers — this is
	// not a body-too-large condition, so it must not be reported as one.
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d (a non-size read error must not become %d)",
			rec.Code, http.StatusBadRequest, http.StatusRequestEntityTooLarge)
	}
	msg := errorMessage(rec.Body.Bytes())
	if !strings.Contains(msg, "unreadable") || !strings.Contains(msg, "boom") {
		t.Fatalf("body = %q, want it to name the underlying read error", msg)
	}
}

// decodeTarget is the destination struct for decodeStrict's tests: one known
// field, so an extra field in the body is unambiguously "unknown".
type decodeTarget struct {
	A int `json:"a"`
}

func TestDecodeStrictRejectsUnknownField(t *testing.T) {
	rec := httptest.NewRecorder()
	var v decodeTarget
	if decodeStrict(rec, []byte(`{"a":1,"b":2}`), &v) {
		t.Fatal("expected ok=false for an unknown field")
	}
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestDecodeStrictRejectsATrailingJSONValue(t *testing.T) {
	rec := httptest.NewRecorder()
	var v decodeTarget
	if decodeStrict(rec, []byte(`{"a":1}{"a":2}`), &v) {
		t.Fatal("expected ok=false for a trailing JSON value")
	}
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestDecodeStrictRejectsTrailingGarbage(t *testing.T) {
	rec := httptest.NewRecorder()
	var v decodeTarget
	if decodeStrict(rec, []byte(`{"a":1}   trailing garbage not json`), &v) {
		t.Fatal("expected ok=false for trailing garbage")
	}
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestDecodeStrictAcceptsTrailingWhitespace(t *testing.T) {
	rec := httptest.NewRecorder()
	var v decodeTarget
	if !decodeStrict(rec, []byte("{\"a\":1}   \n"), &v) {
		t.Fatalf("expected ok=true for trailing whitespace, got status %d, body %q", rec.Code, rec.Body.String())
	}
	if v.A != 1 {
		t.Fatalf("A = %d, want 1", v.A)
	}
}

func TestErrorMessageTruncatesOnARuneBoundary(t *testing.T) {
	// A multi-byte rune ('日', 3 bytes: 0xE6 0x97 0xA5) straddles the 200-byte
	// cut: 198 ASCII bytes put its first byte at index 198 and its last at
	// index 200, so a naive byte-index slice of s[:200] keeps only the rune's
	// first two bytes — an invalid trailing partial encoding.
	prefix := strings.Repeat("a", 198)
	raw := []byte(prefix + "日" + strings.Repeat("b", 50))
	if len(raw) <= 200 {
		t.Fatalf("test fixture too short: %d bytes", len(raw))
	}
	got := errorMessage(raw)
	if !utf8.ValidString(got) {
		t.Fatalf("errorMessage produced invalid UTF-8: %q", got)
	}
	if !strings.HasPrefix(got, prefix) {
		t.Fatalf("got %q, want it to still start with the 198-byte ASCII prefix", got)
	}
}
