package middleware

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"log/slog"

	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/masonwheeler/observability-platform/internal/observability"
)

func decodeLoggerTestLines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line is not JSON: %q: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

// TestContextLoggerReachesHandler is the test that fails if Logger builds the
// request carrying the context logger but then serves the ORIGINAL request
// instead of the modified one: every existing test still passes and the
// access log still looks right, but the handler silently gets a context with
// no logger in it, and observability.FromContext falls back to the process
// default instead of the request-scoped logger writing to buf.
//
// chimiddleware.RequestID runs in front of Logger here, mirroring the
// router's actual middleware order, so GetReqID inside Logger sees a real ID.
func TestContextLoggerReachesHandler(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, nil))

	handler := Logger(log)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observability.FromContext(r.Context()).Info("handler line")
		w.WriteHeader(http.StatusOK)
	}))
	full := chimiddleware.RequestID(handler)

	req := httptest.NewRequest(http.MethodGet, "/widgets", nil)
	rr := httptest.NewRecorder()
	full.ServeHTTP(rr, req)

	lines := decodeLoggerTestLines(t, &buf)
	if len(lines) != 2 {
		t.Fatalf("got %d log lines, want 2 (handler line + access log line): %v", len(lines), lines)
	}
	handlerLine, accessLine := lines[0], lines[1]
	if handlerLine["msg"] != "handler line" {
		t.Fatalf("first line msg = %v, want \"handler line\": %v", handlerLine["msg"], handlerLine)
	}
	if accessLine["msg"] != "request" {
		t.Fatalf("second line msg = %v, want \"request\": %v", accessLine["msg"], accessLine)
	}

	handlerReqID, _ := handlerLine["request_id"].(string)
	if handlerReqID == "" {
		t.Fatalf("handler line missing a non-empty request_id: %v", handlerLine)
	}
	if accessLine["request_id"] != handlerReqID {
		t.Errorf("access log request_id = %v, want %v (same request_id the handler saw via FromContext)",
			accessLine["request_id"], handlerReqID)
	}
}

// TestAccessLogRequestIDAppearsExactlyOnce guards against the specific
// regression this migration could reintroduce: the access-log call site used
// to add "request_id" itself, and the request-scoped logger installed by
// Logger now carries it too. Either dropped would be wrong in a different way
// (missing entirely, or rendered twice by slog).
func TestAccessLogRequestIDAppearsExactlyOnce(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, nil))

	handler := Logger(log)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	full := chimiddleware.RequestID(handler)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rr := httptest.NewRecorder()
	full.ServeHTTP(rr, req)

	line := strings.TrimSpace(buf.String())
	if n := strings.Count(line, `"request_id"`); n != 1 {
		t.Errorf("access log line carries %d request_id keys, want exactly 1: %s", n, line)
	}
}

// TestLoggerWithoutUpstreamRequestIDDoesNotPanic covers a request that never
// passed through chimiddleware.RequestID — GetReqID then returns "" rather
// than panicking, and Logger must tolerate that (a unit test invoking a
// handler directly, or any future caller that omits the RequestID middleware,
// hits exactly this path).
func TestLoggerWithoutUpstreamRequestIDDoesNotPanic(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, nil))

	handler := Logger(log)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req) // no chimiddleware.RequestID in front: must not panic

	lines := decodeLoggerTestLines(t, &buf)
	if len(lines) != 1 {
		t.Fatalf("got %d lines, want 1", len(lines))
	}
	if lines[0]["request_id"] != "" {
		t.Errorf("request_id = %v, want \"\" (no upstream RequestID middleware)", lines[0]["request_id"])
	}
}
