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

// TestHandlerComponentSurvivesAsTheOnlyComponentKey exercises the real shape
// of the finding-1 regression end to end: a request logger installed by the
// actual Logger middleware, then a handler calling
// observability.Component(observability.FromContext(ctx), "<subsystem>") on
// top of it, exactly as internal/api/ingest.go and friends do. If Logger ever
// again installed a context logger that already carried "component" (e.g. by
// being handed one via Component(log, "api"), as cmd/server/main.go used to),
// the handler's own Component call would add a second "component" key
// instead of contributing the request's only one.
func TestHandlerComponentSurvivesAsTheOnlyComponentKey(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, nil))

	handler := Logger(log)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observability.Component(observability.FromContext(r.Context()), "metrics_ingest").
			Warn("ingester append failed")
		w.WriteHeader(http.StatusInternalServerError)
	}))
	full := chimiddleware.RequestID(handler)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rr := httptest.NewRecorder()
	full.ServeHTTP(rr, req)

	rawLines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(rawLines) != 2 {
		t.Fatalf("got %d raw log lines, want 2 (handler line + access log line): %v", len(rawLines), rawLines)
	}
	handlerRaw := rawLines[0]

	// Checked on the raw JSON text, before any decode collapses duplicate
	// keys into a single map entry: decoding first would hide the exact
	// failure mode this test exists to catch.
	if n := strings.Count(handlerRaw, `"component"`); n != 1 {
		t.Errorf("handler line carries %d component keys, want exactly 1: %s", n, handlerRaw)
	}

	var handlerLine map[string]any
	if err := json.Unmarshal([]byte(handlerRaw), &handlerLine); err != nil {
		t.Fatalf("handler line is not JSON: %q: %v", handlerRaw, err)
	}
	if handlerLine["component"] != "metrics_ingest" {
		t.Errorf("component = %v, want the handler's value %q", handlerLine["component"], "metrics_ingest")
	}
}

// TestPanickingRequestStillProducesAnAccessLogLine guards against the other
// half of finding 3: without a defer, a panicking handler skips the
// access-log line entirely and the request's request_id is never logged
// anywhere. The middleware must not recover the panic itself, so this test
// recovers it locally to confirm both that the panic still propagates and
// that the access-log line was written first, with a status that reflects
// the panic rather than a false 200.
func TestPanickingRequestStillProducesAnAccessLogLine(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, nil))

	handler := Logger(log)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("boom")
	}))
	full := chimiddleware.RequestID(handler)

	req := httptest.NewRequest(http.MethodGet, "/panic", nil)
	rr := httptest.NewRecorder()

	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("handler panic did not propagate through the middleware")
			}
		}()
		full.ServeHTTP(rr, req)
	}()

	lines := decodeLoggerTestLines(t, &buf)
	if len(lines) != 1 {
		t.Fatalf("got %d access log lines after a panic, want 1: %v", len(lines), lines)
	}
	line := lines[0]
	if reqID, _ := line["request_id"].(string); reqID == "" {
		t.Errorf("access log line missing a non-empty request_id: %v", line)
	}
	if line["status"] != float64(http.StatusInternalServerError) {
		t.Errorf("status = %v, want %d for a panicking request", line["status"], http.StatusInternalServerError)
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
