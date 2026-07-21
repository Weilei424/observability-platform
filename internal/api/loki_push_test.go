package api_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/api"
	"github.com/masonwheeler/observability-platform/internal/config"
	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/observability"
)

// failingIngester is a logs.Ingester whose Append always fails, used to exercise
// the handler's 500 path.
type failingIngester struct{}

func (f *failingIngester) Append(labels logs.StreamLabels, tsNs int64, line string) error {
	return errors.New("append failed")
}

func mustStreamID(t *testing.T, m map[string]string) logs.StreamID {
	t.Helper()
	sl, err := logs.NewStreamLabels(m)
	if err != nil {
		t.Fatalf("NewStreamLabels(%v): %v", m, err)
	}
	return logs.StreamIDOf(sl)
}

func newPushServer(t *testing.T) (*api.Server, *logs.MemoryStore) {
	t.Helper()
	logStore := logs.NewMemoryStore()
	return newPushServerWithIngester(t, logStore), logStore
}

func newPushServerWithIngester(t *testing.T, ing logs.Ingester) *api.Server {
	t.Helper()
	cfg := &config.Config{HTTPAddr: ":0", DataDir: t.TempDir(), LogLevel: "info"}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	mstore := metrics.NewMemoryStore()
	engine := metrics.NewQueryEngine(mstore)
	reg, _ := observability.NewRegistry(mstore, nil)
	return api.New(cfg, log, mstore, engine, reg, ing)
}

func postPush(t *testing.T, srv *api.Server, body string, contentType string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/loki/api/v1/push", bytes.NewReader([]byte(body)))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	return rr
}

func TestLokiPush_Valid_Returns204(t *testing.T) {
	srv, store := newPushServer(t)
	body := `{"streams":[{"stream":{"service":"api"},"values":[["1700000000000000000","hello"],["1700000000000000001","world"]]}]}`
	rr := postPush(t, srv, body, "application/json")
	if rr.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body=%s", rr.Code, rr.Body.String())
	}
	sl, _ := logs.NewStreamLabels(map[string]string{"service": "api"})
	if got := len(store.StreamEntries(logs.StreamIDOf(sl))); got != 2 {
		t.Errorf("buffered entries = %d, want 2", got)
	}
}

func TestLokiPush_EmptyStreams_Returns400(t *testing.T) {
	srv, _ := newPushServer(t)
	rr := postPush(t, srv, `{"streams":[]}`, "application/json")
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestLokiPush_MalformedJSON_Returns400(t *testing.T) {
	srv, _ := newPushServer(t)
	rr := postPush(t, srv, `{not json`, "application/json")
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestLokiPush_EmptyLabels_Returns400(t *testing.T) {
	srv, _ := newPushServer(t)
	body := `{"streams":[{"stream":{},"values":[["1700000000000000000","x"]]}]}`
	rr := postPush(t, srv, body, "application/json")
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestLokiPush_BadTimestamp_Returns400(t *testing.T) {
	srv, _ := newPushServer(t)
	for _, ts := range []string{"notanumber", "0", "-5"} {
		body := `{"streams":[{"stream":{"service":"api"},"values":[["` + ts + `","x"]]}]}`
		rr := postPush(t, srv, body, "application/json")
		if rr.Code != http.StatusBadRequest {
			t.Errorf("ts=%q: status = %d, want 400", ts, rr.Code)
		}
	}
}

func TestLokiPush_OversizeLine_Returns400(t *testing.T) {
	srv, _ := newPushServer(t)
	big := strings.Repeat("a", logs.MaxLineBytes+1)
	body := `{"streams":[{"stream":{"service":"api"},"values":[["1700000000000000000","` + big + `"]]}]}`
	rr := postPush(t, srv, body, "application/json")
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestLokiPush_StructuredMetadata_RejectedExplicitly(t *testing.T) {
	srv, store := newPushServer(t)
	// Canonical Loki structured metadata: a 3rd object element. This must be
	// rejected through the explicit "values" error, not a generic JSON decode
	// failure, and nothing may be buffered.
	body := `{"streams":[{"stream":{"service":"api"},"values":[["1700000000000000000","x",{"trace":"1"}]]}]}`
	rr := postPush(t, srv, body, "application/json")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
	var resp struct {
		Errors []struct{ Field, Message string } `json:"errors"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if len(resp.Errors) == 0 || resp.Errors[0].Field != "values" {
		t.Errorf("expected explicit values error, got %+v", resp.Errors)
	}
	if !strings.Contains(resp.Errors[0].Message, "structured metadata") {
		t.Errorf("message %q should mention structured metadata", resp.Errors[0].Message)
	}
	if store.StreamCount() != 0 {
		t.Errorf("StreamCount = %d, want 0 (nothing buffered on rejection)", store.StreamCount())
	}
}

func TestLokiPush_NullLine_Returns400NoLeak(t *testing.T) {
	srv, store := newPushServer(t)
	// JSON null for the line must be rejected, not silently coerced to an empty
	// line and persisted.
	body := `{"streams":[{"stream":{"service":"api"},"values":[["1700000000000000000",null]]}]}`
	rr := postPush(t, srv, body, "application/json")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for null line", rr.Code)
	}
	if store.StreamCount() != 0 {
		t.Errorf("StreamCount = %d, want 0 (null line must not be buffered)", store.StreamCount())
	}
}

func TestLokiPush_NullLabelValue_Returns400NoLeak(t *testing.T) {
	srv, store := newPushServer(t)
	// A JSON null label value must be rejected, not silently coerced to "" and
	// persisted as an empty-valued label.
	body := `{"streams":[{"stream":{"service":null},"values":[["1700000000000000000","x"]]}]}`
	rr := postPush(t, srv, body, "application/json")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for null label value; body=%s", rr.Code, rr.Body.String())
	}
	var resp struct {
		Errors []struct{ Field, Message string } `json:"errors"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if len(resp.Errors) == 0 || resp.Errors[0].Field != "service" {
		t.Errorf("expected explicit error on the \"service\" label, got %+v", resp.Errors)
	}
	if store.StreamCount() != 0 {
		t.Errorf("StreamCount = %d, want 0 (null label value must not be buffered)", store.StreamCount())
	}
}

func TestLokiPush_NullTimestamp_Returns400(t *testing.T) {
	srv, _ := newPushServer(t)
	body := `{"streams":[{"stream":{"service":"api"},"values":[[null,"x"]]}]}`
	rr := postPush(t, srv, body, "application/json")
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 for null timestamp", rr.Code)
	}
}

func TestLokiPush_MultiStream_Returns204(t *testing.T) {
	srv, store := newPushServer(t)
	body := `{"streams":[` +
		`{"stream":{"service":"api"},"values":[["1700000000000000000","a"]]},` +
		`{"stream":{"service":"web"},"values":[["1700000000000000001","b"],["1700000000000000002","c"]]}` +
		`]}`
	rr := postPush(t, srv, body, "application/json")
	if rr.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body=%s", rr.Code, rr.Body.String())
	}
	if store.StreamCount() != 2 {
		t.Errorf("StreamCount = %d, want 2", store.StreamCount())
	}
	api := mustStreamID(t, map[string]string{"service": "api"})
	web := mustStreamID(t, map[string]string{"service": "web"})
	if got := len(store.StreamEntries(api)); got != 1 {
		t.Errorf("api stream entries = %d, want 1", got)
	}
	if got := len(store.StreamEntries(web)); got != 2 {
		t.Errorf("web stream entries = %d, want 2", got)
	}
}

func TestLokiPush_MixedBatchInvalidStream_NoLeak(t *testing.T) {
	srv, store := newPushServer(t)
	// One invalid stream (empty labels) + one otherwise-valid stream in the same
	// push. Validate-all-first must reject the whole batch and buffer NOTHING.
	body := `{"streams":[` +
		`{"stream":{},"values":[["1700000000000000000","x"]]},` +
		`{"stream":{"service":"api"},"values":[["1700000000000000001","y"]]}` +
		`]}`
	rr := postPush(t, srv, body, "application/json")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
	if store.StreamCount() != 0 {
		t.Errorf("StreamCount = %d, want 0 (valid stream in a rejected batch must not leak)", store.StreamCount())
	}
}

func TestLokiPush_AppendFailure_Returns500(t *testing.T) {
	srv := newPushServerWithIngester(t, &failingIngester{})
	body := `{"streams":[{"stream":{"service":"api"},"values":[["1700000000000000000","x"]]}]}`
	rr := postPush(t, srv, body, "application/json")
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rr.Code)
	}
}

func TestLokiPush_TrailingData_Returns400(t *testing.T) {
	srv, _ := newPushServer(t)
	// A second top-level JSON object after a valid push.
	body := `{"streams":[{"stream":{"service":"api"},"values":[["1700000000000000000","x"]]}]}{"extra":1}`
	rr := postPush(t, srv, body, "application/json")
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 for trailing data", rr.Code)
	}
}

func TestLokiPush_JsonpContentType_Returns400(t *testing.T) {
	srv, _ := newPushServer(t)
	body := `{"streams":[{"stream":{"service":"api"},"values":[["1700000000000000000","x"]]}]}`
	rr := postPush(t, srv, body, "application/jsonp")
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 for application/jsonp", rr.Code)
	}
}

func TestLokiPush_JsonWithCharset_Returns204(t *testing.T) {
	srv, _ := newPushServer(t)
	body := `{"streams":[{"stream":{"service":"api"},"values":[["1700000000000000000","x"]]}]}`
	rr := postPush(t, srv, body, "application/json; charset=utf-8")
	if rr.Code != http.StatusNoContent {
		t.Errorf("status = %d, want 204 for application/json; charset=utf-8", rr.Code)
	}
}

func TestLokiPush_OversizeBody_Returns400(t *testing.T) {
	srv, _ := newPushServer(t)
	// A body larger than the 4 MiB MaxBytesReader cap must be rejected at read time.
	big := strings.Repeat("a", 5<<20)
	body := `{"streams":[{"stream":{"service":"api"},"values":[["1700000000000000000","` + big + `"]]}]}`
	rr := postPush(t, srv, body, "application/json")
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 for oversize body", rr.Code)
	}
}

func TestLokiPush_ProtobufContentType_Returns400(t *testing.T) {
	srv, _ := newPushServer(t)
	rr := postPush(t, srv, `ignored`, "application/x-protobuf")
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
	var body map[string]any
	_ = json.NewDecoder(rr.Body).Decode(&body)
	if body["error"] == nil {
		t.Error("expected an error message for unsupported content-type")
	}
}
