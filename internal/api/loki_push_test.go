package api_test

import (
	"bytes"
	"encoding/json"
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

func newPushServer(t *testing.T) (*api.Server, *logs.MemoryStore) {
	t.Helper()
	cfg := &config.Config{HTTPAddr: ":0", DataDir: t.TempDir(), LogLevel: "info"}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	mstore := metrics.NewMemoryStore()
	engine := metrics.NewQueryEngine(mstore)
	reg, _ := observability.NewRegistry(mstore, nil)
	logStore := logs.NewMemoryStore()
	return api.New(cfg, log, mstore, engine, reg, logStore), logStore
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

func TestLokiPush_ThreeElementValue_Returns400(t *testing.T) {
	srv, _ := newPushServer(t)
	body := `{"streams":[{"stream":{"service":"api"},"values":[["1700000000000000000","x",{"trace":"1"}]]}]}`
	// A 3-element value fails JSON decode into [][]string OR the len!=2 check;
	// both paths must yield 400.
	rr := postPush(t, srv, body, "application/json")
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
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
