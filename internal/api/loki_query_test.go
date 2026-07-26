package api_test

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/api"
	"github.com/masonwheeler/observability-platform/internal/config"
	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/observability"
)

func newLokiServer(t *testing.T) *api.Server {
	t.Helper()
	dir := t.TempDir()
	store, err := logs.NewStore(
		filepath.Join(dir, "wal"),
		filepath.Join(dir, "chunks"),
		filepath.Join(dir, "index"),
		1<<20, 1, 8<<20,
	)
	if err != nil {
		t.Fatalf("new logs store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	cfg := &config.Config{HTTPAddr: ":0", DataDir: dir, LogLevel: "info"}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	mstore := metrics.NewMemoryStore()
	engine := metrics.NewQueryEngine(mstore)
	reg, _ := observability.NewRegistry(mstore, nil)
	return api.New(cfg, log, mstore, engine, reg, store, logs.NewQueryEngine(store))
}

func pushLogs(t *testing.T, srv *api.Server, body string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/loki/api/v1/push", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("push code = %d, body = %s", w.Code, w.Body.String())
	}
}

func getLoki(t *testing.T, srv *api.Server, path string, q url.Values) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path+"?"+q.Encode(), nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	return w
}

type lokiStreamsResp struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Stream map[string]string `json:"stream"`
			Values [][2]string       `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

const twoStreams = `{"streams":[
 {"stream":{"service":"api"},"values":[["100","GET /a error"],["200","GET /b ok"],["300","GET /c error"]]},
 {"stream":{"service":"web"},"values":[["150","render error"]]}
]}`

func TestLokiQueryRange_LabelOnly(t *testing.T) {
	srv := newLokiServer(t)
	pushLogs(t, srv, twoStreams)

	q := url.Values{}
	q.Set("query", `{service="api"}`)
	q.Set("start", "0")
	q.Set("end", "1000")
	q.Set("direction", "forward")
	w := getLoki(t, srv, "/loki/api/v1/query_range", q)
	if w.Code != 200 {
		t.Fatalf("code = %d, body = %s", w.Code, w.Body.String())
	}
	var resp lokiStreamsResp
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Data.ResultType != "streams" || len(resp.Data.Result) != 1 {
		t.Fatalf("result = %+v", resp.Data)
	}
	if resp.Data.Result[0].Stream["service"] != "api" || len(resp.Data.Result[0].Values) != 3 {
		t.Fatalf("api stream = %+v", resp.Data.Result[0])
	}
	if resp.Data.Result[0].Values[0][0] != "100" {
		t.Fatalf("first value ts = %q, want 100", resp.Data.Result[0].Values[0][0])
	}
}

func TestLokiQueryRange_TimeRangeNarrows(t *testing.T) {
	srv := newLokiServer(t)
	pushLogs(t, srv, twoStreams)
	q := url.Values{}
	q.Set("query", `{service="api"}`)
	q.Set("start", "0")
	q.Set("end", "250")
	q.Set("direction", "forward")
	w := getLoki(t, srv, "/loki/api/v1/query_range", q)
	var resp lokiStreamsResp
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if len(resp.Data.Result) != 1 || len(resp.Data.Result[0].Values) != 2 {
		t.Fatalf("expected 2 values (ts<=250), got %+v", resp.Data.Result)
	}
}

func TestLokiQueryRange_TextFilters(t *testing.T) {
	srv := newLokiServer(t)
	pushLogs(t, srv, twoStreams)

	// |= "error"
	q := url.Values{}
	q.Set("query", `{service="api"} |= "error"`)
	q.Set("start", "0")
	q.Set("end", "1000")
	q.Set("direction", "forward")
	w := getLoki(t, srv, "/loki/api/v1/query_range", q)
	var resp lokiStreamsResp
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if len(resp.Data.Result) != 1 || len(resp.Data.Result[0].Values) != 2 {
		t.Fatalf(`|= "error" expected 2, got %+v`, resp.Data.Result)
	}

	// |~ regex
	q.Set("query", `{service="api"} |~ "/[ac] error"`)
	w = getLoki(t, srv, "/loki/api/v1/query_range", q)
	resp = lokiStreamsResp{}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if len(resp.Data.Result) != 1 || len(resp.Data.Result[0].Values) != 2 {
		t.Fatalf("regex filter expected 2, got %+v", resp.Data.Result)
	}
}

func TestLokiInstantQuery_UpToTime(t *testing.T) {
	srv := newLokiServer(t)
	pushLogs(t, srv, twoStreams)
	q := url.Values{}
	q.Set("query", `{service="api"}`)
	q.Set("time", "250")
	w := getLoki(t, srv, "/loki/api/v1/query", q)
	if w.Code != 200 {
		t.Fatalf("code = %d, body = %s", w.Code, w.Body.String())
	}
	var resp lokiStreamsResp
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if len(resp.Data.Result) != 1 || len(resp.Data.Result[0].Values) != 2 {
		t.Fatalf("instant ts<=250 expected 2, got %+v", resp.Data.Result)
	}
}

func TestLokiLabels(t *testing.T) {
	srv := newLokiServer(t)
	pushLogs(t, srv, twoStreams)
	w := getLoki(t, srv, "/loki/api/v1/labels", url.Values{})
	var resp struct {
		Status string   `json:"status"`
		Data   []string `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Status != "success" || len(resp.Data) != 1 || resp.Data[0] != "service" {
		t.Fatalf("labels = %+v", resp)
	}
}

func TestLokiLabelValues(t *testing.T) {
	srv := newLokiServer(t)
	pushLogs(t, srv, twoStreams)
	w := getLoki(t, srv, "/loki/api/v1/label/service/values", url.Values{})
	var resp struct {
		Status string   `json:"status"`
		Data   []string `json:"data"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if len(resp.Data) != 2 || resp.Data[0] != "api" || resp.Data[1] != "web" {
		t.Fatalf("values = %+v", resp)
	}
}

func TestLokiQueryRange_UnsupportedAndBadParams(t *testing.T) {
	srv := newLokiServer(t)
	pushLogs(t, srv, twoStreams)

	// Unsupported metric query → 400 plain text.
	q := url.Values{}
	q.Set("query", `rate({service="api"}[5m])`)
	q.Set("start", "0")
	q.Set("end", "1000")
	w := getLoki(t, srv, "/loki/api/v1/query_range", q)
	if w.Code != 400 {
		t.Fatalf("unsupported code = %d", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("error content-type = %q", ct)
	}

	// Bad limit → 400.
	q = url.Values{}
	q.Set("query", `{service="api"}`)
	q.Set("start", "0")
	q.Set("end", "1000")
	q.Set("limit", "abc")
	w = getLoki(t, srv, "/loki/api/v1/query_range", q)
	if w.Code != 400 {
		t.Fatalf("bad limit code = %d", w.Code)
	}

	// Missing query → 400.
	w = getLoki(t, srv, "/loki/api/v1/query_range", url.Values{})
	if w.Code != 400 {
		t.Fatalf("missing query code = %d", w.Code)
	}
}
