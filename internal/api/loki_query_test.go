package api_test

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
	if resp.Data.Result[0].Values[0][1] != "GET /a error" {
		t.Fatalf("first value line = %q, want %q", resp.Data.Result[0].Values[0][1], "GET /a error")
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
	// The "api" stream carries a second, distinct label name ("level") so that
	// LabelNames() sortedness across more than one name is actually exercised.
	pushLogs(t, srv, `{"streams":[
	 {"stream":{"service":"api","level":"info"},"values":[["100","GET /a error"]]},
	 {"stream":{"service":"web"},"values":[["150","render error"]]}
	]}`)
	w := getLoki(t, srv, "/loki/api/v1/labels", url.Values{})
	var resp struct {
		Status string   `json:"status"`
		Data   []string `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	want := []string{"level", "service"}
	if resp.Status != "success" || len(resp.Data) != len(want) || resp.Data[0] != want[0] || resp.Data[1] != want[1] {
		t.Fatalf("labels = %+v, want %v", resp, want)
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
	if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("bad limit content-type = %q", ct)
	}

	// Missing query → 400.
	w = getLoki(t, srv, "/loki/api/v1/query_range", url.Values{})
	if w.Code != 400 {
		t.Fatalf("missing query code = %d", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("missing query content-type = %q", ct)
	}
}

// TestLokiQueryRange_DefaultWindow proves query_range with no start/end
// defaults to the window [now-1h, now]: an entry 30m in the past falls inside
// (proving end≈now), an entry 2h in the past falls outside (proving
// start≈now-1h).
func TestLokiQueryRange_DefaultWindow(t *testing.T) {
	srv := newLokiServer(t)
	now := time.Now()
	insideNs := now.Add(-30 * time.Minute).UnixNano()
	outsideNs := now.Add(-2 * time.Hour).UnixNano()
	body := fmt.Sprintf(`{"streams":[{"stream":{"service":"api"},"values":[["%d","inside window"],["%d","outside window"]]}]}`,
		insideNs, outsideNs)
	pushLogs(t, srv, body)

	q := url.Values{}
	q.Set("query", `{service="api"}`)
	// No start/end: exercises the [now-1h, now] default.
	w := getLoki(t, srv, "/loki/api/v1/query_range", q)
	if w.Code != 200 {
		t.Fatalf("code = %d, body = %s", w.Code, w.Body.String())
	}
	var resp lokiStreamsResp
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Data.Result) != 1 || len(resp.Data.Result[0].Values) != 1 {
		t.Fatalf("default window result = %+v", resp.Data.Result)
	}
	if resp.Data.Result[0].Values[0][1] != "inside window" {
		t.Fatalf("default window line = %q, want %q", resp.Data.Result[0].Values[0][1], "inside window")
	}
}

// TestLokiInstantQuery_DefaultTime proves the instant query with no 'time'
// defaults to now, evaluated over [0, now]: an entry 30m in the past falls
// inside, an entry 1h in the future falls outside.
func TestLokiInstantQuery_DefaultTime(t *testing.T) {
	srv := newLokiServer(t)
	now := time.Now()
	pastNs := now.Add(-30 * time.Minute).UnixNano()
	futureNs := now.Add(1 * time.Hour).UnixNano()
	body := fmt.Sprintf(`{"streams":[{"stream":{"service":"api"},"values":[["%d","past entry"],["%d","future entry"]]}]}`,
		pastNs, futureNs)
	pushLogs(t, srv, body)

	q := url.Values{}
	q.Set("query", `{service="api"}`)
	// No time: exercises the now default with a [0, now] window.
	w := getLoki(t, srv, "/loki/api/v1/query", q)
	if w.Code != 200 {
		t.Fatalf("code = %d, body = %s", w.Code, w.Body.String())
	}
	var resp lokiStreamsResp
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Data.Result) != 1 || len(resp.Data.Result[0].Values) != 1 {
		t.Fatalf("default time result = %+v", resp.Data.Result)
	}
	if resp.Data.Result[0].Values[0][1] != "past entry" {
		t.Fatalf("default time line = %q, want %q", resp.Data.Result[0].Values[0][1], "past entry")
	}
}

// TestLokiQueryRange_NilLogQuery drives a query handler against a Server built
// with a nil logs query engine (metrics-only wiring) and expects a 500, per
// requireLogQuery's guard.
func TestLokiQueryRange_NilLogQuery(t *testing.T) {
	srv := newPushServerWithIngester(t, logs.NewMemoryStore())
	q := url.Values{}
	q.Set("query", `{service="api"}`)
	w := getLoki(t, srv, "/loki/api/v1/query_range", q)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("code = %d, want 500; body = %s", w.Code, w.Body.String())
	}
}

// TestLokiQueryRange_BackwardDefaultOrder proves 'direction' defaults to
// 'backward' (newest-first) when omitted, per the Loki API contract. Every
// other test in this file sets direction=forward explicitly.
func TestLokiQueryRange_BackwardDefaultOrder(t *testing.T) {
	srv := newLokiServer(t)
	pushLogs(t, srv, `{"streams":[
	 {"stream":{"service":"api"},"values":[["100","a"],["200","b"],["300","c"]]}
	]}`)

	q := url.Values{}
	q.Set("query", `{service="api"}`)
	q.Set("start", "0")
	q.Set("end", "1000")
	// No 'direction' set: exercises the backward default.
	w := getLoki(t, srv, "/loki/api/v1/query_range", q)
	if w.Code != 200 {
		t.Fatalf("code = %d, body = %s", w.Code, w.Body.String())
	}
	var resp lokiStreamsResp
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Data.Result) != 1 || len(resp.Data.Result[0].Values) != 3 {
		t.Fatalf("result = %+v", resp.Data.Result)
	}
	vals := resp.Data.Result[0].Values
	if vals[0][0] != "300" || vals[1][0] != "200" || vals[2][0] != "100" {
		t.Fatalf("values not newest-first: %+v", vals)
	}
}

// TestLokiQueryRange_GlobalLimitAcrossStreams proves 'limit' truncates
// globally across all matched streams (newest-first), not per-stream. Two
// streams share the "env" label so a single selector matches both.
func TestLokiQueryRange_GlobalLimitAcrossStreams(t *testing.T) {
	srv := newLokiServer(t)
	pushLogs(t, srv, `{"streams":[
	 {"stream":{"env":"prod","service":"api"},"values":[["100","a1"],["300","a2"]]},
	 {"stream":{"env":"prod","service":"web"},"values":[["200","w1"],["400","w2"]]}
	]}`)

	q := url.Values{}
	q.Set("query", `{env="prod"}`)
	q.Set("start", "0")
	q.Set("end", "1000")
	q.Set("limit", "2")
	// No 'direction' set: default backward, so the global limit keeps the two
	// newest entries overall regardless of which stream they belong to.
	w := getLoki(t, srv, "/loki/api/v1/query_range", q)
	if w.Code != 200 {
		t.Fatalf("code = %d, body = %s", w.Code, w.Body.String())
	}
	var resp lokiStreamsResp
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	total := 0
	got := map[string]bool{}
	for _, rs := range resp.Data.Result {
		total += len(rs.Values)
		for _, v := range rs.Values {
			got[v[0]] = true
		}
	}
	if total != 2 || !got["400"] || !got["300"] {
		t.Fatalf("global limit result = %+v (total=%d, ts set=%+v)", resp.Data.Result, total, got)
	}
}

// TestLokiLabelEndpoints_AcceptAndIgnoreTimeParams proves the label endpoints
// tolerate start/end/query params rather than rejecting them: Grafana always
// sends these on its datasource health check, and erroring on them would
// break it.
func TestLokiLabelEndpoints_AcceptAndIgnoreTimeParams(t *testing.T) {
	srv := newLokiServer(t)
	pushLogs(t, srv, `{"streams":[{"stream":{"service":"api"},"values":[["100","a"]]}]}`)

	type lokiStatusResp struct {
		Status string `json:"status"`
	}

	q := url.Values{}
	q.Set("start", "0")
	q.Set("end", "1000")
	q.Set("query", `{service="api"}`)
	w := getLoki(t, srv, "/loki/api/v1/labels", q)
	if w.Code != 200 {
		t.Fatalf("labels code = %d, body = %s", w.Code, w.Body.String())
	}
	var lresp lokiStatusResp
	if err := json.Unmarshal(w.Body.Bytes(), &lresp); err != nil {
		t.Fatalf("unmarshal labels: %v", err)
	}
	if lresp.Status != "success" {
		t.Fatalf("labels status = %q, want success", lresp.Status)
	}

	q2 := url.Values{}
	q2.Set("start", "0")
	q2.Set("end", "1000")
	w = getLoki(t, srv, "/loki/api/v1/label/service/values", q2)
	if w.Code != 200 {
		t.Fatalf("label values code = %d, body = %s", w.Code, w.Body.String())
	}
	var vresp lokiStatusResp
	if err := json.Unmarshal(w.Body.Bytes(), &vresp); err != nil {
		t.Fatalf("unmarshal label values: %v", err)
	}
	if vresp.Status != "success" {
		t.Fatalf("label values status = %q, want success", vresp.Status)
	}
}
