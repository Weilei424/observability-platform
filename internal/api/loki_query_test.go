package api_test

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
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
	srv, _, _ := newLokiServerAt(t, t.TempDir())
	return srv
}

// newLokiServerAt builds a server over the log store rooted at dir, returning the
// store and a close function so tests can flush the head to chunk files and
// reopen the same directory to exercise the persisted read path. close is
// idempotent, so an explicit call plus the registered cleanup is safe.
func newLokiServerAt(t *testing.T, dir string) (*api.Server, *logs.Store, func()) {
	t.Helper()
	store, err := logs.NewStore(
		filepath.Join(dir, "wal"),
		filepath.Join(dir, "chunks"),
		filepath.Join(dir, "index"),
		1<<20, 1, 8<<20,
	)
	if err != nil {
		t.Fatalf("new logs store: %v", err)
	}
	var once sync.Once
	closeStore := func() { once.Do(func() { _ = store.Close() }) }
	t.Cleanup(closeStore)
	cfg := &config.Config{HTTPAddr: ":0", DataDir: dir, LogLevel: "info"}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	mstore := metrics.NewMemoryStore()
	engine := metrics.NewQueryEngine(mstore)
	reg, _ := observability.NewRegistry(mstore, nil)
	return api.New(cfg, log, mstore, engine, reg, store, logs.NewQueryEngine(store)), store, closeStore
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

// Loki reads an integer timestamp of 10 digits or fewer as *seconds* and only a
// longer one as nanoseconds (pkg/loghttp/params.go), so fixtures use realistic
// 19-digit nanosecond epochs. ns() keeps the readable small offsets while putting
// a correctly-scaled value on the wire.
const tsBase int64 = 1_700_000_000_000_000_000 // 2023-11-14T22:13:20Z

func ns(offset int64) string { return strconv.FormatInt(tsBase+offset, 10) }

var twoStreams = fmt.Sprintf(`{"streams":[
 {"stream":{"service":"api"},"values":[[%q,"GET /a error"],[%q,"GET /b ok"],[%q,"GET /c error"]]},
 {"stream":{"service":"web"},"values":[[%q,"render error"]]}
]}`, ns(100), ns(200), ns(300), ns(150))

func TestLokiQueryRange_LabelOnly(t *testing.T) {
	srv := newLokiServer(t)
	pushLogs(t, srv, twoStreams)

	q := url.Values{}
	q.Set("query", `{service="api"}`)
	q.Set("start", ns(0))
	q.Set("end", ns(1000))
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
	if resp.Data.Result[0].Values[0][0] != ns(100) {
		t.Fatalf("first value ts = %q, want %s", resp.Data.Result[0].Values[0][0], ns(100))
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
	q.Set("start", ns(0))
	q.Set("end", ns(250))
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
	q.Set("start", ns(0))
	q.Set("end", ns(1000))
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
	q.Set("time", ns(250))
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
	pushLogs(t, srv, fmt.Sprintf(`{"streams":[
	 {"stream":{"service":"api","level":"info"},"values":[[%q,"GET /a error"]]},
	 {"stream":{"service":"web"},"values":[[%q,"render error"]]}
	]}`, ns(100), ns(150)))
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
	q.Set("start", ns(0))
	q.Set("end", ns(1000))
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
	q.Set("start", ns(0))
	q.Set("end", ns(1000))
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
	pushLogs(t, srv, fmt.Sprintf(`{"streams":[
	 {"stream":{"service":"api"},"values":[[%q,"a"],[%q,"b"],[%q,"c"]]}
	]}`, ns(100), ns(200), ns(300)))

	q := url.Values{}
	q.Set("query", `{service="api"}`)
	q.Set("start", ns(0))
	q.Set("end", ns(1000))
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
	if vals[0][0] != ns(300) || vals[1][0] != ns(200) || vals[2][0] != ns(100) {
		t.Fatalf("values not newest-first: %+v", vals)
	}
}

// TestLokiQueryRange_GlobalLimitAcrossStreams proves 'limit' truncates
// globally across all matched streams (newest-first), not per-stream. Two
// streams share the "env" label so a single selector matches both.
func TestLokiQueryRange_GlobalLimitAcrossStreams(t *testing.T) {
	srv := newLokiServer(t)
	pushLogs(t, srv, fmt.Sprintf(`{"streams":[
	 {"stream":{"env":"prod","service":"api"},"values":[[%q,"a1"],[%q,"a2"]]},
	 {"stream":{"env":"prod","service":"web"},"values":[[%q,"w1"],[%q,"w2"]]}
	]}`, ns(100), ns(300), ns(200), ns(400)))

	q := url.Values{}
	q.Set("query", `{env="prod"}`)
	q.Set("start", ns(0))
	q.Set("end", ns(1000))
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
	if total != 2 || !got[ns(400)] || !got[ns(300)] {
		t.Fatalf("global limit result = %+v (total=%d, ts set=%+v)", resp.Data.Result, total, got)
	}
}

// TestLokiLabelEndpoints_AcceptAndIgnoreTimeParams proves the label endpoints
// tolerate start/end/query params rather than rejecting them: Grafana sends
// these whenever it browses labels for Explore's selector UI and autocomplete,
// and erroring on them would break that. (The datasource *health* check is a
// different request entirely — an instant vector(1)+vector(1) query; see
// TestLokiInstantQuery_GrafanaHealthCheck.)
func TestLokiLabelEndpoints_AcceptAndIgnoreTimeParams(t *testing.T) {
	srv := newLokiServer(t)
	pushLogs(t, srv, fmt.Sprintf(`{"streams":[{"stream":{"service":"api"},"values":[[%q,"a"]]}]}`, ns(100)))

	type lokiStatusResp struct {
		Status string `json:"status"`
	}

	q := url.Values{}
	q.Set("start", ns(0))
	q.Set("end", ns(1000))
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

// TestLokiQueryRange_PersistedChunks_TextFilter exercises the path the Phase 4.4
// DoD actually names — "text filtering works inside candidate chunks". The head
// is flushed to chunk files and the store reopened, so the query has to decode
// chunks off disk rather than read the in-memory head.
func TestLokiQueryRange_PersistedChunks_TextFilter(t *testing.T) {
	dir := t.TempDir()
	srv, store, closeStore := newLokiServerAt(t, dir)
	pushLogs(t, srv, twoStreams)
	if err := store.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	closeStore() // simulate a restart: nothing survives in memory

	chunks, err := os.ReadDir(filepath.Join(dir, "chunks"))
	if err != nil {
		t.Fatalf("read chunks dir: %v", err)
	}
	if len(chunks) == 0 {
		t.Fatal("no chunk files written; the test would pass on head reads alone")
	}

	srv2, _, _ := newLokiServerAt(t, dir)

	q := url.Values{}
	q.Set("query", `{service="api"} |= "error"`)
	q.Set("start", ns(0))
	q.Set("end", ns(1000))
	q.Set("direction", "forward")
	w := getLoki(t, srv2, "/loki/api/v1/query_range", q)
	if w.Code != 200 {
		t.Fatalf("code = %d, body = %s", w.Code, w.Body.String())
	}
	var resp lokiStreamsResp
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Data.Result) != 1 || len(resp.Data.Result[0].Values) != 2 {
		t.Fatalf("filtered chunk read = %+v, want 2 matching lines", resp.Data.Result)
	}
	if resp.Data.Result[0].Values[0][1] != "GET /a error" || resp.Data.Result[0].Values[1][1] != "GET /c error" {
		t.Fatalf("chunk-read lines = %+v", resp.Data.Result[0].Values)
	}

	// A regex filter over the same persisted chunks.
	q.Set("query", `{service="api"} |~ "/[ac] error"`)
	w = getLoki(t, srv2, "/loki/api/v1/query_range", q)
	resp = lokiStreamsResp{}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if len(resp.Data.Result) != 1 || len(resp.Data.Result[0].Values) != 2 {
		t.Fatalf("regex over chunks = %+v, want 2", resp.Data.Result)
	}

	// Label discovery survives the restart too — it is served from the manifest.
	w = getLoki(t, srv2, "/loki/api/v1/labels", url.Values{})
	var labelsResp struct {
		Data []string `json:"data"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &labelsResp)
	if len(labelsResp.Data) != 1 || labelsResp.Data[0] != "service" {
		t.Fatalf("labels after restart = %+v", labelsResp.Data)
	}

	// New writes land in the head and must merge with the persisted chunks.
	pushLogs(t, srv2, fmt.Sprintf(`{"streams":[{"stream":{"service":"api"},"values":[[%q,"GET /d error"]]}]}`, ns(400)))
	q.Set("query", `{service="api"} |= "error"`)
	w = getLoki(t, srv2, "/loki/api/v1/query_range", q)
	resp = lokiStreamsResp{}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if len(resp.Data.Result) != 1 || len(resp.Data.Result[0].Values) != 3 {
		t.Fatalf("head+chunk merge = %+v, want 3", resp.Data.Result)
	}
}

// TestLokiQueryRange_EndIsExclusive pins Loki's range semantics over HTTP:
// "Loki returns results with timestamp greater or equal to [start]" and "lower
// than [end]".
func TestLokiQueryRange_EndIsExclusive(t *testing.T) {
	srv := newLokiServer(t)
	pushLogs(t, srv, twoStreams) // service=api at ts 100, 200, 300

	q := url.Values{}
	q.Set("query", `{service="api"}`)
	q.Set("start", ns(100))
	q.Set("end", ns(300))
	q.Set("direction", "forward")
	w := getLoki(t, srv, "/loki/api/v1/query_range", q)
	var resp lokiStreamsResp
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Data.Result) != 1 || len(resp.Data.Result[0].Values) != 2 {
		t.Fatalf("[100,300) = %+v, want 2 values", resp.Data.Result)
	}
	if resp.Data.Result[0].Values[0][0] != ns(100) || resp.Data.Result[0].Values[1][0] != ns(200) {
		t.Fatalf("values = %+v, want ts %s and %s", resp.Data.Result[0].Values, ns(100), ns(200))
	}

	// The boundary entry belongs to the next window, exactly once.
	q.Set("start", ns(300))
	q.Set("end", ns(400))
	w = getLoki(t, srv, "/loki/api/v1/query_range", q)
	resp = lokiStreamsResp{}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if len(resp.Data.Result) != 1 || len(resp.Data.Result[0].Values) != 1 || resp.Data.Result[0].Values[0][0] != ns(300) {
		t.Fatalf("[300,400) = %+v, want exactly ts 300", resp.Data.Result)
	}
}

type lokiVectorResp struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			Value  [2]any            `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

// TestLokiInstantQuery_GrafanaHealthCheck reproduces the request Grafana's Loki
// datasource sends to verify a connection: Grafana 11.1.0
// (pkg/tsdb/loki/healthcheck.go) runs `vector(1)+vector(1)` as an instant query
// over [Unix 1, Unix 4] and requires one frame, two fields, one row, and the
// value 2. A parse error here is what turns the datasource "Save & test" red.
func TestLokiInstantQuery_GrafanaHealthCheck(t *testing.T) {
	srv := newLokiServer(t)

	q := url.Values{}
	q.Set("query", "vector(1)+vector(1)")
	q.Set("direction", "backward")
	q.Set("time", strconv.FormatInt(time.Unix(4, 0).UnixNano(), 10))
	w := getLoki(t, srv, "/loki/api/v1/query", q)
	if w.Code != 200 {
		t.Fatalf("health check code = %d, body = %s", w.Code, w.Body.String())
	}
	var resp lokiVectorResp
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Status != "success" || resp.Data.ResultType != "vector" {
		t.Fatalf("envelope = %+v, want success/vector", resp)
	}
	if len(resp.Data.Result) != 1 {
		t.Fatalf("result = %+v, want exactly one sample (Grafana requires one frame with one row)", resp.Data.Result)
	}
	sample := resp.Data.Result[0]
	if len(sample.Metric) != 0 {
		t.Fatalf("metric = %+v, want no labels", sample.Metric)
	}
	// Grafana sends End.UnixNano() = 4000000000, which is 10 digits — so Loki's
	// own rule reads it as 4000000000 *seconds*, not 4 seconds. We echo the same
	// instant real Loki would. Grafana only validates the value field, so the
	// quirk is invisible to the health check either way.
	if ts, ok := sample.Value[0].(float64); !ok || ts != 4e9 {
		t.Fatalf("sample timestamp = %v, want 4e9 (epoch seconds, per Loki's <=10-digit rule)", sample.Value[0])
	}
	if v, ok := sample.Value[1].(string); !ok || v != "2" {
		t.Fatalf("sample value = %v, want the string \"2\"", sample.Value[1])
	}
}

// TestLokiInstantQuery_LiteralIsScalar pins the result *type* for a literal-only
// expression. Upstream derives it from the AST — LiteralExpr answers "scalar",
// VectorExpr "vector" (pkg/logql/engine.go) — and both carry the same number, so
// answering "vector" for `1+1` is invisible to a value assertion and wrong to any
// client that switches on resultType.
func TestLokiInstantQuery_LiteralIsScalar(t *testing.T) {
	srv := newLokiServer(t)

	q := url.Values{}
	q.Set("query", "1+1")
	q.Set("time", strconv.FormatInt(time.Unix(4, 0).UnixNano(), 10))
	w := getLoki(t, srv, "/loki/api/v1/query", q)
	if w.Code != 200 {
		t.Fatalf("code = %d, body = %s", w.Code, w.Body.String())
	}
	var resp struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     [2]any `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Status != "success" || resp.Data.ResultType != "scalar" {
		t.Fatalf("envelope = %+v, want success/scalar", resp)
	}
	// A bare [ts, "value"] pair, not a list of samples.
	if ts, ok := resp.Data.Result[0].(float64); !ok || ts != 4e9 {
		t.Fatalf("scalar timestamp = %v, want 4e9", resp.Data.Result[0])
	}
	if v, ok := resp.Data.Result[1].(string); !ok || v != "2" {
		t.Fatalf("scalar value = %v, want the string \"2\"", resp.Data.Result[1])
	}

	// The vector-bearing form still answers "vector" — the two must not collapse
	// onto one type in either direction.
	q.Set("query", "vector(1)+1")
	w = getLoki(t, srv, "/loki/api/v1/query", q)
	var vresp lokiVectorResp
	if err := json.Unmarshal(w.Body.Bytes(), &vresp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if vresp.Data.ResultType != "vector" || len(vresp.Data.Result) != 1 {
		t.Fatalf("vector(1)+1 envelope = %+v, want one vector sample", vresp.Data)
	}
}

// TestLokiInstantQuery_UnsupportedMetricQuery guards the shim's boundary: a
// query that would have to read stored samples still fails explicitly.
func TestLokiInstantQuery_UnsupportedMetricQuery(t *testing.T) {
	srv := newLokiServer(t)
	for _, query := range []string{
		`rate({service="api"}[5m])`,
		`sum by (level) (count_over_time({service="api"}[1m]))`,
	} {
		q := url.Values{}
		q.Set("query", query)
		w := getLoki(t, srv, "/loki/api/v1/query", q)
		if w.Code != 400 {
			t.Fatalf("query %q code = %d, want 400", query, w.Code)
		}
		if !strings.Contains(w.Body.String(), "unsupported") {
			t.Fatalf("query %q body = %q, want an explicit unsupported error", query, w.Body.String())
		}
	}
	// query_range stays log-only: the constant subset is an instant-query shim.
	q := url.Values{}
	q.Set("query", "vector(1)+vector(1)")
	q.Set("start", ns(0))
	q.Set("end", ns(1000))
	if w := getLoki(t, srv, "/loki/api/v1/query_range", q); w.Code != 400 {
		t.Fatalf("query_range vector() code = %d, want 400", w.Code)
	}
}

// TestLokiQuery_EscapedOperands proves a label value or filter operand
// containing a quote — pushable, since ingest only requires valid UTF-8 — is
// also queryable.
func TestLokiQuery_EscapedOperands(t *testing.T) {
	srv := newLokiServer(t)
	pushLogs(t, srv, fmt.Sprintf(`{"streams":[
	 {"stream":{"service":"say \"hi\""},"values":[[%q,"{\"event\":\"login\"}"],[%q,"plain line"]]}
	]}`, ns(100), ns(200)))

	q := url.Values{}
	q.Set("query", `{service="say \"hi\""} |= "\"event\":\"login\""`)
	q.Set("start", ns(0))
	q.Set("end", ns(1000))
	q.Set("direction", "forward")
	w := getLoki(t, srv, "/loki/api/v1/query_range", q)
	if w.Code != 200 {
		t.Fatalf("code = %d, body = %s", w.Code, w.Body.String())
	}
	var resp lokiStreamsResp
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Data.Result) != 1 || len(resp.Data.Result[0].Values) != 1 {
		t.Fatalf("escaped query = %+v, want the one JSON line", resp.Data.Result)
	}
	if resp.Data.Result[0].Stream["service"] != `say "hi"` {
		t.Fatalf("stream label = %q", resp.Data.Result[0].Stream["service"])
	}

	// A malformed selector must be a 400, not a query for a different value.
	q.Set("query", `{service="a"junk"b"}`)
	if w := getLoki(t, srv, "/loki/api/v1/query_range", q); w.Code != 400 {
		t.Fatalf("malformed selector code = %d, want 400", w.Code)
	}
}

// TestLokiQueryRange_TimestampFormats covers the three formats Loki accepts, and
// in particular its digit-length rule: an integer of 10 digits or fewer is
// seconds, a longer one is nanoseconds. Sending a plain second-granularity epoch
// — what a hand-written curl naturally uses — has to select the right window.
func TestLokiQueryRange_TimestampFormats(t *testing.T) {
	srv := newLokiServer(t)
	// One entry at 2023-11-14T22:13:20.000000100Z.
	pushLogs(t, srv, fmt.Sprintf(`{"streams":[{"stream":{"service":"api"},"values":[[%q,"hit"]]}]}`, ns(100)))

	const (
		startSec = 1_699_999_999 // 10 digits → seconds
		endSec   = 1_700_000_001
	)
	cases := []struct {
		name        string
		start, end  string
		wantEntries int
	}{
		{"integer seconds", strconv.Itoa(startSec), strconv.Itoa(endSec), 1},
		{"integer nanoseconds", ns(0), ns(1000), 1},
		{"float seconds", "1699999999.5", "1700000000.5", 1},
		{"RFC3339", "2023-11-14T22:13:19Z", "2023-11-14T22:13:21Z", 1},
		{"RFC3339Nano", "2023-11-14T22:13:20.000000000Z", "2023-11-14T22:13:20.000000200Z", 1},
		{"seconds window before the entry", "1699999990", "1699999995", 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q := url.Values{}
			q.Set("query", `{service="api"}`)
			q.Set("start", c.start)
			q.Set("end", c.end)
			q.Set("direction", "forward")
			w := getLoki(t, srv, "/loki/api/v1/query_range", q)
			if w.Code != 200 {
				t.Fatalf("code = %d, body = %s", w.Code, w.Body.String())
			}
			var resp lokiStreamsResp
			if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			got := 0
			for _, s := range resp.Data.Result {
				got += len(s.Values)
			}
			if got != c.wantEntries {
				t.Fatalf("[%s, %s) returned %d entries, want %d", c.start, c.end, got, c.wantEntries)
			}
		})
	}

	// Rejected: nonsense, and anything outside Go's representable nanosecond
	// window. Time.UnixNano is undefined there and wraps, so without an explicit
	// range check these would become garbage query bounds instead of a 400 — a
	// wrapped negative `start` silently widens the window rather than erroring.
	for _, bad := range []string{
		"not-a-time",
		"99999999999999999999", // overflows int64 outright
		"1e999",
		"9223372036.9",         // whole part in range, total nanoseconds is not
		"-9223372036.9",        // same at the negative boundary
		"9223372037",           // one second past the representable maximum
		"9223372036854775808",  // one nanosecond past the representable maximum
		"2300-01-01T00:00:00Z", // RFC3339 past year 2262
		"1000-01-01T00:00:00Z", // RFC3339 before the 1677 lower bound
	} {
		q := url.Values{}
		q.Set("query", `{service="api"}`)
		q.Set("start", bad)
		if w := getLoki(t, srv, "/loki/api/v1/query_range", q); w.Code != 400 {
			t.Errorf("start=%q code = %d, want 400", bad, w.Code)
		}
	}

	// Accepted at the boundaries themselves, so the check rejects only what it
	// must: the largest whole second on the seconds branch, and the exact
	// nanosecond maximum on the nanosecond branch.
	for _, end := range []string{"9223372036", "9223372036854775807"} {
		q := url.Values{}
		q.Set("query", `{service="api"}`)
		q.Set("start", ns(0))
		q.Set("end", end)
		if w := getLoki(t, srv, "/loki/api/v1/query_range", q); w.Code != 200 {
			t.Errorf("end=%s at the representable maximum: code = %d, want 200", end, w.Code)
		}
	}
}

// TestLokiQueryRange_NegativeTimestampDigitRule pins the parity detail that Loki
// measures the *raw* string when choosing seconds vs nanoseconds: "-1234567890"
// is 11 characters, so it is nanoseconds upstream even though it holds 10 digits.
// Trimming the sign first would silently shift such a bound by a factor of 1e9.
// Ingest only accepts positive nanosecond timestamps, so no fixture can sit
// between the two candidate instants; the range's *validity* is the observable
// difference instead. parseLokiTime's own value assertions live in
// loki_time_test.go.
func TestLokiQueryRange_NegativeTimestampDigitRule(t *testing.T) {
	srv := newLokiServer(t)
	pushLogs(t, srv, twoStreams)

	// start is 14 characters, so it reads as nanoseconds (-1000s) under either
	// rule. end="-1234567890" is 11 characters, so it too is nanoseconds
	// (-1.234567890s) and the range is valid. Trim the sign first and end becomes
	// 10 digits of *seconds* — 1930-11-18Z, decades before start — and the
	// handler answers 400 instead.
	q := url.Values{}
	q.Set("query", `{service="api"}`)
	q.Set("start", "-1000000000000")
	q.Set("end", "-1234567890")
	w := getLoki(t, srv, "/loki/api/v1/query_range", q)
	if w.Code != 200 {
		t.Fatalf("code = %d, body = %s; a 400 means the bound was read as seconds", w.Code, w.Body.String())
	}
	var resp lokiStreamsResp
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Data.Result) != 0 {
		t.Fatalf("pre-epoch window = %+v, want no entries", resp.Data.Result)
	}
}

// TestLokiQueryRange_IntervalRejected covers the guardrail that an unimplemented
// query feature errors rather than returning an unfiltered result. Loki's
// `interval` thins a stream response; silently ignoring it would hand back more
// entries than asked for while looking like it worked. `step` is different — Loki
// itself ignores it on a stream response, so we accept it.
func TestLokiQueryRange_IntervalRejected(t *testing.T) {
	srv := newLokiServer(t)
	pushLogs(t, srv, twoStreams)

	q := url.Values{}
	q.Set("query", `{service="api"}`)
	q.Set("start", ns(0))
	q.Set("end", ns(1000))
	q.Set("interval", "5s")
	w := getLoki(t, srv, "/loki/api/v1/query_range", q)
	if w.Code != 400 {
		t.Fatalf("interval code = %d, want 400; body = %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "interval") {
		t.Fatalf("interval error body = %q, want it to name the parameter", w.Body.String())
	}

	// A valid step is accepted and has no effect on a stream response.
	q.Del("interval")
	q.Set("step", "5s")
	if w := getLoki(t, srv, "/loki/api/v1/query_range", q); w.Code != 200 {
		t.Fatalf("step code = %d, want 200; body = %s", w.Code, w.Body.String())
	}
}

// TestLokiQueryRange_StepValidated covers the half of `step` that is not inert:
// upstream shares one range-query parser across log and metric queries, so it
// parses and range-checks `step` even though the value cannot shape a stream
// response. Accepting garbage silently would diverge from that.
func TestLokiQueryRange_StepValidated(t *testing.T) {
	const ms = int64(time.Millisecond)
	srv := newLokiServer(t)
	pushLogs(t, srv, twoStreams)

	base := func() url.Values {
		q := url.Values{}
		q.Set("query", `{service="api"}`)
		q.Set("start", ns(0))
		q.Set("end", ns(1000))
		q.Set("direction", "forward")
		return q
	}

	for _, bad := range []string{"bogus", "0", "-5", "-1m", "1e300", "5 minutes", "106752d"} {
		q := base()
		q.Set("step", bad)
		w := getLoki(t, srv, "/loki/api/v1/query_range", q)
		if w.Code != 400 {
			t.Errorf("step=%q code = %d, want 400", bad, w.Code)
		}
		if !strings.Contains(w.Body.String(), "step") {
			t.Errorf("step=%q error body = %q, want it to name the parameter", bad, w.Body.String())
		}
	}

	// Both spellings upstream accepts: float seconds and a Prometheus duration.
	// Neither thins the response — all three api entries still come back.
	for _, good := range []string{"5", "0.5", "5s", "1m"} {
		q := base()
		q.Set("step", good)
		if got := entryLines(t, srv, q); len(got) != 3 {
			t.Errorf("step=%q returned %v, want all 3 entries unsampled", good, got)
		}
	}

	// The 11,000-point safety limit, at its boundary. The literal is upstream's
	// own constant, spelled out here so changing ours fails this parity test. An
	// 11,000ms range stepped every 1ms is exactly at the cap; one wider is over.
	const maxPoints = 11000
	q := base()
	q.Set("step", "1ms")
	q.Set("end", ns(maxPoints*ms))
	if w := getLoki(t, srv, "/loki/api/v1/query_range", q); w.Code != 200 {
		t.Errorf("exactly %d points: code = %d, want 200; body = %s", maxPoints, w.Code, w.Body.String())
	}
	q.Set("end", ns((maxPoints+1)*ms))
	w := getLoki(t, srv, "/loki/api/v1/query_range", q)
	if w.Code != 400 {
		t.Errorf("%d points: code = %d, want 400", maxPoints+1, w.Code)
	}
	if !strings.Contains(w.Body.String(), "resolution") {
		t.Errorf("over-limit body = %q, want it to mention the resolution limit", w.Body.String())
	}

	// Sub-millisecond steps, where rounding to milliseconds gets the verdict
	// wrong in *both* directions. 6s at 500µs is 12,000 points and must be
	// rejected; rounding 0.0005 up to 1ms would compute 6,000 and allow it.
	q = base()
	q.Set("step", "0.0005")
	q.Set("end", ns(6*int64(time.Second)))
	if w := getLoki(t, srv, "/loki/api/v1/query_range", q); w.Code != 400 {
		t.Errorf("6s range at 500µs (12,000 points): code = %d, want 400", w.Code)
	}
	// 1s at 100µs is 10,000 points and must be allowed; rounding 0.0001 down to
	// zero would reject it as a non-positive step.
	q = base()
	q.Set("step", "0.0001")
	q.Set("end", ns(int64(time.Second)))
	if w := getLoki(t, srv, "/loki/api/v1/query_range", q); w.Code != 200 {
		t.Errorf("1s range at 100µs (10,000 points): code = %d, want 200; body = %s", w.Code, w.Body.String())
	}

	// The widest expressible range: end-start exceeds int64 nanoseconds and wraps.
	// Upstream computes the span with time.Time.Sub, which saturates at the
	// maximum Duration, so a ~292-year step yields one point and is accepted.
	// Treating the wrapped negative as "over the limit" would 400 instead.
	q = base()
	q.Set("start", "-9223372036854775808")
	q.Set("end", "9223372036854775807")
	q.Set("step", "2562047h")
	if w := getLoki(t, srv, "/loki/api/v1/query_range", q); w.Code != 200 {
		t.Errorf("saturated full-range span at a 292-year step: code = %d, want 200; body = %s", w.Code, w.Body.String())
	}
	// The saturation must not become a blanket pass: the same span with a fine
	// step is still far over the limit.
	q.Set("step", "1s")
	if w := getLoki(t, srv, "/loki/api/v1/query_range", q); w.Code != 400 {
		t.Errorf("saturated full-range span at a 1s step: code = %d, want 400", w.Code)
	}
}

// TestLokiDirectionCaseInsensitive pins parity with upstream's parser, which
// upper-cases the value before matching its enum. Grafana sends lowercase, so
// only a hand-written client notices — but the API advertises Loki compatibility.
func TestLokiDirectionCaseInsensitive(t *testing.T) {
	srv := newLokiServer(t)
	pushLogs(t, srv, twoStreams)

	for _, dir := range []string{"FORWARD", "Forward", "fOrWaRd"} {
		q := url.Values{}
		q.Set("query", `{service="api"}`)
		q.Set("start", ns(0))
		q.Set("end", ns(1000))
		q.Set("direction", dir)
		got := entryLines(t, srv, q)
		if len(got) != 3 || got[0] != "GET /a error" {
			t.Errorf("direction=%q returned %v, want 3 entries oldest-first", dir, got)
		}
	}

	// Backward, upper-cased, orders newest-first.
	q := url.Values{}
	q.Set("query", `{service="api"}`)
	q.Set("start", ns(0))
	q.Set("end", ns(1000))
	q.Set("direction", "BACKWARD")
	if got := entryLines(t, srv, q); len(got) != 3 || got[0] != "GET /c error" {
		t.Errorf("direction=BACKWARD returned %v, want 3 entries newest-first", got)
	}

	// Still rejected when it is not a direction at all, whatever the casing.
	q.Set("direction", "SIDEWAYS")
	if w := getLoki(t, srv, "/loki/api/v1/query_range", q); w.Code != 400 {
		t.Errorf("direction=SIDEWAYS code = %d, want 400", w.Code)
	}
}

// entryLines runs a query_range expected to succeed and returns the log lines in
// response order.
func entryLines(t *testing.T, srv *api.Server, q url.Values) []string {
	t.Helper()
	w := getLoki(t, srv, "/loki/api/v1/query_range", q)
	if w.Code != 200 {
		t.Fatalf("code = %d, body = %s", w.Code, w.Body.String())
	}
	var resp lokiStreamsResp
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	var lines []string
	for _, s := range resp.Data.Result {
		for _, v := range s.Values {
			lines = append(lines, v[1])
		}
	}
	return lines
}

// TestLokiQueryRange_Since covers Loki's `since`: a duration that positions
// `start` relative to `end`. Ignoring it would quietly serve the one-hour
// default instead of the requested window. Its smallest unit is a millisecond,
// so this fixture is spaced in minutes rather than reusing twoStreams.
func TestLokiQueryRange_Since(t *testing.T) {
	const minute = int64(60 * time.Second)
	srv := newLokiServer(t)
	pushLogs(t, srv, fmt.Sprintf(
		`{"streams":[{"stream":{"service":"api"},"values":[[%q,"old"],[%q,"mid"],[%q,"new"]]}]}`,
		ns(0), ns(minute), ns(2*minute)))

	// end at +2m, since 1m → [+1m, +2m): only "mid". The one-hour default would
	// return all three, so a silently ignored `since` fails here.
	q := url.Values{}
	q.Set("query", `{service="api"}`)
	q.Set("end", ns(2*minute))
	q.Set("since", "1m")
	q.Set("direction", "forward")
	if got := entryLines(t, srv, q); len(got) != 1 || got[0] != "mid" {
		t.Fatalf(`since=1m returned %v, want just "mid"`, got)
	}

	// An explicit start supersedes since, per the upstream contract.
	q.Set("start", ns(0))
	if got := entryLines(t, srv, q); len(got) != 2 {
		t.Fatalf("start+since returned %v, want start to win with 2 entries", got)
	}
	q.Del("start")

	// Units Go's time.ParseDuration rejects but Loki accepts, and the bare "0"
	// that needs no unit at all. "0" collapses the window to [end, end).
	// end stays at +2m and the range is half-open, so "new" is out either way.
	q.Set("since", "1d")
	if got := entryLines(t, srv, q); len(got) != 2 {
		t.Errorf(`since=1d returned %v, want "old" and "mid"`, got)
	}
	q.Set("since", "1w")
	if got := entryLines(t, srv, q); len(got) != 2 {
		t.Errorf(`since=1w returned %v, want "old" and "mid"`, got)
	}
	q.Set("since", "0")
	if got := entryLines(t, srv, q); len(got) != 0 {
		t.Errorf("since=0 returned %v, want an empty window", got)
	}

	// Anything outside the Prometheus duration grammar is a 400 rather than a
	// silent fallback — including "150ns" and "1.5h", which Go's parser accepts.
	for _, bad := range []string{"5", "5 minutes", "-5m", "150ns", "1.5h", "30m1h"} {
		q := url.Values{}
		q.Set("query", `{service="api"}`)
		q.Set("since", bad)
		w := getLoki(t, srv, "/loki/api/v1/query_range", q)
		if w.Code != 400 {
			t.Errorf("since=%q code = %d, want 400", bad, w.Code)
		}
		if !strings.Contains(w.Body.String(), "since") {
			t.Errorf("since=%q error body = %q, want it to name the parameter", bad, w.Body.String())
		}
	}
}

// TestLokiQueryRange_SinceAnchorsToNow pins the upstream rule that a relative
// start is measured from min(end, now): with `end` in the future, "the last
// hour" must still mean the last hour of real data, not an empty future window.
func TestLokiQueryRange_SinceAnchorsToNow(t *testing.T) {
	srv := newLokiServer(t)
	recent := time.Now().Add(-30 * time.Minute).UnixNano()
	pushLogs(t, srv, fmt.Sprintf(
		`{"streams":[{"stream":{"service":"api"},"values":[[%q,"recent"]]}]}`,
		strconv.FormatInt(recent, 10)))

	future := strconv.FormatInt(time.Now().Add(24*time.Hour).UnixNano(), 10)

	// Anchored at end, [end-1h, end) would start 23 hours from now and match
	// nothing; anchored at now it covers the 30-minute-old entry.
	for _, name := range []string{"since", "default"} {
		q := url.Values{}
		q.Set("query", `{service="api"}`)
		q.Set("end", future)
		if name == "since" {
			q.Set("since", "1h")
		}
		if got := entryLines(t, srv, q); len(got) != 1 || got[0] != "recent" {
			t.Errorf("%s with a future end returned %v, want the 30-minute-old entry", name, got)
		}
	}
}
