package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/masonwheeler/observability-platform/internal/app"
	"github.com/masonwheeler/observability-platform/internal/config"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/rpc"
)

// httpClient bounds every request this test makes to the cluster: a hung
// component must fail the test, not the test run itself.
var httpClient = &http.Client{Timeout: 5 * time.Second}

// process is one running split component.
type process struct {
	t       *testing.T
	cfg     *config.Config
	app     *app.App
	srv     *http.Server
	cancel  context.CancelFunc
	done    chan struct{}
	started bool
}

func (p *process) start() {
	p.t.Helper()
	if p.started {
		p.t.Fatalf("process %s: start called while already started", p.cfg.Target)
	}
	a, err := app.Build(p.cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		p.t.Fatalf("build %s: %v", p.cfg.Target, err)
	}
	ln := heldPortAt(p.t, p.cfg.HTTPAddr).serve()
	ctx, cancel := context.WithCancel(context.Background())
	p.app, p.cancel, p.done = a, cancel, make(chan struct{})
	p.srv = &http.Server{Handler: a.Handler}
	go func() { _ = p.srv.Serve(ln) }()
	go func() { a.Run(ctx); close(p.done) }()
	p.started = true
}

// stop is a graceful shutdown: stop serving, let the loops finish (the
// ingester's final flush), then close. It is a no-op when the process is not
// currently started, so a start() that fails partway through a restart (and so
// never reaches the line that sets started) cannot be stopped a second time by
// a t.Cleanup that still holds this same *process: without this guard, that
// second stop would call p.app.Close() on an already-closed app and read from
// an already-closed p.done left over from the previous, successful start.
func (p *process) stop() {
	p.t.Helper()
	if !p.started {
		return
	}
	p.started = false
	_ = p.srv.Shutdown(context.Background())
	p.cancel()
	<-p.done
	p.app.Close()
}

// freeAddr reserves a loopback port for the rest of the test and returns its
// address. The listener stays open the whole time — closing it and binding
// again later would let any other socket on the machine take the port in
// between — and each start of a process serves on it through heldPort.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	h := &heldPort{ln: ln}
	addr := ln.Addr().String()
	heldPorts.Store(addr, h)
	go h.acceptLoop()
	t.Cleanup(func() {
		heldPorts.Delete(addr)
		_ = ln.Close()
	})
	return addr
}

// heldPorts maps each address freeAddr reserved to its heldPort.
var heldPorts sync.Map

func heldPortAt(t *testing.T, addr string) *heldPort {
	t.Helper()
	h, ok := heldPorts.Load(addr)
	if !ok {
		t.Fatalf("no port reserved at %s: use freeAddr", addr)
	}
	return h.(*heldPort)
}

// heldPort is a listener that outlives the servers using it. One goroutine
// accepts every connection and hands it to the server currently serving; while
// none is (its process is stopped), it closes the connection at once, which a
// peer's client sees as a transport error — an outage — just as it would a
// refused connection.
type heldPort struct {
	ln  net.Listener
	mu  sync.Mutex
	cur *handoff
}

func (h *heldPort) acceptLoop() {
	for {
		conn, err := h.ln.Accept()
		if err != nil {
			return
		}
		h.mu.Lock()
		cur := h.cur
		h.mu.Unlock()
		if cur == nil || !cur.deliver(conn) {
			_ = conn.Close()
		}
	}
}

// serve returns a listener for one server's lifetime; the server's Shutdown
// closes it, which stops only the handoff, never the held port.
func (h *heldPort) serve() net.Listener {
	l := &handoff{addr: h.ln.Addr(), conns: make(chan net.Conn), done: make(chan struct{})}
	h.mu.Lock()
	h.cur = l
	h.mu.Unlock()
	return l
}

// handoff is the net.Listener one http.Server serves on.
type handoff struct {
	addr  net.Addr
	conns chan net.Conn
	done  chan struct{}
	once  sync.Once
}

func (l *handoff) deliver(c net.Conn) bool {
	select {
	case l.conns <- c:
		return true
	case <-l.done:
		return false
	}
}

func (l *handoff) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *handoff) Close() error {
	l.once.Do(func() { close(l.done) })
	return nil
}

func (l *handoff) Addr() net.Addr { return l.addr }

type cluster struct {
	gateway, ingester, querier, store, compactor    *process
	gatewayURL, ingesterURL, storeURL, compactorURL string
}

func startCluster(t *testing.T) *cluster {
	t.Helper()
	addr := map[config.Target]string{}
	for _, target := range []config.Target{config.TargetGateway, config.TargetIngester, config.TargetQuerier, config.TargetStore, config.TargetCompactor} {
		addr[target] = freeAddr(t)
	}
	peer := func(target config.Target) string { return "http://" + addr[target] }
	cfg := func(target config.Target) *config.Config {
		return &config.Config{
			Target: target, HTTPAddr: addr[target], DataDir: t.TempDir(), LogLevel: "info",
			WALSegmentMaxBytes: 1 << 20, WALSyncEveryN: 1,
			LogsFlushThresholdBytes: 64, // every few pushes flush to the store
			MaintenanceInterval:     50 * time.Millisecond,
			FlushInterval:           time.Hour, // count-based flushes only:
			FlushSealedChunks:       1,         // flush as soon as a chunk seals
			CompactionBaseRange:     2 * time.Hour, CompactionMultiplier: 4, CompactionLevels: 3,
		}
	}
	c := &cluster{
		gatewayURL: peer(config.TargetGateway), ingesterURL: peer(config.TargetIngester),
		storeURL: peer(config.TargetStore), compactorURL: peer(config.TargetCompactor),
	}
	mk := func(target config.Target, set func(*config.Config)) *process {
		conf := cfg(target)
		set(conf)
		return &process{t: t, cfg: conf}
	}
	c.gateway = mk(config.TargetGateway, func(x *config.Config) {
		x.IngesterURL, x.QuerierURL = peer(config.TargetIngester), peer(config.TargetQuerier)
		x.IngesterURLs = []string{peer(config.TargetIngester)}
	})
	c.ingester = mk(config.TargetIngester, func(x *config.Config) { x.StoreURL = peer(config.TargetStore) })
	c.querier = mk(config.TargetQuerier, func(x *config.Config) {
		x.IngesterURL, x.StoreURL = peer(config.TargetIngester), peer(config.TargetStore)
		x.IngesterURLs = []string{peer(config.TargetIngester)}
	})
	c.store = mk(config.TargetStore, func(*config.Config) {})
	c.compactor = mk(config.TargetCompactor, func(x *config.Config) { x.StoreURL = peer(config.TargetStore) })

	// Deliberately in dependency-reversed order: nothing waits for its peers.
	// Each process registers its own cleanup right after it starts, so a
	// process that fails to start partway through this loop still leaves every
	// earlier, successfully started process to be torn down -- the whole
	// t.Cleanup registration line is never reached (t.Fatalf inside start()
	// unwinds via runtime.Goexit before this loop would get there) if it were
	// registered once after the loop instead.
	for _, p := range []*process{c.gateway, c.querier, c.compactor, c.ingester, c.store} {
		p.start()
		t.Cleanup(p.stop)
	}
	return c
}

func (c *cluster) ingest(t *testing.T, name string, ts int64, v float64) {
	t.Helper()
	body := fmt.Sprintf(`{"metrics":[{"name":%q,"labels":{"run":"split"},"timestamp_ms":%d,"value":%v}]}`, name, ts, v)
	resp, err := httpClient.Post(c.gatewayURL+"/api/v1/ingest/metrics", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("ingest %s@%d = %d: %s", name, ts, resp.StatusCode, b)
	}
}

// instant returns (value, HTTP status, errorType) for name at ts through the gateway.
func (c *cluster) instant(t *testing.T, name string, ts int64) (string, int, string) {
	t.Helper()
	resp, err := httpClient.Get(c.gatewayURL + "/api/v1/query?" + url.Values{
		"query": {name}, "time": {fmt.Sprintf("%.3f", float64(ts)/1000)},
	}.Encode())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body struct {
		ErrorType string `json:"errorType"`
		Data      struct {
			Result []struct {
				Value [2]any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode instant query response: %v", err)
	}
	if len(body.Data.Result) != 1 {
		return "", resp.StatusCode, body.ErrorType
	}
	return fmt.Sprint(body.Data.Result[0].Value[1]), resp.StatusCode, body.ErrorType
}

// pushLog pushes one Loki log line for service through the gateway and fails
// the test unless it is answered 204 -- the write path must never fail just
// because a downstream flush is unavailable (the ingester's log head tolerates
// that), so 204 here is itself part of the assertion, not just a precondition.
func (c *cluster) pushLog(t *testing.T, service string, tsNs int64, line string) {
	t.Helper()
	body := fmt.Sprintf(`{"streams":[{"stream":{"service":%q},"values":[["%d",%q]]}]}`, service, tsNs, line)
	resp, err := httpClient.Post(c.gatewayURL+"/loki/api/v1/push", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("loki push %q@%d = %d: %s", line, tsNs, resp.StatusCode, b)
	}
}

// lokiEntry is one decoded Loki query_range log line.
type lokiEntry struct {
	ts   int64
	line string
}

// lokiQueryRange runs a Loki query_range for query over [startNs, endNs]
// through the gateway. On a non-200 answer it returns the status and the raw
// (plain-text) body for the caller to inspect, without attempting to decode it
// as JSON -- writeLokiError never writes JSON. On 200 it decodes the streams
// envelope and fails the test on any decode error, rather than silently
// returning nothing for a response that does not parse.
func (c *cluster) lokiQueryRange(t *testing.T, query string, startNs, endNs int64) (status int, entries []lokiEntry, rawBody string) {
	t.Helper()
	resp, err := httpClient.Get(c.gatewayURL + "/loki/api/v1/query_range?" + url.Values{
		"query": {query}, "start": {fmt.Sprint(startNs)}, "end": {fmt.Sprint(endNs)}, "limit": {"1000"},
	}.Encode())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read loki query_range body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, nil, string(body)
	}
	var parsed struct {
		Data struct {
			Result []struct {
				Values [][2]string `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		t.Fatalf("decode loki query_range response: %v (body=%s)", err, body)
	}
	for _, r := range parsed.Data.Result {
		for _, v := range r.Values {
			ts, err := strconv.ParseInt(v[0], 10, 64)
			if err != nil {
				t.Fatalf("decode loki entry timestamp %q: %v", v[0], err)
			}
			entries = append(entries, lokiEntry{ts: ts, line: v[1]})
		}
	}
	return resp.StatusCode, entries, string(body)
}

// assertLogSet fails the test unless entries is exactly the set described by
// expected (ts in nanoseconds -> line), neither more nor fewer -- an exact
// match, not a substring scan that would pass even if entries were duplicated
// or missing lines interleaved with unrelated ones.
func assertLogSet(t *testing.T, label string, entries []lokiEntry, expected map[int64]string) {
	t.Helper()
	got := make(map[int64]string, len(entries))
	for _, e := range entries {
		got[e.ts] = e.line
	}
	if len(got) != len(entries) {
		t.Fatalf("%s: query_range returned duplicate timestamps: %v", label, entries)
	}
	if len(got) != len(expected) {
		t.Fatalf("%s: got %d log entries, want %d\n got  %v\n want %v", label, len(got), len(expected), got, expected)
	}
	for ts, want := range expected {
		line, ok := got[ts]
		if !ok {
			t.Fatalf("%s: missing log entry at ts %d (want %q)", label, ts, want)
		}
		if line != want {
			t.Fatalf("%s: log entry at ts %d = %q, want %q", label, ts, line, want)
		}
	}
}

// metricSelectFrom reads split_metric directly from one component's own
// /internal/v1/metrics/select -- the ingester's head or the store's blocks,
// never merged -- so a check can tell which side currently holds a sample.
func metricSelectFrom(t *testing.T, peer, baseURL string, minMs, maxMs int64) []metrics.Sample {
	t.Helper()
	cl, err := rpc.NewClient(peer, baseURL)
	if err != nil {
		t.Fatal(err)
	}
	sds, err := rpc.NewMetricsSource(cl).Select(context.Background(), metrics.SelectParams{
		Selector: metrics.Selector{MetricName: "split_metric"},
		MinT:     minMs,
		MaxT:     maxMs,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(sds) == 0 {
		return nil
	}
	if len(sds) != 1 {
		t.Fatalf("%s: split_metric resolved to %d series, want exactly 1", peer, len(sds))
	}
	return sds[0].Samples
}

// metricsMergedSnapshot reads every split_metric sample the ingester and the
// store together hold, merged exactly the way the querier merges them
// (metrics.Merge(ingester, store) over the same rpc.MetricsSource client the
// querier itself uses) -- the querier has no internal API of its own to poll
// directly, so this is that same merge, built in the test process instead of
// inferred from a formatted query_range response.
func metricsMergedSnapshot(t *testing.T, c *cluster) []metrics.Sample {
	t.Helper()
	ing, err := rpc.NewClient("ingester", c.ingesterURL)
	if err != nil {
		t.Fatal(err)
	}
	st, err := rpc.NewClient("store", c.storeURL)
	if err != nil {
		t.Fatal(err)
	}
	merged := metrics.Merge(rpc.NewMetricsSource(ing), rpc.NewMetricsSource(st))
	sds, err := merged.Select(context.Background(), metrics.SelectParams{
		Selector: metrics.Selector{MetricName: "split_metric"},
		MinT:     math.MinInt64,
		MaxT:     math.MaxInt64,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(sds) == 0 {
		return nil
	}
	if len(sds) != 1 {
		t.Fatalf("merged: split_metric resolved to %d series, want exactly 1", len(sds))
	}
	return sds[0].Samples
}

// assertMetricSet fails the test unless samples is exactly the set described
// by expected (offset in ms from base -> value), neither more nor fewer.
func assertMetricSet(t *testing.T, label string, samples []metrics.Sample, base int64, expected map[int64]float64) {
	t.Helper()
	got := make(map[int64]float64, len(samples))
	for _, s := range samples {
		got[s.TimestampMs-base] = s.Value
	}
	if len(got) != len(samples) {
		t.Fatalf("%s: duplicate timestamps in %v", label, samples)
	}
	if len(got) != len(expected) {
		t.Fatalf("%s: got %d samples, want %d\n got  %v\n want %v", label, len(got), len(expected), got, expected)
	}
	for off, want := range expected {
		v, ok := got[off]
		if !ok {
			t.Fatalf("%s: missing sample at offset %dms (want %v)", label, off, want)
		}
		if v != want {
			t.Fatalf("%s: offset %dms = %v, want %v", label, off, v, want)
		}
	}
}

// queryRangeGateway runs a Prometheus-compatible query_range for query over
// [startMs, endMs] at stepMs through the gateway -- the actual public API a
// Grafana panel would hit, and the one component the ingester/store-only
// checks above (metricSelectFrom, metricsMergedSnapshot) cannot exercise,
// since those bypass the gateway, the querier process, and the query engine
// entirely. On a non-200 answer the point map is nil; the caller decides
// whether that is expected.
func (c *cluster) queryRangeGateway(t *testing.T, query string, startMs, endMs, stepMs int64) (status int, points map[int64]float64) {
	t.Helper()
	resp, err := httpClient.Get(c.gatewayURL + "/api/v1/query_range?" + url.Values{
		"query": {query},
		"start": {fmt.Sprintf("%.3f", float64(startMs)/1000)},
		"end":   {fmt.Sprintf("%.3f", float64(endMs)/1000)},
		"step":  {fmt.Sprintf("%.3f", float64(stepMs)/1000)},
	}.Encode())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body struct {
		ErrorType string `json:"errorType"`
		Data      struct {
			Result []struct {
				Values [][2]any `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode query_range response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, nil
	}
	if len(body.Data.Result) != 1 {
		t.Fatalf("query_range %s: got %d series, want exactly 1", query, len(body.Data.Result))
	}
	points = make(map[int64]float64, len(body.Data.Result[0].Values))
	for _, v := range body.Data.Result[0].Values {
		tsSec, ok := v[0].(float64)
		if !ok {
			t.Fatalf("query_range %s: point timestamp %v is not a number", query, v[0])
		}
		valStr, ok := v[1].(string)
		if !ok {
			t.Fatalf("query_range %s: point value %v is not a string", query, v[1])
		}
		val, err := strconv.ParseFloat(valStr, 64)
		if err != nil {
			t.Fatalf("query_range %s: point value %q: %v", query, valStr, err)
		}
		tsMs := int64(math.Round(tsSec * 1000))
		if _, dup := points[tsMs]; dup {
			t.Fatalf("query_range %s: duplicate point at ts %d", query, tsMs)
		}
		points[tsMs] = val
	}
	return resp.StatusCode, points
}

// holdForwardExpected reconstructs, from the exact discrete samples known so
// far (expected: offset in ms from base -> value), what a range query must
// return at every 1-second tick in [0, maxOffsetMs] under
// metrics.QueryEngine.RangeQueryContext's real, documented semantics: each
// tick's value is the latest sample at or before it. A tick with no sample of
// its own therefore repeats the nearest earlier one -- this is not an
// approximation of the engine's behavior, it is that behavior, computed
// test-side so the assertion can be exact even while ingestion is still
// mid-flight (e.g. the offsets between the most recent flush and the next
// batch not yet written).
func holdForwardExpected(expected map[int64]float64, maxOffsetMs int64) map[int64]float64 {
	out := make(map[int64]float64, maxOffsetMs/1000+1)
	var cur float64
	haveCur := false
	for off := int64(0); off <= maxOffsetMs; off += 1000 {
		if v, ok := expected[off]; ok {
			cur, haveCur = v, true
		}
		if haveCur {
			out[off] = cur
		}
	}
	return out
}

// assertMetricRangeGateway runs the gateway's /api/v1/query_range over
// [base, base+240s] at a 1s step -- spec §18/§12.3's actual read path, through
// the gateway, the querier process, its HTTP API, and the query engine, not a
// client-side reconstruction of the merge -- and fails unless every one of the
// 241 (ts, value) points matches expected exactly.
func assertMetricRangeGateway(t *testing.T, label string, c *cluster, base int64, expected map[int64]float64) {
	t.Helper()
	code, points := c.queryRangeGateway(t, "split_metric", base, base+240_000, 1000)
	if code != http.StatusOK {
		t.Fatalf("%s: query_range = %d, want 200", label, code)
	}
	got := make(map[int64]float64, len(points))
	for tsMs, v := range points {
		got[tsMs-base] = v
	}
	if len(got) != len(points) {
		t.Fatalf("%s: query_range returned duplicate timestamps", label)
	}
	if len(got) != len(expected) {
		t.Fatalf("%s: query_range got %d points, want %d\n got  %v\n want %v", label, len(got), len(expected), got, expected)
	}
	for off, want := range expected {
		v, ok := got[off]
		if !ok {
			t.Fatalf("%s: query_range missing point at offset %dms (want %v)", label, off, want)
		}
		if v != want {
			t.Fatalf("%s: query_range offset %dms = %v, want %v", label, off, v, want)
		}
	}
}

func metricValue(t *testing.T, baseURL, name string) float64 {
	t.Helper()
	resp, err := httpClient.Get(baseURL + "/metrics")
	if err != nil {
		return -1
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, name+" ") {
			var v float64
			_, _ = fmt.Sscan(strings.TrimPrefix(line, name+" "), &v)
			return v
		}
	}
	return -1
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestSplitClusterEndToEnd(t *testing.T) {
	c := startCluster(t)
	base := (time.Now().UnixMilli() / 7_200_000) * 7_200_000 // one 2h window: blocks compact together

	// expectedMetrics tracks every split_metric sample that should currently be
	// readable, keyed by its offset in ms from base -- built up as the test
	// ingests, and checked exhaustively (not just at a handful of points) at
	// each of the checkpoints below.
	expectedMetrics := map[int64]float64{}

	// Metrics through the gateway: 121 samples seal one chunk and flush it.
	for i := range 121 {
		c.ingest(t, "split_metric", base+int64(i)*1000, float64(i))
		expectedMetrics[int64(i)*1000] = float64(i)
	}
	if v, code, _ := c.instant(t, "split_metric", base+120_000); v != "120" {
		t.Fatalf("head read through the gateway = %q (%d), want 120", v, code)
	}
	eventually(t, "the first block on the store", func() bool { return metricValue(t, c.storeURL, "obs_blocks_total") >= 1 })
	if v, _, _ := c.instant(t, "split_metric", base+50_000); v != "50" {
		t.Fatalf("flushed sample through the gateway = %q, want 50", v)
	}

	// The store registers a flushed block before it acknowledges the flush,
	// and the ingester's head drops those chunks only once it has that
	// acknowledgement -- so prove the discard actually happens, on each side's
	// own internal API, rather than inferring it from a merged read that could
	// still be satisfied by either side alone.
	eventually(t, "the ingester head to discard the flushed sample", func() bool {
		return len(metricSelectFrom(t, "ingester", c.ingesterURL, base+50_000, base+50_000)) == 0
	})
	if s := metricSelectFrom(t, "store", c.storeURL, base+50_000, base+50_000); len(s) != 1 || s[0].Value != 50 {
		t.Fatalf("store sample at +50s = %v, want exactly one sample of 50", s)
	}

	// An overwrite at a flushed timestamp wins by generation.
	c.ingest(t, "split_metric", base+50_000, 5000)
	expectedMetrics[50_000] = 5000
	if v, _, _ := c.instant(t, "split_metric", base+50_000); v != "5000" {
		t.Fatalf("overwrite across the flush = %q, want 5000", v)
	}

	// Logs through the gateway reach the store's chunks and read back by
	// value: the exact set of (ts, line) entries, not a substring scan.
	expectedLogs := map[int64]string{}
	for i := range 10 {
		ts := (base + int64(i)) * 1_000_000
		line := fmt.Sprintf("split line %d", i)
		c.pushLog(t, "split", ts, line)
		expectedLogs[ts] = line
	}
	eventually(t, "log chunks on the store", func() bool { return metricValue(t, c.storeURL, "obs_log_chunks_total") >= 1 })
	logsStartNs, logsEndNs := base*1_000_000, (base+1000)*1_000_000
	status, entries, _ := c.lokiQueryRange(t, `{service="split"}`, logsStartNs, logsEndNs)
	if status != http.StatusOK {
		t.Fatalf("loki query_range after the initial push = %d, want 200", status)
	}
	assertLogSet(t, "after the initial push", entries, expectedLogs)

	// Restart the ingester: WAL replay plus the persisted generation floor.
	c.ingester.stop()
	c.ingester.start()
	if v, _, _ := c.instant(t, "split_metric", base+50_000); v != "5000" {
		t.Fatalf("after an ingester restart = %q, want 5000", v)
	}
	c.ingest(t, "split_metric", base+60_000, 6000)
	expectedMetrics[60_000] = 6000
	if v, _, _ := c.instant(t, "split_metric", base+60_000); v != "6000" {
		t.Fatalf("post-restart overwrite = %q, want 6000", v)
	}
	// The gateway's actual read path (spec §18/§12.3): through the gateway, the
	// querier process, its HTTP API, and the query engine -- not the
	// client-side merge reconstruction below, kept only as an extra.
	assertMetricRangeGateway(t, "after the ingester restart", c, base, holdForwardExpected(expectedMetrics, 240_000))
	assertMetricSet(t, "after the ingester restart (client-side merge, extra)", metricsMergedSnapshot(t, c), base, expectedMetrics)
	status, entries, _ = c.lokiQueryRange(t, `{service="split"}`, logsStartNs, logsEndNs)
	if status != http.StatusOK {
		t.Fatalf("loki query_range after the ingester restart = %d, want 200", status)
	}
	assertLogSet(t, "after the ingester restart", entries, expectedLogs)

	// A second sealed chunk gives the compactor two blocks in one window.
	for i := 121; i < 241; i++ {
		c.ingest(t, "split_metric", base+int64(i)*1000, float64(i))
		expectedMetrics[int64(i)*1000] = float64(i)
	}
	eventually(t, "a compaction", func() bool { return metricValue(t, c.compactorURL, "obs_compactions_total") >= 1 })
	if v, _, _ := c.instant(t, "split_metric", base+60_000); v != "6000" {
		t.Fatalf("after compaction = %q, want 6000", v)
	}
	assertMetricRangeGateway(t, "after the compaction", c, base, holdForwardExpected(expectedMetrics, 240_000))
	assertMetricSet(t, "after the compaction (client-side merge, extra)", metricsMergedSnapshot(t, c), base, expectedMetrics)
	status, entries, _ = c.lokiQueryRange(t, `{service="split"}`, logsStartNs, logsEndNs)
	if status != http.StatusOK {
		t.Fatalf("loki query_range after the compaction = %d, want 200", status)
	}
	assertLogSet(t, "after the compaction", entries, expectedLogs)

	// Store down: reads fail closed, writes still land -- for both metrics and
	// logs.
	c.store.stop()
	if _, code, errType := c.instant(t, "split_metric", base+60_000); code != http.StatusServiceUnavailable || errType != "unavailable" {
		t.Fatalf("query with the store down = %d %q, want 503 unavailable", code, errType)
	}
	c.ingest(t, "split_metric", base+300_000, 300)
	expectedMetrics[300_000] = 300

	outageTs, outageLine := (base+10)*1_000_000, "split line 10"
	c.pushLog(t, "split", outageTs, outageLine) // writes still land: 204, asserted inside pushLog
	expectedLogs[outageTs] = outageLine
	if code, _, body := c.lokiQueryRange(t, `{service="split"}`, logsStartNs, logsEndNs); code != http.StatusServiceUnavailable || !strings.Contains(body, "unavailable") {
		t.Fatalf("loki query_range with the store down = %d %q, want 503 containing \"unavailable\"", code, body)
	}

	c.store.start()
	eventually(t, "reads to recover", func() bool {
		v, code, _ := c.instant(t, "split_metric", base+300_000)
		return code == http.StatusOK && v == "300"
	})
	// After recovery, re-read everything the store held before the outage (the
	// 50->5000 and 60->6000 overwrites included). The gateway range check stays
	// bounded to [0, 240s] (300 is outside that window and already reconfirmed
	// by the "reads to recover" poll above and expectedMetrics' 300 entry, which
	// the client-side merge check below still covers over the full range).
	assertMetricRangeGateway(t, "after the store recovers", c, base, holdForwardExpected(expectedMetrics, 240_000))
	assertMetricSet(t, "after the store recovers (client-side merge, extra)", metricsMergedSnapshot(t, c), base, expectedMetrics)
	status, entries, _ = c.lokiQueryRange(t, `{service="split"}`, logsStartNs, logsEndNs)
	if status != http.StatusOK {
		t.Fatalf("loki query_range after the store recovers = %d, want 200", status)
	}
	assertLogSet(t, "after the store recovers", entries, expectedLogs)
}
