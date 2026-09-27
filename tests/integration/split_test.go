package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/masonwheeler/observability-platform/internal/app"
	"github.com/masonwheeler/observability-platform/internal/config"
)

// process is one running split component.
type process struct {
	t      *testing.T
	cfg    *config.Config
	app    *app.App
	srv    *http.Server
	cancel context.CancelFunc
	done   chan struct{}
}

func (p *process) start() {
	p.t.Helper()
	a, err := app.Build(p.cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		p.t.Fatalf("build %s: %v", p.cfg.Target, err)
	}
	var ln net.Listener
	for range 50 { // the port was just released by stop(); give the OS a moment
		if ln, err = net.Listen("tcp", p.cfg.HTTPAddr); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		p.t.Fatalf("listen %s: %v", p.cfg.HTTPAddr, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	p.app, p.cancel, p.done = a, cancel, make(chan struct{})
	p.srv = &http.Server{Handler: a.Handler}
	go func() { _ = p.srv.Serve(ln) }()
	go func() { a.Run(ctx); close(p.done) }()
}

// stop is a graceful shutdown: stop serving, let the loops finish (the
// ingester's final flush), then close.
func (p *process) stop() {
	_ = p.srv.Shutdown(context.Background())
	p.cancel()
	<-p.done
	p.app.Close()
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

type cluster struct {
	gateway, ingester, querier, store, compactor *process
	gatewayURL, storeURL, compactorURL           string
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
	c := &cluster{gatewayURL: peer(config.TargetGateway), storeURL: peer(config.TargetStore), compactorURL: peer(config.TargetCompactor)}
	mk := func(target config.Target, set func(*config.Config)) *process {
		conf := cfg(target)
		set(conf)
		return &process{t: t, cfg: conf}
	}
	c.gateway = mk(config.TargetGateway, func(x *config.Config) {
		x.IngesterURL, x.QuerierURL = peer(config.TargetIngester), peer(config.TargetQuerier)
	})
	c.ingester = mk(config.TargetIngester, func(x *config.Config) { x.StoreURL = peer(config.TargetStore) })
	c.querier = mk(config.TargetQuerier, func(x *config.Config) {
		x.IngesterURL, x.StoreURL = peer(config.TargetIngester), peer(config.TargetStore)
	})
	c.store = mk(config.TargetStore, func(*config.Config) {})
	c.compactor = mk(config.TargetCompactor, func(x *config.Config) { x.StoreURL = peer(config.TargetStore) })

	// Deliberately in dependency-reversed order: nothing waits for its peers.
	for _, p := range []*process{c.gateway, c.querier, c.compactor, c.ingester, c.store} {
		p.start()
	}
	t.Cleanup(func() {
		for _, p := range []*process{c.gateway, c.querier, c.compactor, c.ingester, c.store} {
			p.stop()
		}
	})
	return c
}

func (c *cluster) ingest(t *testing.T, name string, ts int64, v float64) {
	t.Helper()
	body := fmt.Sprintf(`{"metrics":[{"name":%q,"labels":{"run":"split"},"timestamp_ms":%d,"value":%v}]}`, name, ts, v)
	resp, err := http.Post(c.gatewayURL+"/api/v1/ingest/metrics", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("ingest %s@%d = %d", name, ts, resp.StatusCode)
	}
}

// instant returns (value, HTTP status, errorType) for name at ts through the gateway.
func (c *cluster) instant(t *testing.T, name string, ts int64) (string, int, string) {
	t.Helper()
	resp, err := http.Get(c.gatewayURL + "/api/v1/query?" + url.Values{
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
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if len(body.Data.Result) != 1 {
		return "", resp.StatusCode, body.ErrorType
	}
	return fmt.Sprint(body.Data.Result[0].Value[1]), resp.StatusCode, body.ErrorType
}

func metricValue(t *testing.T, baseURL, name string) float64 {
	t.Helper()
	resp, err := http.Get(baseURL + "/metrics")
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

	// Metrics through the gateway: 121 samples seal one chunk and flush it.
	for i := range 121 {
		c.ingest(t, "split_metric", base+int64(i)*1000, float64(i))
	}
	if v, code, _ := c.instant(t, "split_metric", base+120_000); v != "120" {
		t.Fatalf("head read through the gateway = %q (%d), want 120", v, code)
	}
	eventually(t, "the first block on the store", func() bool { return metricValue(t, c.storeURL, "obs_blocks_total") >= 1 })
	if v, _, _ := c.instant(t, "split_metric", base+50_000); v != "50" {
		t.Fatalf("flushed sample through the gateway = %q, want 50", v)
	}

	// An overwrite at a flushed timestamp wins by generation.
	c.ingest(t, "split_metric", base+50_000, 5000)
	if v, _, _ := c.instant(t, "split_metric", base+50_000); v != "5000" {
		t.Fatalf("overwrite across the flush = %q, want 5000", v)
	}

	// Logs through the gateway reach the store's chunks and read back by value.
	for i := range 10 {
		body := fmt.Sprintf(`{"streams":[{"stream":{"service":"split"},"values":[["%d","split line %d"]]}]}`, (base+int64(i))*1_000_000, i)
		resp, err := http.Post(c.gatewayURL+"/loki/api/v1/push", "application/json", strings.NewReader(body))
		if err != nil || resp.StatusCode != http.StatusNoContent {
			t.Fatalf("push %d: %v %v", i, err, resp)
		}
		resp.Body.Close()
	}
	eventually(t, "log chunks on the store", func() bool { return metricValue(t, c.storeURL, "obs_log_chunks_total") >= 1 })
	resp, err := http.Get(c.gatewayURL + "/loki/api/v1/query_range?" + url.Values{
		"query": {`{service="split"}`}, "start": {fmt.Sprint(base * 1_000_000)}, "end": {fmt.Sprint((base + 1000) * 1_000_000)}, "limit": {"100"},
	}.Encode())
	if err != nil {
		t.Fatal(err)
	}
	logsBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	for i := range 10 {
		if !bytes.Contains(logsBody, []byte(fmt.Sprintf("split line %d", i))) {
			t.Fatalf("log line %d missing through the gateway: %s", i, logsBody)
		}
	}

	// Restart the ingester: WAL replay plus the persisted generation floor.
	c.ingester.stop()
	c.ingester.start()
	if v, _, _ := c.instant(t, "split_metric", base+50_000); v != "5000" {
		t.Fatalf("after an ingester restart = %q, want 5000", v)
	}
	c.ingest(t, "split_metric", base+60_000, 6000)
	if v, _, _ := c.instant(t, "split_metric", base+60_000); v != "6000" {
		t.Fatalf("post-restart overwrite = %q, want 6000", v)
	}

	// A second sealed chunk gives the compactor two blocks in one window.
	for i := 121; i < 241; i++ {
		c.ingest(t, "split_metric", base+int64(i)*1000, float64(i))
	}
	eventually(t, "a compaction", func() bool { return metricValue(t, c.compactorURL, "obs_compactions_total") >= 1 })
	if v, _, _ := c.instant(t, "split_metric", base+60_000); v != "6000" {
		t.Fatalf("after compaction = %q, want 6000", v)
	}

	// Store down: reads fail closed, writes still land.
	c.store.stop()
	if _, code, errType := c.instant(t, "split_metric", base+60_000); code != http.StatusServiceUnavailable || errType != "unavailable" {
		t.Fatalf("query with the store down = %d %q, want 503 unavailable", code, errType)
	}
	c.ingest(t, "split_metric", base+300_000, 300)
	c.store.start()
	eventually(t, "reads to recover", func() bool {
		v, code, _ := c.instant(t, "split_metric", base+300_000)
		return code == http.StatusOK && v == "300"
	})
}
