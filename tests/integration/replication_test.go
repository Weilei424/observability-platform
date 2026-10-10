package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/rpc"
)

// ingestRep posts one rep_metric{i=<i>} sample per i in [0, n) at ts.
func (c *cluster) ingestRep(t *testing.T, n int, ts int64, v float64) {
	t.Helper()
	items := make([]string, n)
	for i := range n {
		items[i] = fmt.Sprintf(`{"name":"rep_metric","labels":{"i":"%d"},"timestamp_ms":%d,"value":%v}`, i, ts, v)
	}
	resp, err := httpClient.Post(c.gatewayURL+"/api/v1/ingest/metrics", "application/json",
		strings.NewReader(`{"metrics":[`+strings.Join(items, ",")+`]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("ingest rep_metric = %d: %s", resp.StatusCode, b)
	}
}

// instantExpr evaluates a single-result PromQL expression at ts through the
// gateway and returns its value.
func (c *cluster) instantExpr(t *testing.T, expr string, ts int64) string {
	t.Helper()
	return c.singleValue(t, "/api/v1/query", url.Values{"query": {expr}, "time": {fmt.Sprintf("%.3f", float64(ts)/1000)}})
}

// lokiInstant evaluates a LogQL metric query at tsNs through the gateway.
func (c *cluster) lokiInstant(t *testing.T, expr string, tsNs int64) string {
	t.Helper()
	return c.singleValue(t, "/loki/api/v1/query", url.Values{"query": {expr}, "time": {fmt.Sprint(tsNs)}})
}

// instantSeriesCount returns how many series expr returns at ts.
func (c *cluster) instantSeriesCount(t *testing.T, expr string, ts int64) int {
	t.Helper()
	resp, err := httpClient.Get(c.gatewayURL + "/api/v1/query?" + url.Values{
		"query": {expr}, "time": {fmt.Sprintf("%.3f", float64(ts)/1000)}}.Encode())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var body struct {
		Data struct {
			Result []json.RawMessage `json:"result"`
		} `json:"data"`
	}
	if resp.StatusCode != http.StatusOK || json.Unmarshal(b, &body) != nil {
		t.Fatalf("query %s = %d: %s", expr, resp.StatusCode, b)
	}
	return len(body.Data.Result)
}

func (c *cluster) singleValue(t *testing.T, path string, q url.Values) string {
	t.Helper()
	resp, err := httpClient.Get(c.gatewayURL + path + "?" + q.Encode())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var body struct {
		Data struct {
			Result []struct {
				Value [2]any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if resp.StatusCode != http.StatusOK || json.Unmarshal(b, &body) != nil || len(body.Data.Result) != 1 {
		t.Fatalf("%s %s = %d: %s", path, q.Get("query"), resp.StatusCode, b)
	}
	return fmt.Sprint(body.Data.Result[0].Value[1])
}

func TestReplicationOneIngesterDown(t *testing.T) {
	c := startClusterRF(t, 3, 3)
	base := time.Now().UnixMilli()
	c.ingesters[1].stop()

	names := make([]string, 20)
	for i := range names {
		names[i] = fmt.Sprintf("rep_down_%d", i)
	}
	if code := c.ingestBatch(t, names, base, 7); code != http.StatusNoContent {
		t.Fatalf("batch write with one ingester down = %d, want 204", code)
	}
	for _, n := range names {
		if v, code, _ := c.instant(t, n, base); code != http.StatusOK || v != "7" {
			t.Errorf("%s = %q (%d), want 7", n, v, code)
		}
	}

	ts := base * 1_000_000
	c.pushLog(t, "rep_down", ts, "survives")
	status, entries, body := c.lokiQueryRange(t, `{service="rep_down"}`, ts-1_000_000_000, ts+1_000_000_000)
	if status != http.StatusOK {
		t.Fatalf("loki read with one ingester down = %d: %s", status, body)
	}
	assertLogSet(t, "one ingester down", entries, map[int64]string{ts: "survives"})
}

func TestReplicationTwoIngestersDown(t *testing.T) {
	c := startClusterRF(t, 3, 3)
	base := time.Now().UnixMilli()
	c.ingest(t, "rep_two", base, 1) // healthy first, so the read has data to lose
	c.ingesters[0].stop()
	c.ingesters[1].stop()

	names := []string{"rep_two_a", "rep_two_b", "rep_two_c", "rep_two_d"}
	resp, err := httpClient.Post(c.gatewayURL+"/api/v1/ingest/metrics", "application/json", strings.NewReader(fmt.Sprintf(
		`{"metrics":[{"name":%q,"labels":{"run":"split"},"timestamp_ms":%d,"value":1}]}`, names[0], base)))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable || !strings.Contains(string(b), "write quorum not met") {
		t.Fatalf("write with two ingesters down = %d %q, want 503 write quorum not met", resp.StatusCode, b)
	}
	if code := c.ingestBatch(t, names, base, 1); code != http.StatusServiceUnavailable {
		t.Fatalf("batch write with two ingesters down = %d, want 503", code)
	}
	if _, code, typ := c.instant(t, "rep_two", base); code != http.StatusServiceUnavailable || typ != "unavailable" {
		t.Fatalf("read with two ingesters down = %d %q, want 503 unavailable", code, typ)
	}
	if code := c.pushLogCode(t, "rep_two", base*1_000_000, "x"); code != http.StatusServiceUnavailable {
		t.Fatalf("loki push with two ingesters down = %d, want 503", code)
	}
}

func TestReplicationHungIngester(t *testing.T) {
	c := startClusterRF(t, 3, 3)
	stop := make(chan struct{})
	hung := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-stop }))
	t.Cleanup(hung.Close)
	t.Cleanup(func() { close(stop) }) // runs before hung.Close, which waits for handlers
	c.reconfigure(t, []string{c.ingesterURLs[0], c.ingesterURLs[1], hung.URL})

	base := time.Now().UnixMilli()
	start := time.Now()
	names := []string{"rep_hung_a", "rep_hung_b", "rep_hung_c"}
	if code := c.ingestBatch(t, names, base, 3); code != http.StatusNoContent {
		t.Fatalf("write with a hung ingester = %d, want 204", code)
	}
	if d := time.Since(start); d > 1500*time.Millisecond {
		t.Errorf("write took %v, want well inside the 2s timeout (quorum from the live ingesters)", d)
	}
	for _, n := range names {
		start = time.Now()
		v, code, _ := c.instant(t, n, base)
		if code != http.StatusOK || v != "3" {
			t.Errorf("%s = %q (%d), want 3", n, v, code)
		}
		if d := time.Since(start); d > 3*time.Second {
			t.Errorf("read of %s took %v, want under 3s (one 2s timeout skipped)", n, d)
		}
	}
}

func TestReplicationDuplicatesNeverChangeAnswers(t *testing.T) {
	c := startClusterRF(t, 3, 3)
	base := (time.Now().UnixMilli() / 7_200_000) * 7_200_000 // one 2h window: blocks compact together
	baseNs := base * 1_000_000

	c.ingestRep(t, 5, base, 1)
	c.ingestRep(t, 5, base+30_000, 31) // a second sample, so rate() has two points per series
	for i := range 3 {
		c.pushLog(t, "rep", baseNs+int64(i), fmt.Sprintf("rep line %d", i))
	}
	wantLogs := map[int64]string{baseNs: "rep line 0", baseNs + 1: "rep line 1", baseNs + 2: "rep line 2"}
	check := func(label string) {
		t.Helper()
		if v := c.instantExpr(t, "sum(rep_metric)", base); v != "5" {
			t.Errorf("%s: sum = %q, want 5", label, v)
		}
		// rate() is (last - first) / window over each series' samples in the
		// window: (31 - 1) / 60s = 0.5 per series, 2.5 for the five. A copy
		// merged out of order, or counted twice, would change it.
		if v := c.instantExpr(t, "sum(rate(rep_metric[1m]))", base+30_000); v != "2.5" {
			t.Errorf("%s: sum(rate) = %q, want 2.5", label, v)
		}
		if n := c.instantSeriesCount(t, "rep_metric", base); n != 5 {
			t.Errorf("%s: rep_metric returns %d series, want 5 (count() is unsupported, so series are counted directly)", label, n)
		}
		if v := c.lokiInstant(t, `count_over_time({service="rep"}[1m])`, baseNs+10_000_000); v != "3" {
			t.Errorf("%s: count_over_time = %q, want 3", label, v)
		}
		status, entries, body := c.lokiQueryRange(t, `{service="rep"}`, baseNs-1_000_000_000, baseNs+1_000_000_000)
		if status != http.StatusOK {
			t.Fatalf("%s: loki range = %d: %s", label, status, body)
		}
		assertLogSet(t, label, entries, wantLogs)
	}
	check("three head copies")

	// Flush every ingester into the store, then restart it: a drained
	// ingester refuses writes until restarted.
	for _, p := range c.ingesters {
		if code, body := postDrain(t, "http://"+p.cfg.HTTPAddr); code != http.StatusOK {
			t.Fatalf("drain = %d: %s", code, body)
		}
	}
	for _, p := range c.ingesters {
		p.stop()
		p.start()
	}
	check("three store copies")

	// Compaction merges the three copies' blocks; the answer stays the same.
	eventually(t, "a compaction", func() bool { return metricValue(t, c.compactorURL, "obs_compactions_total") >= 1 })
	check("after compaction")
	cl, err := rpc.NewClient("store", c.storeURL)
	if err != nil {
		t.Fatal(err)
	}
	sds, err := rpc.NewMetricsSource(cl).Select(context.Background(), metrics.SelectParams{
		Selector: metrics.Selector{MetricName: "rep_metric"}, MinT: base, MaxT: base + 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(sds) != 5 {
		t.Fatalf("store holds %d rep_metric series, want 5", len(sds))
	}
	for _, sd := range sds {
		if len(sd.Samples) != 1 || sd.Samples[0].TimestampMs != base {
			t.Errorf("store series %v samples = %v, want exactly one at %d", sd.Labels, sd.Samples, base)
		}
	}
}

// Every replica stores the generation the gateway stamped, so replicas agree
// on which of two writes is newer; an overwrite sent through a second gateway
// wins on every replica.
func TestReplicasStoreTheGatewaysGeneration(t *testing.T) {
	c := startClusterRF(t, 3, 3)
	base := time.Now().UnixMilli()
	c.ingest(t, "split_metric", base, 1)
	// The 204 comes at quorum; the third replica's push may land a moment later.
	eventually(t, "every replica holds the write", func() bool {
		for _, u := range c.ingesterURLs {
			if len(metricSelectFrom(t, "ingester", u, base, base)) != 1 {
				return false
			}
		}
		return true
	})
	var gens []int64
	for _, u := range c.ingesterURLs {
		gens = append(gens, metricSelectFrom(t, "ingester", u, base, base)[0].Gen)
	}
	if gens[0] <= 0 || gens[0] != gens[1] || gens[1] != gens[2] {
		t.Fatalf("replicas hold generations %v, want one positive generation everywhere", gens)
	}

	// A second gateway pod over the same ring.
	cfg := *c.gateway.cfg
	cfg.HTTPAddr = freeAddr(t)
	gw2 := &process{t: t, cfg: &cfg}
	gw2.start()
	t.Cleanup(gw2.stop)
	body := fmt.Sprintf(`{"metrics":[{"name":"split_metric","labels":{"run":"split"},"timestamp_ms":%d,"value":2}]}`, base)
	resp, err := httpClient.Post("http://"+cfg.HTTPAddr+"/api/v1/ingest/metrics", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("overwrite through the second gateway = %d", resp.StatusCode)
	}
	if v, code, _ := c.instant(t, "split_metric", base); v != "2" {
		t.Errorf("after the overwrite through the second gateway: %q (%d), want 2", v, code)
	}
	// Each replica's answer is its highest generation (a head read dedups by
	// it); once the third replica's push lands, every one holds the overwrite.
	eventually(t, "every replica's highest generation holds the overwrite", func() bool {
		for _, u := range c.ingesterURLs {
			s := metricSelectFrom(t, "ingester", u, base, base)
			if len(s) != 1 || s[0].Value != 2 {
				return false
			}
		}
		return true
	})
}
