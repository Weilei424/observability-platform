package integration_test

import (
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/ring"
)

// seriesKey is the ring key of the series c.ingest writes for name: __name__
// plus run=split.
func seriesKey(t *testing.T, name string) uint64 {
	t.Helper()
	l, err := metrics.NewLabels(map[string]string{"__name__": name, "run": "split"})
	if err != nil {
		t.Fatal(err)
	}
	return l.Hash()
}

func TestRingClusterSpreadsWritesAndReadsEverything(t *testing.T) {
	c := startClusterN(t, 3)
	base := time.Now().UnixMilli()
	for i := range 60 {
		c.ingest(t, fmt.Sprintf("ring_metric_%d", i), base, float64(i))
	}
	for _, u := range c.ingesterURLs {
		if v := metricValue(t, u, "obs_samples_ingested_total"); v == 0 {
			t.Errorf("%s ingested nothing out of 60 series", u)
		}
	}
	for i := range 60 {
		if v, code, _ := c.instant(t, fmt.Sprintf("ring_metric_%d", i), base); v != fmt.Sprint(i) {
			t.Fatalf("ring_metric_%d read = %q (%d), want %d", i, v, code, i)
		}
	}
}

// Adding a fourth ingester moves part of the keyspace and keeps every read
// complete; an overwrite of a moved series lands on the new owner and wins.
// The old owner's graceful restart flushes the old write into a store block with
// its original generation, so the newer write still wins when both are read.
// Exact WAL-replay restoration of generations is proven by the unit tests in
// internal/metrics/walstore_gen_test.go, not here.
func TestRingClusterMembershipChangeAndLastWriteWins(t *testing.T) {
	c := startClusterN(t, 3)
	before, _ := ring.New(c.ingesterURLs)
	base := time.Now().UnixMilli()
	for i := range 60 {
		c.ingest(t, fmt.Sprintf("move_metric_%d", i), base, 1)
	}

	added := c.addIngester(t)
	after, _ := ring.New(append(slices.Clone(c.ingesterURLs), added))
	c.reconfigure(t, append(slices.Clone(c.ingesterURLs), added))

	var moved, oldOwner string
	for i := range 60 {
		name := fmt.Sprintf("move_metric_%d", i)
		k := seriesKey(t, name)
		if before.Owner(k) != after.Owner(k) {
			if after.Owner(k) != added {
				t.Fatalf("%s moved to %s, not to the added ingester", name, after.Owner(k))
			}
			moved, oldOwner = name, before.Owner(k)
		}
	}
	if moved == "" {
		t.Fatal("no series moved to the added ingester out of 60; the ring is not rebalancing")
	}
	for i := range 60 { // every read is complete across the change
		if v, _, _ := c.instant(t, fmt.Sprintf("move_metric_%d", i), base); v != "1" {
			t.Fatalf("move_metric_%d after the change = %q, want 1", i, v)
		}
	}
	c.ingest(t, moved, base, 2) // the overwrite lands on the new owner
	if v, _, _ := c.instant(t, moved, base); v != "2" {
		t.Fatalf("%s after the overwrite = %q, want 2", moved, v)
	}
	old := c.ingesterByURL(t, oldOwner)
	old.stop()
	old.start() // the final flush put the old write in the store with its original generation
	if v, _, _ := c.instant(t, moved, base); v != "2" {
		t.Fatalf("%s after its old owner restarted = %q, want 2: the old write must not outrank the newer one", moved, v)
	}
}

func TestRingClusterOneIngesterDown(t *testing.T) {
	c := startClusterN(t, 3)
	r, _ := ring.New(c.ingesterURLs)
	down := c.ingesters[1]
	downURL := c.ingesterURLs[1]
	base := time.Now().UnixMilli()

	// Pick names by owner so each batch's routing is known.
	var downOwned, healthyOwned []string
	for i := 0; len(downOwned) < 3 || len(healthyOwned) < 3; i++ {
		name := fmt.Sprintf("down_metric_%d", i)
		if r.Owner(seriesKey(t, name)) == downURL {
			downOwned = append(downOwned, name)
		} else {
			healthyOwned = append(healthyOwned, name)
		}
	}
	down.stop()

	if code := c.ingestBatch(t, healthyOwned, base, 1); code != http.StatusNoContent {
		t.Errorf("batch avoiding the down ingester = %d, want 204", code)
	}
	mixed := append(slices.Clone(healthyOwned[:1]), downOwned...)
	if code := c.ingestBatch(t, mixed, base, 7); code != http.StatusServiceUnavailable {
		t.Errorf("batch touching the down ingester = %d, want 503", code)
	}
	if _, code, errType := c.instant(t, healthyOwned[0], base); code != http.StatusServiceUnavailable || errType != "unavailable" {
		t.Errorf("read with one ingester down = %d %q, want 503 unavailable", code, errType)
	}

	down.start()
	if code := c.ingestBatch(t, mixed, base, 7); code != http.StatusNoContent { // the client's retry
		t.Fatalf("retry after recovery = %d, want 204", code)
	}
	for _, name := range mixed {
		if v, _, _ := c.instant(t, name, base); v != "7" {
			t.Errorf("%s after the retry = %q, want 7", name, v)
		}
	}

	// Logs: a stream owned by the down ingester, pushed during the outage (503),
	// then retried, reads back each line once.
	svc := ""
	for i := 0; svc == ""; i++ {
		candidate := fmt.Sprintf("svc-%d", i)
		l, _ := logs.NewStreamLabels(map[string]string{"service": candidate})
		if r.Owner(uint64(logs.StreamIDOf(l))) == downURL {
			svc = candidate
		}
	}
	down.stop()
	if code := c.pushLogCode(t, svc, base*1e6, "retried line"); code != http.StatusServiceUnavailable {
		t.Errorf("log push during the outage = %d, want 503", code)
	}
	down.start()
	c.pushLog(t, svc, base*1e6, "retried line")
	c.pushLog(t, svc, base*1e6, "retried line") // a duplicate retry
	_, entries, _ := c.lokiQueryRange(t, fmt.Sprintf(`{service=%q}`, svc), base*1e6-1, base*1e6+1)
	if len(entries) != 1 {
		t.Errorf("retried line read back %d times, want once", len(entries))
	}
}

// A write sent straight to one ingester's public route skips the ring but is
// still read: reads cover every member.
func TestRingClusterDirectIngesterWriteIsRead(t *testing.T) {
	c := startClusterN(t, 3)
	base := time.Now().UnixMilli()
	body := fmt.Sprintf(`{"metrics":[{"name":"direct_metric","labels":{"run":"split"},"timestamp_ms":%d,"value":5}]}`, base)
	for _, u := range c.ingesterURLs { // every ingester, so at least two are non-owners
		resp, err := httpClient.Post(u+"/api/v1/ingest/metrics", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}
	if v, _, _ := c.instant(t, "direct_metric", base); v != "5" {
		t.Fatalf("direct write read = %q, want 5", v)
	}
}
