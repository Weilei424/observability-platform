package integration_test

import (
	"fmt"
	"io"
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
	// This checks the retried value only; a duplicate same-value rewrite at one
	// timestamp is undetectable here. The log case below is the read-once check.
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

// A write sent straight to a NON-owner ingester's public route skips the ring
// but is still read: reads cover every member.
func TestRingClusterDirectIngesterWriteIsRead(t *testing.T) {
	c := startClusterN(t, 3)
	r, err := ring.New(c.ingesterURLs)
	if err != nil {
		t.Fatal(err)
	}
	owner := r.Owner(seriesKey(t, "direct_metric"))
	var target string
	for _, u := range c.ingesterURLs {
		if u != owner {
			target = u
			break
		}
	}
	base := time.Now().UnixMilli()
	body := fmt.Sprintf(`{"metrics":[{"name":"direct_metric","labels":{"run":"split"},"timestamp_ms":%d,"value":5}]}`, base)
	resp, err := httpClient.Post(target+"/api/v1/ingest/metrics", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("direct write to non-owner %s = %d: %s", target, resp.StatusCode, b)
	}
	if v, _, _ := c.instant(t, "direct_metric", base); v != "5" {
		t.Fatalf("direct write to non-owner read = %q, want 5", v)
	}
}

// TestRingClusterStagedMembershipChangeNeverHidesWrites drives the Helm chart's
// staged procedure in process, with writes in every window. Adding: the
// querier reads the new ingester before the gateway writes to it. Removing:
// the gateway stops writing to it, it stops (its final flush reaches the
// store) while reads fail closed with 503, then the querier drops it. At no
// stage may a read answer 200 with an acknowledged write missing.
func TestRingClusterStagedMembershipChangeNeverHidesWrites(t *testing.T) {
	c := startClusterN(t, 3)
	three := slices.Clone(c.ingesterURLs)
	base := time.Now().UnixMilli()
	var written []string
	write := func(stage string, n int) {
		t.Helper()
		var names []string
		for i := range n {
			names = append(names, fmt.Sprintf("staged_%s_%d", stage, i))
		}
		if code := c.ingestBatch(t, names, base, 1); code != http.StatusNoContent {
			t.Fatalf("%s: write = %d, want 204", stage, code)
		}
		written = append(written, names...)
	}
	readAll := func(stage string) {
		t.Helper()
		for _, name := range written {
			if v, code, _ := c.instant(t, name, base); v != "1" {
				t.Fatalf("%s: %s read = %q (%d); an acknowledged write is hidden", stage, name, v, code)
			}
		}
	}

	write("before", 30)
	added := c.addIngester(t)
	four := append(slices.Clone(three), added)

	// Add, stage 1: the querier reads four members, the gateway still writes to three.
	restartWithMembers(c.querier, four)
	write("querier_first", 30)
	readAll("add stage 1")
	// Add, stage 2: the gateway writes to all four.
	restartWithMembers(c.gateway, four)
	write("both_four", 60)
	if v := metricValue(t, added, "obs_samples_ingested_total"); v == 0 {
		t.Fatal("the added ingester took no writes after the gateway moved to four members")
	}
	readAll("add stage 2")

	// Remove, stage 1: the gateway stops writing to the added ingester.
	restartWithMembers(c.gateway, three)
	write("gateway_three", 30)
	readAll("remove stage 1")
	// Remove, stage 2: the ingester stops; its final flush reaches the store.
	// The querier still lists it, so reads fail closed rather than miss data.
	c.ingesterByURL(t, added).stop()
	if _, code, errType := c.instant(t, written[0], base); code != http.StatusServiceUnavailable || errType != "unavailable" {
		t.Fatalf("remove stage 2: read = %d %q, want 503 unavailable while a listed ingester is down", code, errType)
	}
	// Remove, stage 3: the querier drops it; every write is still read.
	restartWithMembers(c.querier, three)
	c.ingesterURLs = three
	readAll("remove stage 3")
}
