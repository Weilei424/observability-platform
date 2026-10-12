package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/ring"
	"github.com/masonwheeler/observability-platform/internal/rpc"
)

// The store cluster every test here runs on unless noted: 3 ingesters at RF 3,
// 4 stores at R_s 3 (spec section 11).
const (
	scIngesters = 3
	scRF        = 3
	scStores    = 4
	scRS        = 3
)

// storeNames returns n series names with prefix; c.ingestBatch writes each as
// name{run="split"}.
func storeNames(prefix string, n int) []string {
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("%s_%d", prefix, i)
	}
	return names
}

// ingestEach writes names[i] = i at ts, one request each, each required 204.
func (c *cluster) ingestEach(t *testing.T, names []string, ts int64) {
	t.Helper()
	for i, name := range names {
		// One request per value: ingestBatch writes a single value for all.
		c.ingest(t, name, ts, float64(i))
	}
}

// drainAll drains every ingester, requiring 200, then restarts each (a
// drained ingester refuses writes until it restarts).
func (c *cluster) drainAll(t *testing.T) {
	t.Helper()
	c.drainOnly(t)
	c.restartIngesters(t)
}

// drainOnly drains every ingester, requiring 200, without restarting them:
// a restart would lose the straggler queue.
func (c *cluster) drainOnly(t *testing.T) {
	t.Helper()
	for _, u := range c.ingesterURLs {
		if code, body := postDrain(t, u); code != http.StatusOK {
			t.Fatalf("drain %s = %d: %s", u, code, body)
		}
	}
}

func (c *cluster) restartIngesters(t *testing.T) {
	t.Helper()
	for _, p := range c.ingesters {
		p.stop()
		p.start()
	}
	httpClient.CloseIdleConnections()
}

// assertAllRead fails unless every names[i] reads i at ts through the gateway.
func (c *cluster) assertAllRead(t *testing.T, label string, names []string, ts int64) {
	t.Helper()
	for i, name := range names {
		if v, code, errType := c.instant(t, name, ts); code != http.StatusOK || v != fmt.Sprint(i) {
			t.Fatalf("%s: %s = %q (%d %q), want %d", label, name, v, code, errType, i)
		}
	}
}

// storeHolds reports whether the store at storeURL holds any sample of
// name{run="split"}, read straight from its own select route.
func storeHolds(t *testing.T, storeURL, name string) bool {
	t.Helper()
	cl, err := rpc.NewClient("store", storeURL)
	if err != nil {
		t.Fatal(err)
	}
	sds, err := rpc.NewMetricsSource(cl).Select(context.Background(), metrics.SelectParams{
		Selector: metrics.Selector{MetricName: name},
		MinT:     math.MinInt64,
		MaxT:     math.MaxInt64,
	})
	if err != nil {
		t.Fatalf("select %s on %s: %v", name, storeURL, err)
	}
	for _, sd := range sds {
		if len(sd.Samples) > 0 {
			return true
		}
	}
	return false
}

// holders returns the stores among urls that hold name.
func holders(t *testing.T, urls []string, name string) []string {
	t.Helper()
	var out []string
	for _, u := range urls {
		if storeHolds(t, u, name) {
			out = append(out, u)
		}
	}
	return out
}

// replicaSet is name{run="split"}'s R_s stores on the ring over urls, sorted.
func replicaSet(t *testing.T, urls []string, name string, rs int) []string {
	t.Helper()
	r, err := ring.New(urls)
	if err != nil {
		t.Fatal(err)
	}
	reps, err := r.Replicas(seriesKey(t, name), rs)
	if err != nil {
		t.Fatal(err)
	}
	reps = slices.Clone(reps)
	slices.Sort(reps)
	return reps
}

// streamReplicaSet is {service=svc}'s R_s stores on the ring over urls.
func streamReplicaSet(t *testing.T, urls []string, svc string, rs int) []string {
	t.Helper()
	r, err := ring.New(urls)
	if err != nil {
		t.Fatal(err)
	}
	l, err := logs.NewStreamLabels(map[string]string{"service": svc})
	if err != nil {
		t.Fatal(err)
	}
	reps, err := r.Replicas(l.Hash(), rs)
	if err != nil {
		t.Fatal(err)
	}
	return reps
}

// storeBlockCount is how many metrics blocks the store at storeURL lists.
func storeBlockCount(t *testing.T, storeURL string) int {
	t.Helper()
	resp, err := httpClient.Get(storeURL + "/internal/v1/metrics/blocks")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body struct {
		Blocks []json.RawMessage `json:"blocks"`
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("blocks on %s = %d: %s", storeURL, resp.StatusCode, b)
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode blocks on %s: %v", storeURL, err)
	}
	return len(body.Blocks)
}

// pushLines pushes n lines for svc at tsNs, tsNs+1, ... and returns them.
func (c *cluster) pushLines(t *testing.T, svc string, tsNs int64, n int) map[int64]string {
	t.Helper()
	want := make(map[int64]string, n)
	for i := range n {
		line := fmt.Sprintf("%s line %d", svc, i)
		c.pushLog(t, svc, tsNs+int64(i), line)
		want[tsNs+int64(i)] = line
	}
	return want
}

// Flushed data is sharded: each series lands on exactly its R_s stores on the
// store ring, and reads back through the gateway.
func TestStoreClusterFlushedDataIsShardedAndReadBack(t *testing.T) {
	c := startClusterStores(t, scIngesters, scRF, scStores, scRS)
	base := time.Now().UnixMilli()
	names := storeNames("shard_metric", 30)
	c.ingestEach(t, names, base)
	c.drainAll(t)
	c.assertAllRead(t, "after the drain", names, base)

	held := make(map[string]int, len(c.storeURLs))
	for _, name := range names {
		want := replicaSet(t, c.storeURLs, name, scRS)
		got := holders(t, c.storeURLs, name)
		slices.Sort(got)
		if !slices.Equal(got, want) {
			t.Fatalf("%s is held by %v, want exactly its replica set %v", name, got, want)
		}
		for _, u := range got {
			held[u]++
		}
	}
	for _, u := range c.storeURLs {
		if held[u] == 0 || held[u] == len(names) {
			t.Errorf("store %s holds %d of %d series; want a share, not none or all", u, held[u], len(names))
		}
	}
}

// One store down: the flush meets quorum (the drain answers 200), reads are
// complete for metrics and logs, and once the store returns the straggler
// queue fills in the copies it missed.
func TestStoreClusterOneStoreDown(t *testing.T) {
	c := startClusterStores(t, scIngesters, scRF, scStores, scRS)
	down := c.storeURLs[0]
	base := time.Now().UnixMilli()
	names := storeNames("one_down", 30)
	var owned []string
	for _, name := range names {
		if slices.Contains(replicaSet(t, c.storeURLs, name, scRS), down) {
			owned = append(owned, name)
		}
	}
	if len(owned) == 0 {
		t.Fatal("no series out of 30 has the down store in its replica set")
	}
	svc := ""
	for i := 0; svc == ""; i++ {
		if cand := fmt.Sprintf("one-down-%d", i); slices.Contains(streamReplicaSet(t, c.storeURLs, cand, scRS), down) {
			svc = cand
		}
	}

	c.stores[0].stop()
	c.ingestEach(t, names, base)
	tsNs := base * 1_000_000
	wantLogs := c.pushLines(t, svc, tsNs, 5)
	c.drainOnly(t) // 200: every key met its quorum on the other stores
	c.assertAllRead(t, "one store down", names, base)
	status, entries, body := c.lokiQueryRange(t, fmt.Sprintf(`{service=%q}`, svc), tsNs-1, tsNs+10)
	if status != http.StatusOK {
		t.Fatalf("loki query_range with one store down = %d: %s", status, body)
	}
	assertLogSet(t, "one store down", entries, wantLogs)

	c.stores[0].start()
	httpClient.CloseIdleConnections()
	eventually(t, "the straggler queue to fill the returned store", func() bool {
		for _, name := range owned {
			if !storeHolds(t, down, name) {
				return false
			}
		}
		return true
	})
	for _, name := range names { // and nothing it does not own
		if !slices.Contains(owned, name) && storeHolds(t, down, name) {
			t.Errorf("returned store holds %s, outside its replica set", name)
		}
	}
	c.assertAllRead(t, "after the store returned", names, base)
}

// Two stores sharing a key's replica set down: the flush fails its quorum (the
// drain answers 503, the head keeps the data) and reads answer 503, for
// metrics and logs. Once they return, a drain succeeds and everything reads.
func TestStoreClusterTwoStoresDown(t *testing.T) {
	c := startClusterStores(t, scIngesters, scRF, scStores, scRS)
	downA, downB := c.storeURLs[0], c.storeURLs[1]
	base := time.Now().UnixMilli()
	names := storeNames("two_down", 30)
	both := ""
	for _, name := range names {
		if reps := replicaSet(t, c.storeURLs, name, scRS); slices.Contains(reps, downA) && slices.Contains(reps, downB) {
			both = name
			break
		}
	}
	if both == "" {
		t.Fatal("no series out of 30 has both down stores in its replica set")
	}
	svc := ""
	for i := 0; svc == ""; i++ {
		cand := fmt.Sprintf("two-down-%d", i)
		if reps := streamReplicaSet(t, c.storeURLs, cand, scRS); slices.Contains(reps, downA) && slices.Contains(reps, downB) {
			svc = cand
		}
	}

	c.stores[0].stop()
	c.stores[1].stop()
	c.ingestEach(t, names, base)
	tsNs := base * 1_000_000
	wantLogs := c.pushLines(t, svc, tsNs, 5)
	for _, u := range c.ingesterURLs {
		if code, body := postDrain(t, u); code != http.StatusServiceUnavailable {
			t.Fatalf("drain %s with two stores down = %d %s, want 503", u, code, body)
		}
	}
	if _, code, errType := c.instant(t, both, base); code != http.StatusServiceUnavailable || errType != "unavailable" {
		t.Fatalf("read of %s (both down stores in its replica set) = %d %q, want 503 unavailable", both, code, errType)
	}
	if code, _, body := c.lokiQueryRange(t, fmt.Sprintf(`{service=%q}`, svc), tsNs-1, tsNs+10); code != http.StatusServiceUnavailable {
		t.Fatalf("loki query_range with two stores down = %d %s, want 503", code, body)
	}

	c.stores[0].start()
	c.stores[1].start()
	httpClient.CloseIdleConnections()
	c.drainAll(t) // the head kept the batch: this drain flushes it
	c.assertAllRead(t, "after the stores returned", names, base)
	status, entries, body := c.lokiQueryRange(t, fmt.Sprintf(`{service=%q}`, svc), tsNs-1, tsNs+10)
	if status != http.StatusOK {
		t.Fatalf("loki query_range after the stores returned = %d: %s", status, body)
	}
	assertLogSet(t, "after the stores returned", entries, wantLogs)
	for _, u := range replicaSet(t, c.storeURLs, both, scRS) {
		if !storeHolds(t, u, both) {
			t.Errorf("%s missing from %s in its replica set after the retried drain", both, u)
		}
	}
}

// The compactor compacts every store: each drain puts a block on each store,
// and with the compactor started every store's blocks in the window merge.
func TestStoreClusterCompactsEveryStore(t *testing.T) {
	c := startClusterStores(t, scIngesters, scRF, scStores, scRS)
	c.compactor.stop()                                       // hold compaction until every store has blocks to merge
	base := (time.Now().UnixMilli() / 7_200_000) * 7_200_000 // one 2h window
	names := storeNames("compact_metric", 30)
	c.ingestEach(t, names, base)
	c.drainAll(t) // one drain per ingester: three blocks per store at RF 3

	before := make(map[string]int, len(c.storeURLs))
	for _, u := range c.storeURLs {
		before[u] = storeBlockCount(t, u)
		if before[u] < 2 {
			t.Fatalf("store %s has %d blocks before compaction, want at least 2", u, before[u])
		}
	}
	c.compactor.start()
	for _, u := range c.storeURLs {
		eventually(t, "compaction on "+u, func() bool { return storeBlockCount(t, u) < before[u] })
	}
	c.assertAllRead(t, "after compaction", names, base)
	for _, name := range names {
		got := holders(t, c.storeURLs, name)
		slices.Sort(got)
		if want := replicaSet(t, c.storeURLs, name, scRS); !slices.Equal(got, want) {
			t.Fatalf("after compaction %s is held by %v, want %v", name, got, want)
		}
	}
}

// Adding a store (spec section 8.1): the querier and compactor first, then
// the ingesters one at a time. Older data stays readable on its old stores;
// new data lands on its replica set on the four-store ring.
func TestStoreClusterAddStore(t *testing.T) {
	c := startClusterStores(t, scIngesters, scRF, 3, scRS)
	base := time.Now().UnixMilli()
	oldNames := storeNames("add_old", 20)
	c.ingestEach(t, oldNames, base)
	c.drainAll(t)

	p, added := newStore(t)
	p.start()
	t.Cleanup(p.stop)
	c.stores, c.storeURLs = append(c.stores, p), append(c.storeURLs, added)
	setStores(c.querier, c.storeURLs)
	setStores(c.compactor, c.storeURLs)
	c.assertAllRead(t, "after the querier reads the new store", oldNames, base)
	for _, ing := range c.ingesters {
		setStores(ing, c.storeURLs)
	}

	newNames := storeNames("add_new", 20)
	c.ingestEach(t, newNames, base)
	c.drainAll(t)
	c.assertAllRead(t, "old series after the add", oldNames, base)
	c.assertAllRead(t, "new series after the add", newNames, base)
	onAdded := 0
	for _, name := range newNames {
		want := replicaSet(t, c.storeURLs, name, scRS)
		got := holders(t, c.storeURLs, name)
		slices.Sort(got)
		if !slices.Equal(got, want) {
			t.Fatalf("%s is held by %v, want its four-store replica set %v", name, got, want)
		}
		if slices.Contains(got, added) {
			onAdded++
		}
	}
	if onAdded == 0 {
		t.Fatal("no new series landed on the added store")
	}
	for _, name := range oldNames {
		if storeHolds(t, added, name) {
			t.Errorf("%s, flushed before the add, is on the added store", name)
		}
	}
}

// Removing a store with a handoff (spec section 8.2): ingesters stop flushing
// to it, it copies its data to every owner on the remaining ring, the querier
// and compactor drop it, and it stops. A read that also skips one more store
// is complete.
func TestStoreClusterRemoveStoreWithHandoff(t *testing.T) {
	c := startClusterStores(t, scIngesters, scRF, scStores, scRS)
	base := time.Now().UnixMilli()
	names := storeNames("remove_metric", 30)
	c.ingestEach(t, names, base)
	tsNs := base * 1_000_000
	wantLogs := c.pushLines(t, "remove-svc", tsNs, 5)
	c.drainAll(t)

	leaving, remaining := c.storeURLs[3], slices.Clone(c.storeURLs[:3])
	// Series the handoff must copy: held by the leaving store and missing from
	// a remaining one (every key misses exactly one of the four stores).
	var needCopy []string
	for _, name := range names {
		if reps := replicaSet(t, c.storeURLs, name, scRS); slices.Contains(reps, leaving) {
			needCopy = append(needCopy, name)
		}
	}
	if len(needCopy) == 0 {
		t.Fatal("no series out of 30 is on the leaving store")
	}

	for _, ing := range c.ingesters {
		setStores(ing, remaining)
	}
	body, _ := json.Marshal(map[string]any{"stores": remaining, "replication_factor": scRS})
	resp, err := httpClient.Post(leaving+"/internal/v1/handoff", "application/json", strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("handoff = %d: %s", resp.StatusCode, b)
	}
	var counts struct {
		Series  int `json:"series"`
		Streams int `json:"streams"`
	}
	if err := json.Unmarshal(b, &counts); err != nil {
		t.Fatalf("decode handoff answer %s: %v", b, err)
	}
	if counts.Series != len(needCopy) {
		t.Errorf("handoff sent %d series, want the %d on the leaving store", counts.Series, len(needCopy))
	}
	// Every key on the leaving store is now on every remaining store (the
	// three-store ring at R_s 3 gives each key all three).
	for _, name := range needCopy {
		got := holders(t, remaining, name)
		slices.Sort(got)
		want := slices.Clone(remaining)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Fatalf("after the handoff %s is held by %v, want every remaining store %v", name, got, want)
		}
	}

	setStores(c.querier, remaining)
	setStores(c.compactor, remaining)
	c.stores[3].stop()
	c.stores[0].stop() // a read that also skips one store
	c.assertAllRead(t, "after the removal with one more store down", names, base)
	status, entries, rawBody := c.lokiQueryRange(t, `{service="remove-svc"}`, tsNs-1, tsNs+10)
	if status != http.StatusOK {
		t.Fatalf("loki query_range after the removal = %d: %s", status, rawBody)
	}
	assertLogSet(t, "after the removal", entries, wantLogs)
}

// The querier reads its ingesters in parallel: three ingesters each slowed by
// 300ms on their select route cost one delay, not three.
func TestStoreClusterReadsIngestersInParallel(t *testing.T) {
	const delay = 300 * time.Millisecond
	c := startClusterStores(t, scIngesters, scRF, scStores, scRS)
	base := time.Now().UnixMilli()
	c.ingest(t, "parallel_metric", base, 9)
	for _, p := range c.ingesters {
		p.stop()
		p.selectDelay = delay
		p.start()
	}
	httpClient.CloseIdleConnections()
	if v, code, _ := c.instant(t, "parallel_metric", base); v != "9" { // warm the connections
		t.Fatalf("warm-up read = %q (%d), want 9", v, code)
	}
	start := time.Now()
	v, code, _ := c.instant(t, "parallel_metric", base)
	took := time.Since(start)
	if v != "9" {
		t.Fatalf("timed read = %q (%d), want 9", v, code)
	}
	if took < delay {
		t.Fatalf("timed read took %v, under one delay of %v: the delay is not applied", took, delay)
	}
	if took >= 700*time.Millisecond {
		t.Fatalf("timed read took %v, want under 700ms: three %v ingester reads ran sequentially", took, delay)
	}
}

// Stores keep their data across a restart.
func TestStoreClusterRestartKeepsStores(t *testing.T) {
	c := startClusterStores(t, scIngesters, scRF, scStores, scRS)
	base := time.Now().UnixMilli()
	names := storeNames("restart_metric", 30)
	c.ingestEach(t, names, base)
	tsNs := base * 1_000_000
	wantLogs := c.pushLines(t, "restart-svc", tsNs, 5)
	c.drainAll(t)
	for _, p := range c.stores {
		p.stop()
		p.start()
	}
	httpClient.CloseIdleConnections()
	c.assertAllRead(t, "after every store restarted", names, base)
	status, entries, body := c.lokiQueryRange(t, `{service="restart-svc"}`, tsNs-1, tsNs+10)
	if status != http.StatusOK {
		t.Fatalf("loki query_range after every store restarted = %d: %s", status, body)
	}
	assertLogSet(t, "after every store restarted", entries, wantLogs)
	for _, name := range names {
		got := holders(t, c.storeURLs, name)
		slices.Sort(got)
		if want := replicaSet(t, c.storeURLs, name, scRS); !slices.Equal(got, want) {
			t.Fatalf("after the restart %s is held by %v, want %v", name, got, want)
		}
	}
}
