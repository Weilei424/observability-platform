package integration_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/ring"
	"github.com/masonwheeler/observability-platform/internal/rpc"
	"github.com/masonwheeler/observability-platform/internal/storage/index"
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
	// Remove, stage 2: drain it and wait for the acknowledgment — a 200 means
	// its whole head reached the store — then stop it. The querier still lists
	// it, so reads fail closed rather than miss data.
	if code, body := postDrain(t, added); code != http.StatusOK {
		t.Fatalf("remove stage 2: drain = %d %s, want 200", code, body)
	}
	c.ingesterByURL(t, added).stop()
	if _, code, errType := c.instant(t, written[0], base); code != http.StatusServiceUnavailable || errType != "unavailable" {
		t.Fatalf("remove stage 2: read = %d %q, want 503 unavailable while a listed ingester is down", code, errType)
	}
	// Remove, stage 3: the querier drops it; every write is still read.
	restartWithMembers(c.querier, three)
	c.ingesterURLs = three
	readAll("remove stage 3")
}

// postDrain calls an ingester's drain route and returns the status and body.
func postDrain(t *testing.T, ingester string) (int, string) {
	t.Helper()
	resp, err := httpClient.Post(ingester+"/internal/v1/drain", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	return resp.StatusCode, string(body)
}

// The drain acknowledges only what reached the store: with the store down it
// answers 503 and the data stays readable from the ingester; once the store is
// back it answers 200, and the data is in the store.
func TestRingClusterDrainAcknowledgesOnlyWhatReachedTheStore(t *testing.T) {
	c := startClusterN(t, 1)
	base := time.Now().UnixMilli()
	c.ingest(t, "split_metric", base, 4)
	c.pushLog(t, "drain-svc", base*1e6, "drained line")

	c.store.stop()
	if code, body := postDrain(t, c.ingesterURLs[0]); code != http.StatusServiceUnavailable {
		t.Fatalf("drain with the store down = %d %s, want 503", code, body)
	}
	c.store.start()
	if v, code, _ := c.instant(t, "split_metric", base); v != "4" {
		t.Fatalf("after a failed drain: read = %q (%d), want 4 still readable", v, code)
	}
	if code, body := postDrain(t, c.ingesterURLs[0]); code != http.StatusOK {
		t.Fatalf("drain with the store back = %d %s, want 200", code, body)
	}
	// metricSelectFrom reads split_metric straight from the store.
	if got := metricSelectFrom(t, "store", c.storeURL, base, base); len(got) != 1 || got[0].Value != 4 {
		t.Fatalf("store holds %+v after a 200 drain, want the one sample", got)
	}
	_, entries, _ := c.lokiQueryRange(t, `{service="drain-svc"}`, base*1e6-1, base*1e6+1)
	if len(entries) != 1 {
		t.Fatalf("drained line read back %d times, want once", len(entries))
	}
}

// A drain's 200 is a write barrier: writes keep arriving while it runs, and
// every one the cluster acknowledged with a 204 is in the store once the
// drain answers 200, while every write after it is refused with a 503. Without
// the barrier, a write landing after the metrics flush but before the answer
// would sit only in the ingester's WAL behind the 200.
func TestRingClusterDrainIsAWriteBarrier(t *testing.T) {
	c := startClusterN(t, 1)
	base := time.Now().UnixMilli()

	post := func(path, body string) int {
		resp, err := httpClient.Post(c.gatewayURL+path, "application/json", strings.NewReader(body))
		if err != nil {
			return 0
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		return resp.StatusCode
	}
	type result struct {
		acked   []int64 // timestamps answered 204
		refused int     // answered 503 after the drain's 200
		other   []int   // anything else
	}
	drained := make(chan struct{})
	var started sync.WaitGroup
	write := func(send func(i int64) int, out *result, done *sync.WaitGroup) {
		defer done.Done()
		afterDrain := 0
		for i := int64(0); afterDrain < 20; i++ {
			select {
			case <-drained:
				afterDrain++
			default:
			}
			drainedBefore := afterDrain > 0
			switch code := send(i); {
			case code == http.StatusNoContent:
				if drainedBefore {
					out.other = append(out.other, code) // acknowledged after the 200
				}
				out.acked = append(out.acked, i)
				if len(out.acked) == 20 {
					started.Done()
				}
			case code == http.StatusServiceUnavailable && drainedBefore:
				out.refused++
			case code == http.StatusServiceUnavailable:
				// refused while the drain was running: the barrier is up
			default:
				out.other = append(out.other, code)
			}
		}
	}
	var m, l result
	var done sync.WaitGroup
	started.Add(2)
	done.Add(2)
	go write(func(i int64) int {
		return post("/api/v1/ingest/metrics", fmt.Sprintf(`{"metrics":[{"name":"split_metric","labels":{"run":"split"},"timestamp_ms":%d,"value":%d}]}`, base+i, i))
	}, &m, &done)
	go write(func(i int64) int {
		return post("/loki/api/v1/push", fmt.Sprintf(`{"streams":[{"stream":{"service":"barrier"},"values":[["%d","line %d"]]}]}`, (base+i)*1e6, i))
	}, &l, &done)
	started.Wait() // both writers are well under way

	code, body := postDrain(t, c.ingesterURLs[0])
	close(drained)
	done.Wait()
	if code != http.StatusOK {
		t.Fatalf("drain under concurrent writes = %d %s, want 200", code, body)
	}
	for name, r := range map[string]result{"metrics": m, "logs": l} {
		if len(r.other) != 0 {
			t.Errorf("%s: unexpected answers %v (a 204 here was acknowledged after the drain's 200)", name, r.other)
		}
		if r.refused != 20 {
			t.Errorf("%s: %d of 20 writes after the drain's 200 were refused with 503", name, r.refused)
		}
	}

	// Every acknowledged write is in the store itself, not only in the
	// ingester's head or WAL.
	inStore := map[int64]bool{}
	for _, s := range metricSelectFrom(t, "store", c.storeURL, base, base+1<<20) {
		inStore[s.TimestampMs-base] = true
	}
	for _, i := range m.acked {
		if !inStore[i] {
			t.Errorf("acknowledged sample %d is not in the store after the drain's 200", i)
		}
	}
	cl, err := rpc.NewClient("store", c.storeURL)
	if err != nil {
		t.Fatal(err)
	}
	sds, err := rpc.NewLogsSource(cl).SelectStreams(context.Background(), []index.Pair{{Name: "service", Value: "barrier"}}, base*1e6, (base+1<<20)*1e6)
	if err != nil {
		t.Fatal(err)
	}
	linesInStore := map[string]bool{}
	for _, sd := range sds {
		for _, e := range sd.Entries {
			linesInStore[e.Line] = true
		}
	}
	for _, i := range l.acked {
		if !linesInStore[fmt.Sprintf("line %d", i)] {
			t.Errorf("acknowledged line %d is not in the store after the drain's 200", i)
		}
	}
	t.Logf("acknowledged before the barrier: %d samples, %d lines", len(m.acked), len(l.acked))
}
