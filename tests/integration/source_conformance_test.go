package integration_test

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/rpc"
	"github.com/masonwheeler/observability-platform/internal/storage/index"
	"github.com/masonwheeler/observability-platform/internal/storage/wal"
)

// The same data, three ways: all-in-one's stores; the split's ingester head and
// store merged in-process; and that same merge over HTTP. Every answer must be
// identical — that is what "the transport is invisible" means.
type conformance struct {
	aioMetrics, splitMetrics, remoteMetrics metrics.Source
	aioLogs, splitLogs, remoteLogs          logs.Source
}

// genFloor starts both sides near 2^62, so generations cross the wire at the
// top of their range.
const genFloor = int64(1)<<62 - 1_000_000

func newConformance(t *testing.T) conformance {
	t.Helper()
	aioDir, ingDir, storeDir := t.TempDir(), t.TempDir(), t.TempDir()

	aioBlocks, err := metrics.NewBlockStore(aioDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = aioBlocks.Close() })
	aioBlocks.MemStore().EnsureGenFloor(genFloor)

	// Write through a real metrics.WALStore, the same wrapper both all-in-one
	// and the ingester put in front of their head, rather than calling
	// aioBlocks.Append/FlushBlock directly: that keeps the "all-in-one"
	// reference on the real production write path (WAL durability and the
	// generation-exhaustion preflight included), not a shortcut around it.
	aioWAL, err := wal.Open(filepath.Join(aioDir, "metrics", "wal"), 1<<20, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = aioWAL.Close() })
	aioStore := metrics.NewWALStore(aioWAL, aioBlocks, aioDir)

	storeBlocks, err := metrics.NewBlockStore(storeDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = storeBlocks.Close() })
	if err := os.MkdirAll(filepath.Join(ingDir, "metrics"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(metrics.GenFloorPath(ingDir), []byte(strconv.FormatInt(genFloor, 10)), 0o644); err != nil {
		t.Fatal(err)
	}
	head, err := metrics.OpenHeadStore(ingDir, storeBlocks, metrics.HeadStoreOptions{})
	if err != nil {
		t.Fatal(err)
	}

	aioLogs, err := logs.NewStore(filepath.Join(aioDir, "logs", "wal"), filepath.Join(aioDir, "logs", "chunks"),
		filepath.Join(aioDir, "logs", "index"), 1<<20, 1, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = aioLogs.Close() })
	chunks, err := logs.OpenChunkStore(filepath.Join(storeDir, "logs", "chunks"), filepath.Join(storeDir, "logs", "index"))
	if err != nil {
		t.Fatal(err)
	}
	logHead, err := logs.OpenHead(filepath.Join(ingDir, "logs", "wal"), 1<<20, 1, 1<<30, chunks, logs.HeadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = logHead.Close() })

	// Identical writes to both sides, a flush part-way, then overwrites.
	r := rand.New(rand.NewPCG(42, 42))
	var series []metrics.Labels
	for _, job := range []string{"a", "b"} {
		for _, city := range []string{"東京", "São Paulo"} {
			l, err := metrics.NewLabels(map[string]string{"__name__": "conf", "job": job, "city": city})
			if err != nil {
				t.Fatal(err)
			}
			series = append(series, l)
		}
	}
	streams := []logs.StreamLabels{}
	for _, svc := range []string{"api", "wörker"} {
		l, err := logs.NewStreamLabels(map[string]string{"service": svc})
		if err != nil {
			t.Fatal(err)
		}
		streams = append(streams, l)
	}
	value := func() float64 {
		switch r.IntN(20) {
		case 0:
			return math.NaN()
		case 1:
			return math.Inf(1)
		case 2:
			return math.Inf(-1)
		default:
			return float64(r.IntN(1000)) / 7
		}
	}
	round := func(n int) {
		for range n {
			l := series[r.IntN(len(series))]
			ts := int64(r.IntN(3*3600)) * 1000
			v := value()
			if err := aioStore.Append(l, ts, v); err != nil {
				t.Fatal(err)
			}
			if err := head.Append(l, ts, v); err != nil {
				t.Fatal(err)
			}
			sl := streams[r.IntN(len(streams))]
			tsNs := int64(r.IntN(3*3600)+1) * 1_000_000_000
			line := fmt.Sprintf("line %d \"quoted\" 🚀", r.IntN(50))
			if err := aioLogs.Append(sl, tsNs, line); err != nil {
				t.Fatal(err)
			}
			if err := logHead.Append(sl, tsNs, line); err != nil {
				t.Fatal(err)
			}
		}
	}
	round(1500)
	if _, err := aioStore.FlushBlock(); err != nil {
		t.Fatal(err)
	}
	if _, err := head.FlushBlock(); err != nil {
		t.Fatal(err)
	}
	if err := aioLogs.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := logHead.Flush(); err != nil {
		t.Fatal(err)
	}
	round(700)

	serve := func(m metrics.Source, l logs.Source) *rpc.Client {
		router := chi.NewRouter()
		router.Route("/internal/v1", func(r chi.Router) { rpc.MountReads(r, m, l) })
		srv := httptest.NewServer(router)
		t.Cleanup(srv.Close)
		c, err := rpc.NewClient("peer", srv.URL)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	ing := serve(head, logs.AsSource(logHead))
	st := serve(storeBlocks, logs.AsSource(chunks))

	return conformance{
		aioMetrics:    aioStore,
		splitMetrics:  metrics.Merge(head, storeBlocks),
		remoteMetrics: metrics.Merge(rpc.NewMetricsSource(ing), rpc.NewMetricsSource(st)),
		aioLogs:       logs.AsSource(aioLogs),
		splitLogs:     logs.Merge(logs.AsSource(logHead), logs.AsSource(chunks)),
		remoteLogs:    logs.Merge(rpc.NewLogsSource(ing), rpc.NewLogsSource(st)),
	}
}

func renderSeries(t *testing.T, src metrics.Source, p metrics.SelectParams) string {
	t.Helper()
	sds, err := src.Select(context.Background(), p)
	if err != nil {
		t.Fatalf("Select(%+v): %v", p, err)
	}
	rows := make([]string, len(sds))
	for i, sd := range sds {
		var b strings.Builder
		m := sd.Labels.Map()
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(&b, "%s=%q ", k, m[k])
		}
		if sd.Anchor != nil {
			fmt.Fprintf(&b, "anchor %d/%s/%d ", sd.Anchor.TimestampMs, strconv.FormatFloat(sd.Anchor.Value, 'g', -1, 64), sd.Anchor.Gen)
		}
		for _, s := range sd.Samples {
			fmt.Fprintf(&b, "%d/%s/%d ", s.TimestampMs, strconv.FormatFloat(s.Value, 'g', -1, 64), s.Gen)
		}
		rows[i] = b.String()
	}
	sort.Strings(rows)
	return strings.Join(rows, "\n")
}

func renderStreams(t *testing.T, src logs.Source, matchers []index.Pair, minTs, maxTs int64) string {
	t.Helper()
	sds, err := src.SelectStreams(context.Background(), matchers, minTs, maxTs)
	if err != nil {
		t.Fatalf("SelectStreams: %v", err)
	}
	var b strings.Builder
	for _, sd := range sds {
		fmt.Fprintf(&b, "%v:", sd.Labels.Map())
		for _, e := range sd.Entries {
			fmt.Fprintf(&b, " %d/%q", e.TimestampNs, e.Line)
		}
		b.WriteString("\n")
	}
	return b.String()
}

func TestEverySourceAgrees(t *testing.T) {
	c := newConformance(t)
	sels := []metrics.Selector{
		{MetricName: "conf"},
		{MetricName: "conf", Matchers: []metrics.Matcher{{Name: "city", Value: "東京"}}},
		{Matchers: []metrics.Matcher{{Name: "job", Value: "b"}}},
	}
	r := rand.New(rand.NewPCG(7, 7))
	for q := range 60 {
		minT := int64(r.IntN(4*3600)-1800) * 1000
		p := metrics.SelectParams{Selector: sels[r.IntN(len(sels))], MinT: minT, MaxT: minT + int64(r.IntN(3600))*1000}
		switch r.IntN(4) {
		case 0:
			p.Anchor = true
		case 1:
			p.SeriesOnly = true
		case 2:
			p.SeriesOnly, p.AnyTime = true, true
		}
		want := renderSeries(t, c.aioMetrics, p)
		for name, src := range map[string]metrics.Source{"split": c.splitMetrics, "remote": c.remoteMetrics} {
			if got := renderSeries(t, src, p); got != want {
				t.Fatalf("query %d %+v: %s differs from all-in-one\n got  %s\n want %s", q, p, name, got, want)
			}
		}
	}
	// Label discovery must agree across every source too, for both families and
	// both methods (names and values) — not metrics label values alone. Every
	// call's error is checked: a discarded error here would let a source that
	// fails outright pass as "empty and therefore equal".
	wantMetricNames, err := c.aioMetrics.SelectLabelNames(context.Background())
	if err != nil {
		t.Fatalf("aio metrics SelectLabelNames: %v", err)
	}
	for name, src := range map[string]metrics.Source{"split": c.splitMetrics, "remote": c.remoteMetrics} {
		got, err := src.SelectLabelNames(context.Background())
		if err != nil {
			t.Fatalf("%s metrics SelectLabelNames: %v", name, err)
		}
		if fmt.Sprint(got) != fmt.Sprint(wantMetricNames) {
			t.Fatalf("%s metrics label names = %v, want %v", name, got, wantMetricNames)
		}
	}
	for _, name := range []string{"job", "city", "__name__"} {
		want, err := c.aioMetrics.SelectLabelValues(context.Background(), name)
		if err != nil {
			t.Fatalf("aio metrics SelectLabelValues(%s): %v", name, err)
		}
		for label, src := range map[string]metrics.Source{"split": c.splitMetrics, "remote": c.remoteMetrics} {
			got, err := src.SelectLabelValues(context.Background(), name)
			if err != nil {
				t.Fatalf("%s metrics SelectLabelValues(%s): %v", label, name, err)
			}
			if fmt.Sprint(got) != fmt.Sprint(want) {
				t.Fatalf("%s metrics label values for %s = %v, want %v", label, name, got, want)
			}
		}
	}

	wantLogNames, err := c.aioLogs.SelectLabelNames(context.Background())
	if err != nil {
		t.Fatalf("aio logs SelectLabelNames: %v", err)
	}
	for name, src := range map[string]logs.Source{"split": c.splitLogs, "remote": c.remoteLogs} {
		got, err := src.SelectLabelNames(context.Background())
		if err != nil {
			t.Fatalf("%s logs SelectLabelNames: %v", name, err)
		}
		if fmt.Sprint(got) != fmt.Sprint(wantLogNames) {
			t.Fatalf("%s logs label names = %v, want %v", name, got, wantLogNames)
		}
	}
	for _, name := range []string{"service"} {
		want, err := c.aioLogs.SelectLabelValues(context.Background(), name)
		if err != nil {
			t.Fatalf("aio logs SelectLabelValues(%s): %v", name, err)
		}
		for label, src := range map[string]logs.Source{"split": c.splitLogs, "remote": c.remoteLogs} {
			got, err := src.SelectLabelValues(context.Background(), name)
			if err != nil {
				t.Fatalf("%s logs SelectLabelValues(%s): %v", label, name, err)
			}
			if fmt.Sprint(got) != fmt.Sprint(want) {
				t.Fatalf("%s logs label values for %s = %v, want %v", label, name, got, want)
			}
		}
	}

	for q := range 30 {
		minTs := int64(r.IntN(3*3600)) * 1_000_000_000
		maxTs := minTs + int64(r.IntN(3600))*1_000_000_000
		var matchers []index.Pair
		if q%2 == 0 {
			matchers = []index.Pair{{Name: "service", Value: "wörker"}}
		}
		want := renderStreams(t, c.aioLogs, matchers, minTs, maxTs)
		for name, src := range map[string]logs.Source{"split": c.splitLogs, "remote": c.remoteLogs} {
			if got := renderStreams(t, src, matchers, minTs, maxTs); got != want {
				t.Fatalf("logs query %d: %s differs from all-in-one\n got  %s\n want %s", q, name, got, want)
			}
		}
	}
}
