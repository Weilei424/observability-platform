package metrics_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/metrics"
)

// orderSource records whether the other side had finished when it was called.
type orderSource struct {
	metrics.Source
	name   string
	events *[]string
}

func (o orderSource) Select(ctx context.Context, p metrics.SelectParams) ([]metrics.SeriesData, error) {
	*o.events = append(*o.events, o.name+" start")
	out, err := o.Source.Select(ctx, p)
	*o.events = append(*o.events, o.name+" end")
	return out, err
}

type failingSource struct{ metrics.Source }

func (failingSource) Select(context.Context, metrics.SelectParams) ([]metrics.SeriesData, error) {
	return nil, errors.New("simulated peer failure")
}

func TestMergeReadsFirstToCompletionBeforeSecond(t *testing.T) {
	var events []string
	a := orderSource{Source: metrics.NewMemoryStore(), name: "first", events: &events}
	b := orderSource{Source: metrics.NewMemoryStore(), name: "second", events: &events}
	if _, err := metrics.Merge(a, b).Select(context.Background(), metrics.SelectParams{MinT: 0, MaxT: 10}); err != nil {
		t.Fatal(err)
	}
	want := []string{"first start", "first end", "second start", "second end"}
	for i := range want {
		if i >= len(events) || events[i] != want[i] {
			t.Fatalf("events = %v, want %v", events, want)
		}
	}
}

func TestMergeCombinesSeriesByGeneration(t *testing.T) {
	m, _ := metrics.NewLabels(map[string]string{"__name__": "m"})
	only, _ := metrics.NewLabels(map[string]string{"__name__": "only_second"})
	older := metrics.NewMemoryStore()
	for _, s := range []struct {
		l  metrics.Labels
		ts int64
		v  float64
	}{{m, 10, 1}, {m, 20, 2}, {only, 5, 50}} {
		if err := older.Append(s.l, s.ts, s.v); err != nil {
			t.Fatal(err)
		}
	}
	newer := metrics.NewMemoryStore()
	newer.EnsureGenFloor(1000) // every write here outranks every write in older
	for _, s := range []struct {
		ts int64
		v  float64
	}{{20, 22}, {30, 3}} {
		if err := newer.Append(m, s.ts, s.v); err != nil {
			t.Fatal(err)
		}
	}

	got, err := metrics.Merge(newer, older).Select(context.Background(), metrics.SelectParams{MinT: 15, MaxT: 40, Anchor: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d series, want 2: m from both sources, only_second from the second", len(got))
	}
	if n, _ := got[0].Labels.Get("__name__"); n != "m" {
		t.Fatalf("first series = %s, want m: first's series come first", n)
	}
	if s := got[0].Samples; len(s) != 2 || s[0].Value != 22 || s[1].Value != 3 {
		t.Fatalf("m samples = %+v, want 22 at 20 (the higher generation) then 3 at 30", s)
	}
	if a := got[0].Anchor; a == nil || a.Value != 1 {
		t.Fatalf("m anchor = %+v, want the sample at 10 from the second source", a)
	}
	if a := got[1].Anchor; a == nil || a.Value != 50 || len(got[1].Samples) != 0 {
		t.Fatalf("only_second = %+v, want only its anchor at 5", got[1])
	}
}

// The generation tie-break must not depend on which side of Merge a sample
// arrives from. TestMergeCombinesSeriesByGeneration puts the higher generation
// in first; this puts it in second.
func TestMergeSecondSourceGenerationWinsAtEqualTimestamp(t *testing.T) {
	m, _ := metrics.NewLabels(map[string]string{"__name__": "m"})
	older := metrics.NewMemoryStore()
	if err := older.Append(m, 20, 2); err != nil {
		t.Fatal(err)
	}
	newer := metrics.NewMemoryStore()
	newer.EnsureGenFloor(1000) // every write here outranks every write in older
	if err := newer.Append(m, 20, 22); err != nil {
		t.Fatal(err)
	}

	got, err := metrics.Merge(older, newer).Select(context.Background(), metrics.SelectParams{MinT: 0, MaxT: 40})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || len(got[0].Samples) != 1 || got[0].Samples[0].Value != 22 {
		t.Fatalf("got %+v, want one sample at 20 with value 22: the second source's higher generation must win even though it is second", got)
	}
}

// rawSource returns a fixed answer regardless of the query. Unlike
// *MemoryStore, it does not itself keep samples sorted and deduped, so it can
// hand Merge input shaped the way a peer over the network could send it: out
// of order, or with two samples at one timestamp.
type rawSource struct{ data []metrics.SeriesData }

func (r rawSource) Select(context.Context, metrics.SelectParams) ([]metrics.SeriesData, error) {
	return r.data, nil
}
func (r rawSource) SelectLabelNames(context.Context) ([]string, error) { return nil, nil }
func (r rawSource) SelectLabelValues(context.Context, string) ([]string, error) {
	return nil, nil
}

// Merge must normalize through the same rule a single Source's own reads use
// (sortAndDedup), not a second implementation that only agrees with it on
// contract-conforming input.
func TestMergeNormalizesUnsortedOrDuplicateTimestampsAcrossSources(t *testing.T) {
	m, _ := metrics.NewLabels(map[string]string{"__name__": "messy"})
	// a arrives out of order: timestamp 30 before timestamp 10.
	a := rawSource{data: []metrics.SeriesData{{Labels: m, Samples: []metrics.Sample{
		{TimestampMs: 30, Value: 3, Gen: 1},
		{TimestampMs: 10, Value: 1, Gen: 1},
	}}}}
	// b holds two samples at the same timestamp — a shape a single
	// contract-conforming Source never returns on its own.
	b := rawSource{data: []metrics.SeriesData{{Labels: m, Samples: []metrics.Sample{
		{TimestampMs: 10, Value: 50, Gen: 1},
		{TimestampMs: 10, Value: 2, Gen: 5},
	}}}}

	got, err := metrics.Merge(a, b).Select(context.Background(), metrics.SelectParams{MinT: 0, MaxT: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d series, want 1: %+v", len(got), got)
	}
	s := got[0].Samples
	if len(s) != 2 ||
		s[0].TimestampMs != 10 || s[0].Value != 2 ||
		s[1].TimestampMs != 30 || s[1].Value != 3 {
		t.Fatalf("samples = %+v, want [{10 2} {30 3}]: one sample per timestamp, ascending, "+
			"the higher generation (5) winning at the duplicated timestamp 10", s)
	}
}

// A SeriesOnly (with AnyTime) select is the metadata path: no samples travel,
// only labels. The same series known to both sources must still collapse to
// one entry, not two.
func TestMergeSeriesOnlySelectUnionsSeriesAcrossSources(t *testing.T) {
	both, _ := metrics.NewLabels(map[string]string{"__name__": "both"})
	onlyFirst, _ := metrics.NewLabels(map[string]string{"__name__": "only_first"})
	onlySecond, _ := metrics.NewLabels(map[string]string{"__name__": "only_second"})

	first := metrics.NewMemoryStore()
	if err := first.Append(both, 10, 1); err != nil {
		t.Fatal(err)
	}
	if err := first.Append(onlyFirst, 5, 2); err != nil {
		t.Fatal(err)
	}
	second := metrics.NewMemoryStore()
	if err := second.Append(both, 20, 3); err != nil {
		t.Fatal(err)
	}
	if err := second.Append(onlySecond, 7, 4); err != nil {
		t.Fatal(err)
	}

	got, err := metrics.Merge(first, second).Select(context.Background(), metrics.SelectParams{SeriesOnly: true, AnyTime: true})
	if err != nil {
		t.Fatal(err)
	}
	counts := make(map[string]int, len(got))
	for _, sd := range got {
		n, _ := sd.Labels.Get("__name__")
		counts[n]++
	}
	if len(got) != 3 {
		t.Fatalf("got %d series, want 3 (the union, \"both\" counted once): %+v", len(got), counts)
	}
	for _, want := range []string{"both", "only_first", "only_second"} {
		if counts[want] != 1 {
			t.Fatalf("series %q appears %d times, want exactly 1: %+v", want, counts[want], counts)
		}
	}
}

// SelectLabelNames/SelectLabelValues must return the sorted union of both
// sides with no duplicate entries, not first's answer, not a concatenation.
func TestMergeUnionsLabelNamesAndValues(t *testing.T) {
	a, _ := metrics.NewLabels(map[string]string{"__name__": "u", "env": "prod", "shared": "x"})
	b, _ := metrics.NewLabels(map[string]string{"__name__": "u", "region": "us", "shared": "x"})
	first := metrics.NewMemoryStore()
	if err := first.Append(a, 1, 1); err != nil {
		t.Fatal(err)
	}
	second := metrics.NewMemoryStore()
	if err := second.Append(b, 1, 1); err != nil {
		t.Fatal(err)
	}
	merged := metrics.Merge(first, second)

	names, err := merged.SelectLabelNames(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wantNames := []string{"__name__", "env", "region", "shared"}
	if len(names) != len(wantNames) {
		t.Fatalf("label names = %v, want %v (sorted union, no duplicates)", names, wantNames)
	}
	for i, n := range wantNames {
		if names[i] != n {
			t.Fatalf("label names = %v, want %v (sorted union, no duplicates)", names, wantNames)
		}
	}

	values, err := merged.SelectLabelValues(context.Background(), "shared")
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 1 || values[0] != "x" {
		t.Fatalf("shared values = %v, want [x]: both sides agree, so the union must not duplicate it", values)
	}
}

func TestMergePropagatesEitherFailure(t *testing.T) {
	ok := metrics.NewMemoryStore()
	for _, src := range []metrics.Source{metrics.Merge(failingSource{ok}, ok), metrics.Merge(ok, failingSource{ok})} {
		if _, err := src.Select(context.Background(), metrics.SelectParams{MinT: 0, MaxT: 1}); err == nil {
			t.Fatal("a failing side must fail the merged select: a partial answer would be silently wrong")
		}
	}
}

// A flush that lands between the two reads must not open a gap: the ingester is
// read first, so data it drops afterwards is already registered in the store.
func TestMergeHasNoGapAcrossAFlush(t *testing.T) {
	store, err := metrics.NewBlockStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	head, err := metrics.OpenHeadStore(t.TempDir(), store, metrics.HeadStoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	l, _ := metrics.NewLabels(map[string]string{"__name__": "gap"})
	for i := range 120 {
		if err := head.Append(l, int64(i)*1000, float64(i)); err != nil {
			t.Fatal(err)
		}
	}
	// The flush lands after whichever read Merge performs first. Read in the
	// right order (head, then store) nothing is lost; an implementation that
	// read the store first would see an empty store and then an emptied head.
	var once sync.Once
	var didFlush bool
	flushOnce := func() {
		once.Do(func() {
			ok, err := head.FlushBlock()
			if err != nil {
				t.Errorf("FlushBlock: %v", err)
			}
			didFlush = ok
		})
	}
	eng := metrics.NewQueryEngineFromSource(metrics.Merge(
		flushAfter{Source: head, flush: flushOnce},
		flushAfter{Source: store, flush: flushOnce},
	))
	got, err := eng.RangeQuery(metrics.Selector{MetricName: "gap"}, 0, 119_000, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || len(got[0].Points) != 120 {
		t.Fatalf("range across a mid-query flush = %d series / %v points, want all 120", len(got), got)
	}
	// Both SealedChunkCount()==0 below and an empty range would also be true of
	// a flush that silently did nothing, so pin FlushBlock's own report of
	// whether it moved data — the test would otherwise pass vacuously.
	if !didFlush {
		t.Fatal("FlushBlock reported nothing to flush (ok=false); the test proved nothing")
	}
	if n := head.SealedChunkCount(); n != 0 {
		t.Fatalf("the flush did not happen (sealed=%d); the test proved nothing", n)
	}
}

// flushAfter answers from its Source, then flushes — the interleaving a query
// can meet in the split topology.
type flushAfter struct {
	metrics.Source
	flush func()
}

func (f flushAfter) Select(ctx context.Context, p metrics.SelectParams) ([]metrics.SeriesData, error) {
	out, err := f.Source.Select(ctx, p)
	f.flush()
	return out, err
}

// The restart check from Task 8, end to end: after a restart, an overwrite at a
// flushed timestamp is what a query through the merge returns.
func TestMergeServesAPostRestartOverwrite(t *testing.T) {
	dataDir := t.TempDir()
	store, err := metrics.NewBlockStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	h, err := metrics.OpenHeadStore(dataDir, store, metrics.HeadStoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	l, _ := metrics.NewLabels(map[string]string{"__name__": "restart"})
	for i := range 121 {
		if err := h.Append(l, int64(i)*1000, float64(i)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := h.FlushBlock(); err != nil {
		t.Fatal(err)
	}
	h2, err := metrics.OpenHeadStore(dataDir, store, metrics.HeadStoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := h2.Append(l, 0, 999); err != nil {
		t.Fatal(err)
	}
	got, err := metrics.NewQueryEngineFromSource(metrics.Merge(h2, store)).InstantQuery(metrics.Selector{MetricName: "restart"}, 0)
	if err != nil || len(got) != 1 || got[0].Value != 999 {
		t.Fatalf("post-restart overwrite = %+v, %v; want 999", got, err)
	}
}
