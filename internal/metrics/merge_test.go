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
	flushOnce := func() {
		once.Do(func() {
			if _, err := head.FlushBlock(); err != nil {
				t.Errorf("FlushBlock: %v", err)
			}
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
