package metrics_test

import (
	"testing"

	"github.com/masonwheeler/observability-platform/internal/metrics"
)

func makeLabels(t *testing.T, m map[string]string) metrics.Labels {
	t.Helper()
	l, err := metrics.NewLabels(m)
	if err != nil {
		t.Fatalf("NewLabels: %v", err)
	}
	return l
}

func TestBlockStore_FlushBlock_DrainsSealedChunks(t *testing.T) {
	dataDir := t.TempDir()
	bs, err := metrics.NewBlockStore(dataDir)
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}

	labels := makeLabels(t, map[string]string{"__name__": "cpu", "host": "a"})

	// Append 121 samples to force one sealed chunk + one open head chunk.
	for i := 0; i < 121; i++ {
		if err := bs.Append(labels, int64(i*1000), float64(i)); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	if _, err := bs.FlushBlock(); err != nil {
		t.Fatalf("FlushBlock: %v", err)
	}

	// After flush, the memory store should retain only the unsealed head chunk.
	id := labels.Fingerprint()
	mem := bs.MemStore()
	if mem.ChunkCount(id) != 1 {
		t.Errorf("ChunkCount after flush = %d, want 1 (head only)", mem.ChunkCount(id))
	}
}

func TestBlockStore_QueryRange_SpansMemoryAndBlock(t *testing.T) {
	dataDir := t.TempDir()
	bs, err := metrics.NewBlockStore(dataDir)
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}

	labels := makeLabels(t, map[string]string{"__name__": "req", "env": "test"})
	id := labels.Fingerprint()

	// Append 120 samples — exactly fills one chunk (seals it).
	for i := 0; i < 120; i++ {
		if err := bs.Append(labels, int64(i*1000), float64(i)); err != nil {
			t.Fatalf("Append block sample %d: %v", i, err)
		}
	}

	if _, err := bs.FlushBlock(); err != nil {
		t.Fatalf("FlushBlock: %v", err)
	}

	// Append 3 more samples into the new head chunk.
	for i := 120; i < 123; i++ {
		if err := bs.Append(labels, int64(i*1000), float64(i)); err != nil {
			t.Fatalf("Append head sample %d: %v", i, err)
		}
	}

	got, err := bs.QueryRange(id, 0, int64(122*1000))
	if err != nil {
		t.Fatalf("QueryRange: %v", err)
	}
	if len(got) != 123 {
		t.Errorf("QueryRange returned %d samples, want 123", len(got))
	}
}

func TestBlockStore_QueryRange_Deduplication(t *testing.T) {
	dataDir := t.TempDir()
	bs, err := metrics.NewBlockStore(dataDir)
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}

	labels := makeLabels(t, map[string]string{"__name__": "dup", "host": "x"})
	id := labels.Fingerprint()

	// Append 120 samples and flush to block.
	for i := 0; i < 120; i++ {
		if err := bs.Append(labels, int64(i*1000), float64(i)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if _, err := bs.FlushBlock(); err != nil {
		t.Fatalf("FlushBlock: %v", err)
	}

	// Simulate crash-recovery: re-append the same 120 samples into memory.
	for i := 0; i < 120; i++ {
		if err := bs.Append(labels, int64(i*1000), float64(i)); err != nil {
			t.Fatalf("Re-append: %v", err)
		}
	}

	got, err := bs.QueryRange(id, 0, int64(119*1000))
	if err != nil {
		t.Fatalf("QueryRange: %v", err)
	}
	if len(got) != 120 {
		t.Errorf("QueryRange returned %d samples after re-append, want 120 (no duplicates)", len(got))
	}
}

func TestBlockStore_SelectSeries_IncludesBlockSeries(t *testing.T) {
	dataDir := t.TempDir()
	bs, err := metrics.NewBlockStore(dataDir)
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}

	labels := makeLabels(t, map[string]string{"__name__": "disk", "dev": "sda"})

	// Fill and flush a chunk so the series lands in a block.
	for i := 0; i < 120; i++ {
		if err := bs.Append(labels, int64(i*1000), float64(i)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if _, err := bs.FlushBlock(); err != nil {
		t.Fatalf("FlushBlock: %v", err)
	}

	// Open a fresh BlockStore from the same dataDir to simulate restart:
	// memory is empty, block is loaded from disk.
	bs2, err := metrics.NewBlockStore(dataDir)
	if err != nil {
		t.Fatalf("NewBlockStore restart: %v", err)
	}

	sel := metrics.Selector{MetricName: "disk"}
	matched := bs2.SelectSeries(sel)
	if len(matched) == 0 {
		t.Fatal("SelectSeries on fresh BlockStore returned no series, want series from block")
	}
}

func TestBlockStore_SelectSeries_IndexedAcrossBlockAndMemory(t *testing.T) {
	bs, err := metrics.NewBlockStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}
	defer bs.Close()

	// Seal a chunk (>120 samples) for an "http"/"api" series, then flush to a block.
	apiLabels := mustLabels(t, map[string]string{"__name__": "http", "job": "api"})
	for i := int64(0); i < 130; i++ {
		if err := bs.Append(apiLabels, 1000+i, float64(i)); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if _, err := bs.FlushBlock(); err != nil {
		t.Fatalf("FlushBlock: %v", err)
	}
	// A memory-only series.
	webLabels := mustLabels(t, map[string]string{"__name__": "http", "job": "web"})
	if err := bs.Append(webLabels, 2000, 1); err != nil {
		t.Fatalf("append web: %v", err)
	}

	got := bs.SelectSeries(metrics.Selector{MetricName: "http"})
	if len(got) != 2 {
		t.Fatalf("SelectSeries(http) matched %d, want 2 (block+memory)", len(got))
	}
	gotAPI := bs.SelectSeries(metrics.Selector{MetricName: "http", Matchers: []metrics.Matcher{{Name: "job", Value: "api"}}})
	if len(gotAPI) != 1 {
		t.Fatalf("SelectSeries(http,job=api) matched %d, want 1", len(gotAPI))
	}
}

func TestBlockStore_LabelNamesValues_AcrossBlockAndMemory(t *testing.T) {
	bs, err := metrics.NewBlockStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}
	defer bs.Close()

	l := mustLabels(t, map[string]string{"__name__": "http", "job": "api"})
	for i := int64(0); i < 130; i++ {
		_ = bs.Append(l, 1000+i, float64(i))
	}
	_, _ = bs.FlushBlock()
	_ = bs.Append(mustLabels(t, map[string]string{"__name__": "cpu", "host": "h1"}), 2000, 1)

	names := bs.LabelNames()
	want := map[string]bool{"__name__": true, "job": true, "host": true}
	if len(names) != 3 {
		t.Fatalf("LabelNames = %v, want 3 distinct", names)
	}
	for _, n := range names {
		if !want[n] {
			t.Fatalf("unexpected label name %q in %v", n, names)
		}
	}
	if got := bs.LabelValues("__name__"); len(got) != 2 {
		t.Fatalf("LabelValues(__name__) = %v, want [cpu http]", got)
	}
}
