package metrics_test

import (
	"path/filepath"
	"sort"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/storage/block"
	"github.com/masonwheeler/observability-platform/internal/storage/chunk"
)

func makeLabels(t *testing.T, m map[string]string) metrics.Labels {
	t.Helper()
	l, err := metrics.NewLabels(m)
	if err != nil {
		t.Fatalf("NewLabels: %v", err)
	}
	return l
}

// labelsToBlockPairs converts a label set to sorted block.LabelPair form.
func labelsToBlockPairs(l metrics.Labels) []block.LabelPair {
	m := l.Map()
	pairs := make([]block.LabelPair, 0, len(m))
	for name, val := range m {
		pairs = append(pairs, block.LabelPair{Name: name, Value: val})
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].Name < pairs[j].Name })
	return pairs
}

// sealedChunk returns a chunk filled past the seal threshold.
func sealedChunk(t *testing.T) *chunk.Chunk {
	t.Helper()
	c := chunk.NewChunk()
	for i := int64(0); i < 120; i++ {
		if err := c.Append(1000+i, float64(i)); err != nil {
			t.Fatalf("chunk append: %v", err)
		}
	}
	return c
}

// TestNewBlockStore_RejectsFingerprintMismatch verifies that a block whose
// stored series ID does not match the fingerprint of its label set is rejected
// at load, rather than silently producing wrong query results later.
func TestNewBlockStore_RejectsFingerprintMismatch(t *testing.T) {
	dataDir := t.TempDir()
	blocksDir := filepath.Join(dataDir, "metrics", "blocks")
	tmpDir := filepath.Join(dataDir, "metrics", "tmp")
	w, err := block.NewWriter(blocksDir, tmpDir)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	labels := makeLabels(t, map[string]string{"__name__": "cpu", "host": "a"})
	// Deliberately store a wrong ID (correct fingerprint + 1).
	wrongID := uint64(labels.Fingerprint()) + 1
	if err := w.AddSeries(wrongID, labelsToBlockPairs(labels), []*chunk.Chunk{sealedChunk(t)}); err != nil {
		t.Fatalf("AddSeries: %v", err)
	}
	if _, err := w.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	if _, err := metrics.NewBlockStore(dataDir); err == nil {
		t.Fatal("NewBlockStore with fingerprint/ID mismatch: want error, got nil")
	}
}

// TestNewBlockStore_FailureWithValidBlockLoadedFirst exercises the cleanup path
// where one valid block is already loaded before a later block fails validation,
// so the readers accumulated so far are released rather than leaked.
func TestNewBlockStore_FailureWithValidBlockLoadedFirst(t *testing.T) {
	dataDir := t.TempDir()

	// A valid block written through the normal flush path.
	bs, err := metrics.NewBlockStore(dataDir)
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}
	good := makeLabels(t, map[string]string{"__name__": "ok", "host": "a"})
	for i := 0; i < 120; i++ {
		if err := bs.Append(good, int64(i*1000), float64(i)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if _, err := bs.FlushBlock(); err != nil {
		t.Fatalf("FlushBlock: %v", err)
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// A second, invalid block (wrong stored ID) written directly.
	blocksDir := filepath.Join(dataDir, "metrics", "blocks")
	tmpDir := filepath.Join(dataDir, "metrics", "tmp")
	w, err := block.NewWriter(blocksDir, tmpDir)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	bad := makeLabels(t, map[string]string{"__name__": "bad", "host": "b"})
	if err := w.AddSeries(uint64(bad.Fingerprint())+1, labelsToBlockPairs(bad), []*chunk.Chunk{sealedChunk(t)}); err != nil {
		t.Fatalf("AddSeries: %v", err)
	}
	if _, err := w.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	if _, err := metrics.NewBlockStore(dataDir); err == nil {
		t.Fatal("NewBlockStore with one valid and one invalid block: want error, got nil")
	}
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
	matched, err := bs2.SelectSeries(sel)
	if err != nil {
		t.Fatalf("SelectSeries: %v", err)
	}
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

	got, err := bs.SelectSeries(metrics.Selector{MetricName: "http"})
	if err != nil {
		t.Fatalf("SelectSeries(http): %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("SelectSeries(http) matched %d, want 2 (block+memory)", len(got))
	}
	gotAPI, err := bs.SelectSeries(metrics.Selector{MetricName: "http", Matchers: []metrics.Matcher{{Name: "job", Value: "api"}}})
	if err != nil {
		t.Fatalf("SelectSeries(http,job=api): %v", err)
	}
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
