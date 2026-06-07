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

	if err := bs.FlushBlock(); err != nil {
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

	if err := bs.FlushBlock(); err != nil {
		t.Fatalf("FlushBlock: %v", err)
	}

	// Append 3 more samples into the new head chunk.
	for i := 120; i < 123; i++ {
		if err := bs.Append(labels, int64(i*1000), float64(i)); err != nil {
			t.Fatalf("Append head sample %d: %v", i, err)
		}
	}

	got := bs.QueryRange(id, 0, int64(122*1000))
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
	if err := bs.FlushBlock(); err != nil {
		t.Fatalf("FlushBlock: %v", err)
	}

	// Simulate crash-recovery: re-append the same 120 samples into memory.
	for i := 0; i < 120; i++ {
		if err := bs.Append(labels, int64(i*1000), float64(i)); err != nil {
			t.Fatalf("Re-append: %v", err)
		}
	}

	got := bs.QueryRange(id, 0, int64(119*1000))
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
	if err := bs.FlushBlock(); err != nil {
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
