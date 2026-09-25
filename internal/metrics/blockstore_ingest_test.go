package metrics_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/storage/chunk"
)

// sealedChunkAt returns a sealed chunk of n samples starting at base, one per
// second. (blockstore_test.go already declares a sealedChunk helper.)
func sealedChunkAt(t *testing.T, base int64, n int, gen int64) *chunk.Chunk {
	t.Helper()
	c := chunk.NewChunk()
	for i := range n {
		if err := c.Append(base+int64(i)*1000, float64(i), gen+int64(i)); err != nil {
			t.Fatalf("chunk Append: %v", err)
		}
	}
	return c
}

func TestBlockStore_IngestSeriesChunks_PersistsAndServes(t *testing.T) {
	dir := t.TempDir()
	bs, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}
	l, _ := metrics.NewLabels(map[string]string{"__name__": "ingested", "k": "v"})
	meta, err := bs.IngestSeriesChunks(context.Background(), []metrics.SeriesChunks{{
		ID: metrics.SeriesID(l.Hash()), Labels: l, Chunks: []*chunk.Chunk{sealedChunkAt(t, 0, 120, 1)},
	}})
	if err != nil {
		t.Fatalf("IngestSeriesChunks: %v", err)
	}
	if meta.NumSeries != 1 || meta.NumSamples != 120 {
		t.Fatalf("meta = %+v, want 1 series and 120 samples", meta)
	}

	// Queryable before the call returned — the flush-in contract.
	got, err := bs.QueryRange(metrics.SeriesID(l.Hash()), 0, 119_000)
	if err != nil || len(got) != 120 {
		t.Fatalf("QueryRange = %d samples, %v; want 120", len(got), err)
	}

	// And durable: a restart finds it.
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	bs2, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = bs2.Close() })
	if got, _ := bs2.QueryRange(metrics.SeriesID(l.Hash()), 0, 119_000); len(got) != 120 {
		t.Fatalf("after restart: %d samples, want 120", len(got))
	}
}

func TestBlockStore_IngestSeriesChunks_RejectsEmptyAndMismatchedInput(t *testing.T) {
	dir := t.TempDir()
	bs, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	if _, err := bs.IngestSeriesChunks(context.Background(), nil); err == nil {
		t.Error("an empty ingest must fail rather than publish an empty block")
	}

	// An ID that is not the label set's fingerprint must be refused by the same
	// pre-publication validation a flush gets, leaving nothing on disk.
	l, _ := metrics.NewLabels(map[string]string{"__name__": "mismatch"})
	if _, err := bs.IngestSeriesChunks(context.Background(), []metrics.SeriesChunks{{
		ID: metrics.SeriesID(l.Hash() + 1), Labels: l, Chunks: []*chunk.Chunk{sealedChunkAt(t, 0, 120, 1)},
	}}); err == nil {
		t.Fatal("a fingerprint mismatch must be rejected")
	}
	entries, err := os.ReadDir(filepath.Join(dir, "metrics", "blocks"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("a rejected ingest left %d block(s) on disk", len(entries))
	}
}

func TestBlockStore_IngestSeriesChunks_HonoursCancellation(t *testing.T) {
	bs, err := metrics.NewBlockStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	l, _ := metrics.NewLabels(map[string]string{"__name__": "cancelled"})
	if _, err := bs.IngestSeriesChunks(ctx, []metrics.SeriesChunks{{
		ID: metrics.SeriesID(l.Hash()), Labels: l, Chunks: []*chunk.Chunk{sealedChunkAt(t, 0, 120, 1)},
	}}); err == nil {
		t.Fatal("a cancelled context must stop the ingest before it writes")
	}
}
