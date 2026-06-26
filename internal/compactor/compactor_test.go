package compactor_test

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/masonwheeler/observability-platform/internal/compactor"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/observability"
	"github.com/masonwheeler/observability-platform/internal/storage/wal"
)

func newStores(t *testing.T, dir string) (*metrics.WALStore, *metrics.BlockStore) {
	t.Helper()
	bs, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}
	w, err := wal.Open(filepath.Join(dir, "metrics", "wal"), 128<<20, 1)
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return metrics.NewWALStore(w, bs, dir), bs
}

func ingestSealedBlock(t *testing.T, ws *metrics.WALStore, name string, base int64) {
	t.Helper()
	lbls, _ := metrics.NewLabels(map[string]string{"__name__": name})
	for i := 0; i < 120; i++ {
		if err := ws.Append(lbls, base+int64(i)*1000, float64(i)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
}

func testConfig() compactor.Config {
	return compactor.Config{
		MaintenanceInterval: time.Hour, // RunOnce is called directly in tests
		FlushInterval:       0,         // always flush when RunOnce runs
		FlushSealedChunks:   0,
		FlushWALBytes:       0,
		Ranges:              compactor.Ranges(3_600_000, 4, 2), // 1h, 4h
		Retention:           0,
	}
}

func TestCompactor_RunOnce_FlushesCompactsAndQueryable(t *testing.T) {
	dir := t.TempDir()
	ws, bs := newStores(t, dir)
	_, mx := observability.NewRegistry(bs, bs)
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	c := compactor.New(ws, bs, ws, time.Now, testConfig(), mx, log)

	// A single FlushBlock drains ALL sealed chunks into ONE block, so to get two
	// blocks to compact we must flush between the two batches. First batch →
	// RunOnce flushes block 1 (level 1); nothing to compact yet.
	ingestSealedBlock(t, ws, "m", 0) // ts 0..119000
	c.RunOnce(context.Background())
	if got := len(bs.BlockInfos()); got != 1 {
		t.Fatalf("after first RunOnce blocks = %d, want 1", got)
	}

	// Second batch → RunOnce flushes block 2 (level 1); now two aligned level-1
	// blocks in window 0 of range 1h → compacted to one level-2 block.
	ingestSealedBlock(t, ws, "m", 1_000_000) // ts 1_000_000..1_119_000
	c.RunOnce(context.Background())

	infos := bs.BlockInfos()
	if len(infos) != 1 {
		t.Fatalf("after second RunOnce blocks = %d, want 1 (compacted)", len(infos))
	}
	if infos[0].Level != 2 {
		t.Fatalf("remaining block level = %d, want 2 (proves two level-1 blocks were merged, not just flushed)", infos[0].Level)
	}
	id := mustFingerprint(t, "m")
	samples, err := bs.QueryRange(id, 0, 2_000_000)
	if err != nil {
		t.Fatalf("QueryRange: %v", err)
	}
	if len(samples) != 240 {
		t.Fatalf("query returned %d samples, want 240", len(samples))
	}
}

func TestCompactor_RunOnce_RetentionDeletesExpired(t *testing.T) {
	dir := t.TempDir()
	ws, bs := newStores(t, dir)
	_, mx := observability.NewRegistry(bs, bs)
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	ingestSealedBlock(t, ws, "m", 0) // block MaxTime 119000

	cfg := testConfig()
	cfg.Retention = time.Millisecond
	// Fixed clock far in the future so the block is expired.
	clock := func() time.Time { return time.UnixMilli(10_000_000) }
	c := compactor.New(ws, bs, ws, clock, cfg, mx, log)
	c.RunOnce(context.Background())

	if got := len(bs.BlockInfos()); got != 0 {
		t.Fatalf("after retention blocks = %d, want 0", got)
	}
}

func TestCompactor_CompactedDataSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	ws, bs := newStores(t, dir)
	_, mx := observability.NewRegistry(bs, bs)
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	c := compactor.New(ws, bs, ws, time.Now, testConfig(), mx, log)

	// Flush between batches to form two blocks, then compact them into one.
	ingestSealedBlock(t, ws, "m", 0)
	c.RunOnce(context.Background())
	ingestSealedBlock(t, ws, "m", 1_000_000)
	c.RunOnce(context.Background())
	if infos := bs.BlockInfos(); len(infos) != 1 || infos[0].Level != 2 {
		t.Fatalf("pre-restart blocks = %+v, want one level-2 compacted block", infos)
	}
	_ = bs.Close()

	// Reopen from the same data dir; the compacted block persists and startup GC
	// tolerates the already-deleted sources.
	bs2, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	id := mustFingerprint(t, "m")
	samples, err := bs2.QueryRange(id, 0, 2_000_000)
	if err != nil {
		t.Fatalf("QueryRange after restart: %v", err)
	}
	if len(samples) != 240 {
		t.Fatalf("after restart query returned %d samples, want 240", len(samples))
	}
}

func mustFingerprint(t *testing.T, name string) metrics.SeriesID {
	t.Helper()
	lbls, err := metrics.NewLabels(map[string]string{"__name__": name})
	if err != nil {
		t.Fatal(err)
	}
	return lbls.Fingerprint()
}
