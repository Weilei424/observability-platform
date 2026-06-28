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
	"github.com/prometheus/client_golang/prometheus"
)

// counterValue returns the current value of the named counter from reg.
func counterValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() == name {
			if m := mf.GetMetric(); len(m) > 0 {
				return m[0].GetCounter().GetValue()
			}
		}
	}
	return 0
}

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
	reg, mx := observability.NewRegistry(bs, bs)
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
	if v := counterValue(t, reg, "obs_retention_deleted_blocks_total"); v != 1 {
		t.Errorf("RetentionDeletedTotal = %v, want 1", v)
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

// TestCompactor_RunOnce_FlushesOnSealedChunkThreshold proves the sealed-chunk
// flush trigger fires independently of the interval trigger.
func TestCompactor_RunOnce_FlushesOnSealedChunkThreshold(t *testing.T) {
	dir := t.TempDir()
	ws, bs := newStores(t, dir)
	_, mx := observability.NewRegistry(bs, bs)
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	cfg := testConfig()
	cfg.FlushInterval = time.Hour // interval must not be what triggers the flush
	cfg.FlushSealedChunks = 1
	cfg.FlushWALBytes = 0
	c := compactor.New(ws, bs, ws, time.Now, cfg, mx, log)

	// Nothing sealed yet: neither interval nor threshold is due.
	c.RunOnce(context.Background())
	if got := len(bs.BlockInfos()); got != 0 {
		t.Fatalf("flushed with no sealed chunks: blocks = %d, want 0", got)
	}

	ingestSealedBlock(t, ws, "m", 0) // one sealed chunk crosses the threshold
	c.RunOnce(context.Background())
	if got := len(bs.BlockInfos()); got != 1 {
		t.Fatalf("sealed-chunk threshold did not flush: blocks = %d, want 1", got)
	}
}

// TestCompactor_RunOnce_FlushesOnWALBytesThreshold proves the WAL-size flush
// trigger fires independently of interval and sealed-chunk triggers.
func TestCompactor_RunOnce_FlushesOnWALBytesThreshold(t *testing.T) {
	dir := t.TempDir()
	ws, bs := newStores(t, dir)
	_, mx := observability.NewRegistry(bs, bs)
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	cfg := testConfig()
	cfg.FlushInterval = time.Hour
	cfg.FlushSealedChunks = 0 // isolate the WAL-bytes trigger
	cfg.FlushWALBytes = 1
	c := compactor.New(ws, bs, ws, time.Now, cfg, mx, log)

	ingestSealedBlock(t, ws, "m", 0) // grows the WAL past 1 byte and seals a chunk
	c.RunOnce(context.Background())
	if got := len(bs.BlockInfos()); got != 1 {
		t.Fatalf("WAL-bytes threshold did not flush: blocks = %d, want 1", got)
	}
}

// TestCompactor_RunOnce_NoOpFlushNotCounted verifies an idle interval tick whose
// flush writes no block does not inflate obs_flushes_total.
func TestCompactor_RunOnce_NoOpFlushNotCounted(t *testing.T) {
	dir := t.TempDir()
	ws, bs := newStores(t, dir)
	reg, mx := observability.NewRegistry(bs, bs)
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	c := compactor.New(ws, bs, ws, time.Now, testConfig(), mx, log) // FlushInterval 0 → always due

	// No sealed chunks: the flush is a no-op and must not be counted.
	c.RunOnce(context.Background())
	if v := counterValue(t, reg, "obs_flushes_total"); v != 0 {
		t.Errorf("FlushesTotal after no-op flush = %v, want 0", v)
	}

	// A real flush increments the counter.
	ingestSealedBlock(t, ws, "m", 0)
	c.RunOnce(context.Background())
	if v := counterValue(t, reg, "obs_flushes_total"); v != 1 {
		t.Errorf("FlushesTotal after one real flush = %v, want 1", v)
	}
}

// TestCompactor_RunOnce_MetricsReflectMaintenance asserts the flush and compaction
// counters hold the expected values after a known maintenance sequence (two flushes
// that are then compacted into one block).
func TestCompactor_RunOnce_MetricsReflectMaintenance(t *testing.T) {
	dir := t.TempDir()
	ws, bs := newStores(t, dir)
	reg, mx := observability.NewRegistry(bs, bs)
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	c := compactor.New(ws, bs, ws, time.Now, testConfig(), mx, log) // Retention 0

	ingestSealedBlock(t, ws, "m", 0)
	c.RunOnce(context.Background())
	ingestSealedBlock(t, ws, "m", 1_000_000)
	c.RunOnce(context.Background())

	if v := counterValue(t, reg, "obs_flushes_total"); v != 2 {
		t.Errorf("FlushesTotal = %v, want 2", v)
	}
	if v := counterValue(t, reg, "obs_compactions_total"); v != 1 {
		t.Errorf("CompactionsTotal = %v, want 1", v)
	}
	if v := counterValue(t, reg, "obs_flush_failures_total"); v != 0 {
		t.Errorf("FlushFailuresTotal = %v, want 0", v)
	}
	if v := counterValue(t, reg, "obs_compaction_failures_total"); v != 0 {
		t.Errorf("CompactionFailuresTotal = %v, want 0", v)
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
