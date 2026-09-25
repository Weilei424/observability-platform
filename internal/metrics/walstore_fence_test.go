package metrics_test

import (
	"path/filepath"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/storage/wal"
)

// A chunk that seals while a flush is in flight is not in that flush's
// snapshot, so it stays in memory — and its samples must stay in the WAL until
// a later flush persists them. With a fence that tracked only each series'
// newest chunk, the head chunk allocated after the seal moved the fence past the
// sealed chunk's segments and the checkpoint deleted them.
func TestWALStore_FlushBlock_FenceCoversChunksSealedDuringFlush(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "metrics", "wal")
	w, err := wal.Open(walDir, 1, 1) // a 1-byte limit gives every record its own segment
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	bs, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}
	store := metrics.NewWALStore(w, bs, dir)
	labels, _ := metrics.NewLabels(map[string]string{"__name__": "fence"})
	appendRange := func(from, to int) {
		for i := from; i < to; i++ {
			if err := store.Append(labels, int64(i)*1000, float64(i)); err != nil {
				t.Fatalf("Append %d: %v", i, err)
			}
		}
	}

	appendRange(0, 121) // 0..119 seal the first chunk; 120 opens the second
	store.SetTestBeforeCheckpoint(func() {
		// The flush has persisted 0..119 and dropped them from memory. Seal the
		// second chunk (120..239) and open a third at 240, all before the
		// checkpoint boundary is computed.
		appendRange(121, 241)
	})
	if _, err := store.FlushBlock(); err != nil {
		t.Fatalf("FlushBlock: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close WAL: %v", err)
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("Close block store: %v", err)
	}

	// Restart: blocks, then WAL replay from the checkpoint.
	bs2, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("NewBlockStore restart: %v", err)
	}
	t.Cleanup(func() { _ = bs2.Close() })
	if err := wal.ReplayFrom(walDir, metrics.ReadCheckpoint(dir), func(pairs []wal.LabelPair, tsMs int64, value float64) {
		lm := make(map[string]string, len(pairs))
		for _, p := range pairs {
			lm[p.Name] = p.Value
		}
		l, err := metrics.NewLabels(lm)
		if err != nil {
			t.Errorf("replay NewLabels: %v", err)
			return
		}
		if err := bs2.Append(l, tsMs, value); err != nil {
			t.Errorf("replay Append: %v", err)
		}
	}); err != nil {
		t.Fatalf("ReplayFrom: %v", err)
	}

	got, err := bs2.QueryRange(metrics.SeriesID(labels.Hash()), 0, 240*1000)
	if err != nil {
		t.Fatalf("QueryRange: %v", err)
	}
	if len(got) != 241 {
		t.Fatalf("got %d samples after restart, want 241: the chunk that sealed mid-flush lost its WAL segments", len(got))
	}
}
