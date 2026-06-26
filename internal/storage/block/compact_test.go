package block_test

import (
	"path/filepath"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/storage/block"
	"github.com/masonwheeler/observability-platform/internal/storage/chunk"
)

// writeBlock commits a one-or-more-series block into blocks/ (tmp under tmp/)
// and returns an open Reader for it.
func writeBlock(t *testing.T, blocks, tmp string, series map[uint64]struct {
	labels  []block.LabelPair
	samples [][2]int64
}) *block.Reader {
	t.Helper()
	w, err := block.NewWriter(blocks, tmp)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for id, s := range series {
		if err := w.AddSeries(id, s.labels, []*chunk.Chunk{makeChunk(t, s.samples)}); err != nil {
			t.Fatalf("AddSeries %d: %v", id, err)
		}
	}
	meta, err := w.Commit()
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	r, err := block.OpenReader(filepath.Join(blocks, meta.BlockID))
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	return r
}

func readAll(t *testing.T, r *block.Reader, id uint64) [][2]int64 {
	t.Helper()
	se, ok := r.SeriesByID(id)
	if !ok {
		return nil
	}
	var out [][2]int64
	for _, ref := range se.Chunks {
		c, err := r.ReadChunk(ref)
		if err != nil {
			t.Fatalf("ReadChunk: %v", err)
		}
		it := c.Iterator()
		for it.Next() {
			ts, v := it.At()
			out = append(out, [2]int64{ts, int64(v)})
		}
	}
	return out
}

func TestCompact_MergesSharedSeries_NoDataLoss(t *testing.T) {
	dir := t.TempDir()
	blocks, tmp := filepath.Join(dir, "blocks"), filepath.Join(dir, "tmp")
	lbl := []block.LabelPair{{Name: "__name__", Value: "m"}}
	type sd = struct {
		labels  []block.LabelPair
		samples [][2]int64
	}
	r1 := writeBlock(t, blocks, tmp, map[uint64]sd{1: {lbl, [][2]int64{{1000, 10}, {2000, 20}}}})
	r2 := writeBlock(t, blocks, tmp, map[uint64]sd{1: {lbl, [][2]int64{{3000, 30}, {4000, 40}}}})

	out := filepath.Join(dir, "out")
	meta, err := block.Compact(out, tmp, []*block.Reader{r2, r1}) // unsorted input on purpose
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if meta.EffectiveLevel() != 2 {
		t.Fatalf("merged level = %d, want 2", meta.EffectiveLevel())
	}
	if len(meta.Sources) != 2 {
		t.Fatalf("merged sources = %v, want 2", meta.Sources)
	}
	merged, err := block.OpenReader(filepath.Join(out, meta.BlockID))
	if err != nil {
		t.Fatalf("OpenReader merged: %v", err)
	}
	got := readAll(t, merged, 1)
	want := [][2]int64{{1000, 10}, {2000, 20}, {3000, 30}, {4000, 40}}
	if len(got) != len(want) {
		t.Fatalf("merged samples = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("sample %d = %v, want %v", i, got[i], want[i])
		}
	}
}

func TestCompact_DedupsEqualTimestamps_LaterSourceWins(t *testing.T) {
	dir := t.TempDir()
	blocks, tmp := filepath.Join(dir, "blocks"), filepath.Join(dir, "tmp")
	lbl := []block.LabelPair{{Name: "__name__", Value: "m"}}
	type sd = struct {
		labels  []block.LabelPair
		samples [][2]int64
	}
	r1 := writeBlock(t, blocks, tmp, map[uint64]sd{1: {lbl, [][2]int64{{1000, 10}}}}) // MinTime 1000
	r2 := writeBlock(t, blocks, tmp, map[uint64]sd{1: {lbl, [][2]int64{{1000, 99}}}}) // MinTime 1000 too

	// Same MinTime: tie broken by input order after stable sort — give r2 a later
	// sample so its MinTime is higher and it sorts last. Use distinct windows:
	r2b := writeBlock(t, blocks, tmp, map[uint64]sd{1: {lbl, [][2]int64{{1000, 99}, {5000, 1}}}})
	_ = r2

	out := filepath.Join(dir, "out")
	meta, err := block.Compact(out, tmp, []*block.Reader{r1, r2b})
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	merged, _ := block.OpenReader(filepath.Join(out, meta.BlockID))
	got := readAll(t, merged, 1)
	// ts=1000 should resolve to the later source (r2b): both r1 and r2b have the
	// same MinTime (1000), so the stable sort preserves input order and r2b (passed
	// second as [r1, r2b]) remains the later source → keep value 99.
	if got[0][0] != 1000 || got[0][1] != 99 {
		t.Fatalf("ts=1000 resolved to %v, want value 99", got[0])
	}
}

func TestCompact_LabelMismatchSameID_Errors(t *testing.T) {
	dir := t.TempDir()
	blocks, tmp := filepath.Join(dir, "blocks"), filepath.Join(dir, "tmp")
	type sd = struct {
		labels  []block.LabelPair
		samples [][2]int64
	}
	r1 := writeBlock(t, blocks, tmp, map[uint64]sd{1: {[]block.LabelPair{{Name: "__name__", Value: "a"}}, [][2]int64{{1, 1}}}})
	r2 := writeBlock(t, blocks, tmp, map[uint64]sd{1: {[]block.LabelPair{{Name: "__name__", Value: "b"}}, [][2]int64{{2, 2}}}})
	if _, err := block.Compact(filepath.Join(dir, "out"), tmp, []*block.Reader{r1, r2}); err == nil {
		t.Fatal("Compact with conflicting label sets for same ID: want error, got nil")
	}
}

func TestCompact_FewerThanTwoSources_Errors(t *testing.T) {
	if _, err := block.Compact(t.TempDir(), t.TempDir(), nil); err == nil {
		t.Fatal("Compact with <2 sources: want error")
	}
}
