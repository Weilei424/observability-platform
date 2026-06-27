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

func TestCompact_DedupsEqualTimestamps_HigherGenerationWins(t *testing.T) {
	dir := t.TempDir()
	blocks, tmp := filepath.Join(dir, "blocks"), filepath.Join(dir, "tmp")
	lbl := []block.LabelPair{{Name: "__name__", Value: "m"}}

	// Two blocks share ts=1000; the sample written later (higher generation) wins,
	// independent of the order the sources are passed to Compact.
	older := writeBlockSeq(t, blocks, tmp, 1, 1, lbl, [][2]int64{{1000, 10}})
	newer := writeBlockSeq(t, blocks, tmp, 2, 1, lbl, [][2]int64{{1000, 99}})

	out := filepath.Join(dir, "out")
	meta, err := block.Compact(out, tmp, []*block.Reader{newer, older}) // newer first on purpose
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	merged, err := block.OpenReader(filepath.Join(out, meta.BlockID))
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	got := readAll(t, merged, 1)
	if len(got) != 1 || got[0][0] != 1000 || got[0][1] != 99 {
		t.Fatalf("ts=1000 resolved to %v, want [[1000 99]] (higher generation wins)", got)
	}
}

// writeBlockSeq writes a one-series block whose samples all carry write generation
// gen, returning an open Reader. Used to give compaction sources distinct
// generations independent of their time bounds.
func writeBlockSeq(t *testing.T, blocks, tmp string, gen int64, id uint64, lbl []block.LabelPair, samples [][2]int64) *block.Reader {
	t.Helper()
	w, err := block.NewWriter(blocks, tmp)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	c := chunk.NewChunk()
	for _, s := range samples {
		if err := c.Append(s[0], float64(s[1]), gen); err != nil {
			t.Fatalf("chunk.Append: %v", err)
		}
	}
	if err := w.AddSeries(id, lbl, []*chunk.Chunk{c}); err != nil {
		t.Fatalf("AddSeries: %v", err)
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

// TestCompact_DedupResolvedByGeneration_NotMinTime proves equal-timestamp dedup is
// decided by per-sample write generation, not time bounds. The newer block (gen 2)
// deliberately has the SMALLER MinTime — as happens when a correction arrives with
// an out-of-order earlier sample — so a MinTime-ordered merge would wrongly keep
// the older value.
func TestCompact_DedupResolvedByGeneration_NotMinTime(t *testing.T) {
	dir := t.TempDir()
	blocks, tmp := filepath.Join(dir, "blocks"), filepath.Join(dir, "tmp")
	lbl := []block.LabelPair{{Name: "__name__", Value: "m"}}

	newer := writeBlockSeq(t, blocks, tmp, 2, 1, lbl, [][2]int64{{50, 5}, {100, 99}})   // MinTime 50
	older := writeBlockSeq(t, blocks, tmp, 1, 1, lbl, [][2]int64{{100, 11}, {200, 22}}) // MinTime 100

	out := filepath.Join(dir, "out")
	// Pass newer first so input order alone cannot explain a correct result.
	meta, err := block.Compact(out, tmp, []*block.Reader{newer, older})
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if meta.MaxGen != 2 {
		t.Fatalf("merged MaxGen = %d, want 2 (max sample generation)", meta.MaxGen)
	}
	merged, err := block.OpenReader(filepath.Join(out, meta.BlockID))
	if err != nil {
		t.Fatalf("OpenReader merged: %v", err)
	}
	got := readAll(t, merged, 1)
	want := [][2]int64{{50, 5}, {100, 99}, {200, 22}}
	if len(got) != len(want) {
		t.Fatalf("merged = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("sample %d = %v, want %v (ts=100 must resolve to the higher-generation block)", i, got[i], want[i])
		}
	}
}

// TestCompact_RechunkSealsAtSampleBoundary verifies the merged block re-chunks at
// the 120-sample seal boundary rather than emitting one oversized chunk.
func TestCompact_RechunkSealsAtSampleBoundary(t *testing.T) {
	dir := t.TempDir()
	blocks, tmp := filepath.Join(dir, "blocks"), filepath.Join(dir, "tmp")
	lbl := []block.LabelPair{{Name: "__name__", Value: "m"}}

	first := make([][2]int64, 0, 80)
	for i := 0; i < 80; i++ {
		first = append(first, [2]int64{int64(i) * 1000, int64(i)})
	}
	second := make([][2]int64, 0, 80)
	for i := 80; i < 160; i++ {
		second = append(second, [2]int64{int64(i) * 1000, int64(i)})
	}
	r1 := writeBlockSeq(t, blocks, tmp, 1, 1, lbl, first)
	r2 := writeBlockSeq(t, blocks, tmp, 2, 1, lbl, second)

	out := filepath.Join(dir, "out")
	meta, err := block.Compact(out, tmp, []*block.Reader{r1, r2})
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	merged, err := block.OpenReader(filepath.Join(out, meta.BlockID))
	if err != nil {
		t.Fatalf("OpenReader merged: %v", err)
	}
	se, ok := merged.SeriesByID(1)
	if !ok {
		t.Fatal("series 1 missing from merged block")
	}
	if len(se.Chunks) != 2 {
		t.Fatalf("merged series has %d chunks, want 2 (120 + 40 across the 120-sample seal)", len(se.Chunks))
	}
	c0, err := merged.ReadChunk(se.Chunks[0])
	if err != nil {
		t.Fatalf("ReadChunk: %v", err)
	}
	if c0.NumSamples() != 120 {
		t.Fatalf("first merged chunk has %d samples, want 120", c0.NumSamples())
	}
	if total := len(readAll(t, merged, 1)); total != 160 {
		t.Fatalf("merged total samples = %d, want 160", total)
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
