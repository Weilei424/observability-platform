package block_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/storage/block"
	"github.com/masonwheeler/observability-platform/internal/storage/chunk"
)

func makeChunk(t *testing.T, samples [][2]int64) *chunk.Chunk {
	t.Helper()
	c := chunk.NewChunk()
	for _, s := range samples {
		if err := c.Append(s[0], float64(s[1])); err != nil {
			t.Fatalf("chunk.Append: %v", err)
		}
	}
	return c
}

func makeWriter(t *testing.T) (*block.Writer, string, string) {
	t.Helper()
	dir := t.TempDir()
	blocksDir := filepath.Join(dir, "blocks")
	tmpDir := filepath.Join(dir, "tmp")
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		t.Fatalf("mkdir tmp: %v", err)
	}
	w, err := block.NewWriter(blocksDir, tmpDir)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	return w, blocksDir, tmpDir
}

func TestWriter_Commit_CreatesValidBlock(t *testing.T) {
	w, blocksDir, _ := makeWriter(t)

	labels1 := []block.LabelPair{{"__name__", "cpu"}, {"host", "a"}}
	labels2 := []block.LabelPair{{"__name__", "mem"}, {"host", "b"}}

	c1 := makeChunk(t, [][2]int64{{1000, 10}, {2000, 20}, {3000, 30}})
	c2 := makeChunk(t, [][2]int64{{1500, 5}, {2500, 15}})
	c3 := makeChunk(t, [][2]int64{{1000, 100}})

	if err := w.AddSeries(1, labels1, []*chunk.Chunk{c1, c2}); err != nil {
		t.Fatalf("AddSeries 1: %v", err)
	}
	if err := w.AddSeries(2, labels2, []*chunk.Chunk{c3}); err != nil {
		t.Fatalf("AddSeries 2: %v", err)
	}

	meta, err := w.Commit()
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if meta.NumSeries != 2 {
		t.Errorf("NumSeries = %d, want 2", meta.NumSeries)
	}
	if meta.NumSamples != 6 {
		t.Errorf("NumSamples = %d, want 6", meta.NumSamples)
	}

	// Block directory must exist at blocksDir/<block_id>
	blockDir := filepath.Join(blocksDir, meta.BlockID)
	for _, f := range []string{"meta.json", "index", "chunks"} {
		if _, err := os.Stat(filepath.Join(blockDir, f)); err != nil {
			t.Errorf("missing file %s: %v", f, err)
		}
	}
}

func TestWriter_Abort_CleansUpTempDir(t *testing.T) {
	dir := t.TempDir()
	blocksDir := filepath.Join(dir, "blocks")
	tmpDir := filepath.Join(dir, "tmp")
	_ = os.MkdirAll(tmpDir, 0o755)

	w, err := block.NewWriter(blocksDir, tmpDir)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	_ = w.AddSeries(1, []block.LabelPair{{"__name__", "x"}}, []*chunk.Chunk{makeChunk(t, [][2]int64{{1, 1}})})

	if err := w.Abort(); err != nil {
		t.Fatalf("Abort: %v", err)
	}

	entries, _ := os.ReadDir(tmpDir)
	if len(entries) != 0 {
		t.Errorf("tmp dir has %d entries after Abort, want 0", len(entries))
	}
}

func TestWriter_Commit_AtomicRename(t *testing.T) {
	w, blocksDir, _ := makeWriter(t)
	_ = w.AddSeries(1, []block.LabelPair{{"__name__", "x"}}, []*chunk.Chunk{makeChunk(t, [][2]int64{{1, 1}})})

	// Before Commit, blocksDir should not exist or be empty.
	entries, _ := os.ReadDir(blocksDir)
	if len(entries) != 0 {
		t.Fatalf("blocksDir has entries before Commit")
	}

	meta, err := w.Commit()
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	entries, _ = os.ReadDir(blocksDir)
	if len(entries) != 1 {
		t.Fatalf("blocksDir has %d entries after Commit, want 1", len(entries))
	}
	if entries[0].Name() != meta.BlockID {
		t.Errorf("dir name = %q, want block_id %q", entries[0].Name(), meta.BlockID)
	}
}

func TestReader_OpenReader_LoadsSeries(t *testing.T) {
	w, blocksDir, _ := makeWriter(t)
	labels := []block.LabelPair{{"__name__", "req"}, {"env", "prod"}}
	c := makeChunk(t, [][2]int64{{1000, 42}, {2000, 84}})
	_ = w.AddSeries(99, labels, []*chunk.Chunk{c})
	meta, err := w.Commit()
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	r, err := block.OpenReader(filepath.Join(blocksDir, meta.BlockID))
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	series := r.Series()
	if len(series) != 1 {
		t.Fatalf("Series() len = %d, want 1", len(series))
	}
	se := series[0]
	if se.ID != 99 {
		t.Errorf("ID = %d, want 99", se.ID)
	}
	if len(se.Labels) != 2 || se.Labels[0].Name != "__name__" || se.Labels[0].Value != "req" {
		t.Errorf("Labels = %v, want [{__name__ req} {env prod}]", se.Labels)
	}
	if len(se.Chunks) != 1 {
		t.Fatalf("Chunks len = %d, want 1", len(se.Chunks))
	}
}

func TestWriter_AddSeries_CanonicalizesUnsortedLabels(t *testing.T) {
	w, blocksDir, _ := makeWriter(t)
	// Labels deliberately out of name order; the writer must canonicalize them so
	// the block round-trips through the reader (which requires sorted names).
	unsorted := []block.LabelPair{{"job", "api"}, {"__name__", "http"}, {"instance", "i1"}}
	if err := w.AddSeries(1, unsorted, []*chunk.Chunk{makeChunk(t, [][2]int64{{1000, 1}})}); err != nil {
		t.Fatalf("AddSeries: %v", err)
	}
	meta, err := w.Commit()
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	r, err := block.OpenReader(filepath.Join(blocksDir, meta.BlockID))
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	se, ok := r.SeriesByID(1)
	if !ok {
		t.Fatal("SeriesByID(1): not found")
	}
	wantNames := []string{"__name__", "instance", "job"}
	for i, w := range wantNames {
		if se.Labels[i].Name != w {
			t.Errorf("Labels[%d].Name = %q, want %q (sorted)", i, se.Labels[i].Name, w)
		}
	}
}

func TestWriter_AddSeries_RejectsDuplicateLabelName(t *testing.T) {
	w, _, _ := makeWriter(t)
	dup := []block.LabelPair{{"job", "a"}, {"job", "b"}}
	if err := w.AddSeries(1, dup, []*chunk.Chunk{makeChunk(t, [][2]int64{{1000, 1}})}); err == nil {
		t.Fatal("AddSeries with duplicate label name: want error, got nil")
	}
}

// TestWriter_AddSeries_RejectsEmptyLabelName guards against committing a block
// that the reader cannot open: an empty-named pair (in particular the reserved
// ("","") pair) collides with the all-series postings sentinel.
func TestWriter_AddSeries_RejectsEmptyLabelName(t *testing.T) {
	cases := map[string][]block.LabelPair{
		"reserved_sentinel": {{"", ""}},
		"empty_name":        {{"__name__", "http"}, {"", "x"}},
	}
	for name, labels := range cases {
		t.Run(name, func(t *testing.T) {
			w, _, _ := makeWriter(t)
			if err := w.AddSeries(1, labels, []*chunk.Chunk{makeChunk(t, [][2]int64{{1000, 1}})}); err == nil {
				t.Fatal("AddSeries with empty label name: want error, got nil")
			}
		})
	}
}

// TestWriter_EmptyLabelName_WouldBreakReader documents the round-trip the
// rejection protects: a block whose label set contains an empty name must not be
// producible, because OpenReader would otherwise reject it after Commit.
func TestWriter_EmptyLabelName_WouldBreakReader(t *testing.T) {
	w, blocksDir, _ := makeWriter(t)
	// A normal series still round-trips cleanly.
	if err := w.AddSeries(1, []block.LabelPair{{"__name__", "http"}, {"job", "api"}}, []*chunk.Chunk{makeChunk(t, [][2]int64{{1000, 1}})}); err != nil {
		t.Fatalf("AddSeries: %v", err)
	}
	meta, err := w.Commit()
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	r, err := block.OpenReader(filepath.Join(blocksDir, meta.BlockID))
	if err != nil {
		t.Fatalf("OpenReader on valid block: %v", err)
	}
	r.Close()
}

func TestWriter_AddSeries_RejectsDuplicateID(t *testing.T) {
	w, _, _ := makeWriter(t)
	if err := w.AddSeries(5, []block.LabelPair{{"__name__", "a"}}, []*chunk.Chunk{makeChunk(t, [][2]int64{{1000, 1}})}); err != nil {
		t.Fatalf("first AddSeries: %v", err)
	}
	if err := w.AddSeries(5, []block.LabelPair{{"__name__", "b"}}, []*chunk.Chunk{makeChunk(t, [][2]int64{{1000, 2}})}); err == nil {
		t.Fatal("second AddSeries with duplicate ID: want error, got nil")
	}
}

func TestReader_SeriesByID(t *testing.T) {
	w, blocksDir, _ := makeWriter(t)
	_ = w.AddSeries(7, []block.LabelPair{{"__name__", "req"}}, []*chunk.Chunk{makeChunk(t, [][2]int64{{1000, 1}})})
	_ = w.AddSeries(42, []block.LabelPair{{"__name__", "lat"}}, []*chunk.Chunk{makeChunk(t, [][2]int64{{1000, 2}})})
	meta, err := w.Commit()
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	r, err := block.OpenReader(filepath.Join(blocksDir, meta.BlockID))
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	se, ok := r.SeriesByID(42)
	if !ok {
		t.Fatal("SeriesByID(42): want found, got missing")
	}
	if se.ID != 42 || len(se.Labels) != 1 || se.Labels[0].Value != "lat" {
		t.Errorf("SeriesByID(42) = %+v, want id 42 / lat", se)
	}
	if _, ok := r.SeriesByID(999); ok {
		t.Error("SeriesByID(999): want missing, got found")
	}
}

func TestReader_ReadChunk_RoundTrip(t *testing.T) {
	w, blocksDir, _ := makeWriter(t)
	samples := [][2]int64{{1000, 10}, {2000, 20}, {3000, 30}}
	c := makeChunk(t, samples)
	_ = w.AddSeries(7, []block.LabelPair{{"__name__", "x"}}, []*chunk.Chunk{c})
	meta, err := w.Commit()
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	r, err := block.OpenReader(filepath.Join(blocksDir, meta.BlockID))
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	ref := r.Series()[0].Chunks[0]
	got, err := r.ReadChunk(ref)
	if err != nil {
		t.Fatalf("ReadChunk: %v", err)
	}
	it := got.Iterator()
	var n int
	for it.Next() {
		ts, val := it.At()
		if ts != samples[n][0] || val != float64(samples[n][1]) {
			t.Errorf("sample[%d] = {%d, %v}, want {%d, %v}", n, ts, val, samples[n][0], samples[n][1])
		}
		n++
	}
	if n != 3 {
		t.Errorf("got %d samples, want 3", n)
	}
}

func TestReader_OpenReader_MissingMetaJson(t *testing.T) {
	dir := t.TempDir()
	blockDir := filepath.Join(dir, "deadbeef12345678")
	_ = os.MkdirAll(blockDir, 0o755)

	_, err := block.OpenReader(blockDir)
	if err == nil {
		t.Fatal("expected error for missing meta.json, got nil")
	}
}

func TestReader_ReadChunk_CorruptPayload(t *testing.T) {
	w, blocksDir, _ := makeWriter(t)
	c := makeChunk(t, [][2]int64{{1000, 1}, {2000, 2}})
	_ = w.AddSeries(1, []block.LabelPair{{"__name__", "x"}}, []*chunk.Chunk{c})
	meta, err := w.Commit()
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Corrupt the chunks file payload bytes (after the 12-byte header).
	chunksPath := filepath.Join(blocksDir, meta.BlockID, "chunks")
	data, _ := os.ReadFile(chunksPath)
	for i := 12; i < len(data); i++ {
		data[i] = 0
	}
	_ = os.WriteFile(chunksPath, data, 0o644)

	r, err := block.OpenReader(filepath.Join(blocksDir, meta.BlockID))
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	ref := r.Series()[0].Chunks[0]
	_, err = r.ReadChunk(ref)
	if err == nil {
		t.Fatal("expected error for corrupt chunk payload, got nil")
	}
}
