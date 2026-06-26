package block_test

import (
	"testing"

	"github.com/masonwheeler/observability-platform/internal/storage/block"
	"github.com/masonwheeler/observability-platform/internal/storage/chunk"
)

func TestCommit_DefaultLevelIsOne(t *testing.T) {
	w, blocks, _ := makeWriter(t)
	if err := w.AddSeries(1, []block.LabelPair{{Name: "__name__", Value: "x"}},
		[]*chunk.Chunk{makeChunk(t, [][2]int64{{1000, 1}})}); err != nil {
		t.Fatalf("AddSeries: %v", err)
	}
	meta, err := w.Commit()
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if meta.EffectiveLevel() != 1 {
		t.Fatalf("EffectiveLevel = %d, want 1", meta.EffectiveLevel())
	}
	got, err := block.ReadMeta(blocks + "/" + meta.BlockID)
	if err != nil {
		t.Fatalf("ReadMeta: %v", err)
	}
	if got.Level != 1 || len(got.Sources) != 0 {
		t.Fatalf("read meta level=%d sources=%v, want level=1 sources=[]", got.Level, got.Sources)
	}
}

func TestCommit_SetCompaction_WritesLevelAndSources(t *testing.T) {
	w, blocks, _ := makeWriter(t)
	w.SetCompaction(3, []string{"aaa", "bbb"})
	if err := w.AddSeries(1, []block.LabelPair{{Name: "__name__", Value: "x"}},
		[]*chunk.Chunk{makeChunk(t, [][2]int64{{1000, 1}})}); err != nil {
		t.Fatalf("AddSeries: %v", err)
	}
	meta, err := w.Commit()
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	got, err := block.ReadMeta(blocks + "/" + meta.BlockID)
	if err != nil {
		t.Fatalf("ReadMeta: %v", err)
	}
	if got.Level != 3 || len(got.Sources) != 2 || got.Sources[0] != "aaa" || got.Sources[1] != "bbb" {
		t.Fatalf("read meta = %+v, want level=3 sources=[aaa bbb]", got)
	}
}

func TestEffectiveLevel_ZeroIsOne(t *testing.T) {
	if (block.Meta{Level: 0}).EffectiveLevel() != 1 {
		t.Fatal("EffectiveLevel(0) should be 1")
	}
	if (block.Meta{Level: 2}).EffectiveLevel() != 2 {
		t.Fatal("EffectiveLevel(2) should be 2")
	}
}
