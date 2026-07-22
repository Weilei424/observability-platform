package logs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/storage/index"
	"github.com/masonwheeler/observability-platform/internal/storage/logchunk"
)

func addChunk(t *testing.T, dir string, x *streamIndex, m map[string]string, tss ...int64) {
	t.Helper()
	labels, err := NewStreamLabels(m)
	if err != nil {
		t.Fatalf("labels: %v", err)
	}
	id := StreamIDOf(labels)
	c := logchunk.NewChunk()
	for _, ts := range tss {
		c.Append(ts, "line")
	}
	ref, err := writeChunkFile(dir, id, labels, c)
	if err != nil {
		t.Fatalf("writeChunkFile: %v", err)
	}
	x.add(id, labels, ref)
}

func TestStreamIndex_LabelFilterNarrows(t *testing.T) {
	dir := t.TempDir()
	x := newStreamIndex()
	addChunk(t, dir, x, map[string]string{"service": "api"}, 100)
	addChunk(t, dir, x, map[string]string{"service": "web"}, 100)

	got := x.matchingStreamIDs([]index.Pair{{Name: "service", Value: "api"}})
	apiID := StreamIDOf(mustLabels(t, map[string]string{"service": "api"}))
	if len(got) != 1 || got[0] != apiID {
		t.Fatalf("matching = %v, want [%d]", got, apiID)
	}
}

func TestStreamIndex_ChunkRefsTimeFilter(t *testing.T) {
	dir := t.TempDir()
	x := newStreamIndex()
	labels := mustLabels(t, map[string]string{"service": "api"})
	id := StreamIDOf(labels)
	addChunk(t, dir, x, map[string]string{"service": "api"}, 100, 200) // chunk A [100,200]
	addChunk(t, dir, x, map[string]string{"service": "api"}, 500, 600) // chunk B [500,600]

	if refs := x.chunkRefs(id, 550, 900); len(refs) != 1 || refs[0].MinTs != 500 {
		t.Fatalf("time filter = %v, want only chunk B", refs)
	}
	if refs := x.chunkRefs(id, 0, 1000); len(refs) != 2 {
		t.Fatalf("wide range = %d refs, want 2", len(refs))
	}
}

func TestStreamIndex_ManifestRoundTripAndRebuildMatch(t *testing.T) {
	dir := t.TempDir()
	chunksDir := filepath.Join(dir, "chunks")
	if err := os.MkdirAll(chunksDir, 0o755); err != nil {
		t.Fatal(err)
	}
	x := newStreamIndex()
	addChunk(t, chunksDir, x, map[string]string{"service": "api"}, 100, 200)
	addChunk(t, chunksDir, x, map[string]string{"service": "web"}, 300)

	manifest := filepath.Join(dir, "streams.index")
	if err := x.writeManifest(manifest); err != nil {
		t.Fatalf("writeManifest: %v", err)
	}
	loaded, err := loadManifest(manifest)
	if err != nil {
		t.Fatalf("loadManifest: %v", err)
	}
	rebuilt, err := rebuildFromScan(chunksDir)
	if err != nil {
		t.Fatalf("rebuildFromScan: %v", err)
	}
	assertSameIndex(t, x, loaded)
	assertSameIndex(t, x, rebuilt)
}

func TestLoadManifest_CorruptReturnsError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "streams.index")
	if err := os.WriteFile(path, []byte("not a manifest"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadManifest(path); err == nil {
		t.Error("expected error loading corrupt manifest")
	}
}

// mustLabels is defined in store_test.go and reused here.

func assertSameIndex(t *testing.T, want, got *streamIndex) {
	t.Helper()
	for id, refs := range want.refs {
		g := got.refs[id]
		if len(g) != len(refs) {
			t.Fatalf("stream %d: %d refs, want %d", id, len(g), len(refs))
		}
	}
	if want.postings.SeriesCount() != got.postings.SeriesCount() {
		t.Fatalf("postings series count %d != %d", got.postings.SeriesCount(), want.postings.SeriesCount())
	}
}
