package logs

import (
	"os"
	"path/filepath"
	"reflect"
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

func TestLoadManifest_DetectsBodyCorruption(t *testing.T) {
	dir := t.TempDir()
	chunksDir := filepath.Join(dir, "chunks")
	if err := os.MkdirAll(chunksDir, 0o755); err != nil {
		t.Fatal(err)
	}
	x := newStreamIndex()
	addChunk(t, chunksDir, x, map[string]string{"service": "api"}, 100, 200)
	manifest := filepath.Join(dir, "streams.index")
	if err := x.writeManifest(manifest); err != nil {
		t.Fatalf("writeManifest: %v", err)
	}
	// Flip a byte in the manifest body (past magic+version+crc); the CRC must catch
	// this same-length corruption that the structural checks would otherwise miss.
	data, err := os.ReadFile(manifest)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xff
	if err := os.WriteFile(manifest, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadManifest(manifest); err == nil {
		t.Error("expected a checksum-mismatch error on body corruption")
	}
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

// assertSameIndex proves the spec invariant that loadManifest and rebuildFromScan
// yield an identical index: same stream set, same ChunkRef VALUES per stream (as a
// set — rebuildFromScan may order refs differently), and same labels per stream.
func assertSameIndex(t *testing.T, want, got *streamIndex) {
	t.Helper()
	if len(want.refs) != len(got.refs) {
		t.Fatalf("stream count: got %d, want %d", len(got.refs), len(want.refs))
	}
	for id, wrefs := range want.refs {
		grefs, ok := got.refs[id]
		if !ok {
			t.Fatalf("stream %d missing from rebuilt index", id)
		}
		if len(grefs) != len(wrefs) {
			t.Fatalf("stream %d: %d refs, want %d", id, len(grefs), len(wrefs))
		}
		wset := make(map[ChunkRef]struct{}, len(wrefs))
		for _, r := range wrefs {
			wset[r] = struct{}{}
		}
		for _, r := range grefs {
			if _, in := wset[r]; !in {
				t.Errorf("stream %d: ref %+v not present in the other index", id, r)
			}
		}
		if !reflect.DeepEqual(want.labels[id].Map(), got.labels[id].Map()) {
			t.Errorf("stream %d: labels differ: %v vs %v", id, got.labels[id].Map(), want.labels[id].Map())
		}
	}
	if want.postings.SeriesCount() != got.postings.SeriesCount() {
		t.Fatalf("postings series count %d != %d", got.postings.SeriesCount(), want.postings.SeriesCount())
	}
}
