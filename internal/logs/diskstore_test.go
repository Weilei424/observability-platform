package logs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/storage/index"
)

func newTestStore(t *testing.T, dir string, flushThreshold int) *Store {
	t.Helper()
	s, err := NewStore(
		filepath.Join(dir, "wal"),
		filepath.Join(dir, "chunks"),
		filepath.Join(dir, "index"),
		1<<20, 1, flushThreshold,
	)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	return s
}

func TestStore_ThresholdFlushWritesChunksAndManifest(t *testing.T) {
	dir := t.TempDir()
	s := newTestStore(t, dir, 1) // tiny threshold: flush after first append
	labels := mustLabels(t, map[string]string{"service": "api"})
	if err := s.Append(labels, 100, "hello"); err != nil {
		t.Fatalf("Append: %v", err)
	}

	chunks, _ := os.ReadDir(filepath.Join(dir, "chunks"))
	if len(chunks) == 0 {
		t.Fatal("expected a chunk file after threshold flush")
	}
	if _, err := os.Stat(filepath.Join(dir, "index", "streams.index")); err != nil {
		t.Fatalf("expected manifest: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestStore_SurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	labels := mustLabels(t, map[string]string{"service": "api"})

	s := newTestStore(t, dir, 1<<30) // no threshold flush; flush happens on Close
	if err := s.Append(labels, 100, "a"); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := s.Append(labels, 200, "b"); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := s.Close(); err != nil { // flushes head to chunks + checkpoints WAL
		t.Fatalf("Close: %v", err)
	}

	s2 := newTestStore(t, dir, 1<<30)
	defer s2.Close()
	id := StreamIDOf(labels)
	got, err := s2.StreamEntries(id, 0, 1000)
	if err != nil {
		t.Fatalf("StreamEntries: %v", err)
	}
	if len(got) != 2 || got[0].Line != "a" || got[1].Line != "b" {
		t.Fatalf("after restart entries = %+v, want a,b", got)
	}
}

func TestStore_CheckpointPreventsDoubleCount(t *testing.T) {
	dir := t.TempDir()
	labels := mustLabels(t, map[string]string{"service": "api"})

	s := newTestStore(t, dir, 1) // flush after each append
	if err := s.Append(labels, 100, "a"); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2 := newTestStore(t, dir, 1<<30)
	defer s2.Close()
	got, err := s2.StreamEntries(StreamIDOf(labels), 0, 1000)
	if err != nil {
		t.Fatalf("StreamEntries: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("entries = %d, want 1 (no double count from WAL + chunk)", len(got))
	}
}

func TestStore_LabelFilterNarrows(t *testing.T) {
	dir := t.TempDir()
	s := newTestStore(t, dir, 1<<30)
	defer s.Close()
	api := mustLabels(t, map[string]string{"service": "api"})
	web := mustLabels(t, map[string]string{"service": "web"})
	_ = s.Append(api, 100, "x")
	_ = s.Append(web, 100, "y")

	got := s.MatchingStreamIDs([]index.Pair{{Name: "service", Value: "api"}})
	if len(got) != 1 || got[0] != StreamIDOf(api) {
		t.Fatalf("matching = %v, want [api]", got)
	}
}

func TestStore_MergeDedupsCrashWindow(t *testing.T) {
	// Simulate the crash window: a chunk was written but the WAL was NOT
	// checkpointed, so a WAL replay reintroduces the same entry. The merged read
	// must dedup by (streamID, tsNs, line).
	dir := t.TempDir()
	labels := mustLabels(t, map[string]string{"service": "api"})
	id := StreamIDOf(labels)

	s := newTestStore(t, dir, 1<<30)
	// One entry in the head, backed by the WAL.
	if err := s.Append(labels, 100, "dup"); err != nil {
		t.Fatalf("Append: %v", err)
	}
	// Force just the chunk write + index (no checkpoint) to mimic a crash after
	// chunks but before WAL checkpoint.
	if err := s.writeChunksAndIndexForTest(); err != nil {
		t.Fatalf("writeChunksAndIndexForTest: %v", err)
	}
	if err := s.closeWALForTest(); err != nil {
		t.Fatalf("closeWALForTest: %v", err)
	}

	s2 := newTestStore(t, dir, 1<<30) // manifest has the chunk; WAL still has "dup"
	defer s2.Close()
	got, err := s2.StreamEntries(id, 0, 1000)
	if err != nil {
		t.Fatalf("StreamEntries: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("entries = %d, want 1 (crash-window duplicate must be deduped)", len(got))
	}
}
