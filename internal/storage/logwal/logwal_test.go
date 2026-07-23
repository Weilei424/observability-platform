package logwal

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/storage/fsutil"
)

func TestOpen_FsyncsSegmentDirectory(t *testing.T) {
	var synced []string
	restore := fsutil.SyncDir
	fsutil.SyncDir = func(dir string) error { synced = append(synced, dir); return restore(dir) }
	defer func() { fsutil.SyncDir = restore }()

	dir := t.TempDir()
	w, err := Open(dir, 1<<20, 1)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w.Close()
	found := false
	for _, d := range synced {
		if d == dir {
			found = true
		}
	}
	if !found {
		t.Errorf("expected SyncDir(%q) for the new segment's directory, got %v", dir, synced)
	}
}

func TestOpen_DirSyncFailurePropagates(t *testing.T) {
	restore := fsutil.SyncDir
	fsutil.SyncDir = func(string) error { return errors.New("boom") }
	defer func() { fsutil.SyncDir = restore }()

	if _, err := Open(t.TempDir(), 1<<20, 1); err == nil {
		t.Fatal("Open should fail when the segment-directory fsync fails")
	}
}

func TestRotation_SyncsUnsyncedRemainder(t *testing.T) {
	dir := t.TempDir()
	// syncEveryN huge so no periodic fsync fires; tiny segMaxBytes forces a
	// rotation while the just-written record is still unsynced.
	w, err := Open(dir, 8, 1000)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w.Close()
	if err := w.WriteRecord([]LabelPair{{"service", "api"}}, 1, "line"); err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}
	if w.preRotationSyncs == 0 {
		t.Error("expected a pre-rotation fsync of the unsynced remainder")
	}
	if w.autoSyncs != 0 {
		t.Errorf("autoSyncs = %d, want 0 (syncEveryN=1000 must not periodically sync)", w.autoSyncs)
	}
}

func TestLogWAL_WriteAndRotate(t *testing.T) {
	dir := t.TempDir()
	// segMaxBytes small so a couple of records force a rotation.
	w, err := Open(dir, 40, 1)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	startSeg := w.SegmentIndex()
	for i := 0; i < 5; i++ {
		if err := w.WriteRecord([]LabelPair{{"service", "api"}}, int64(i+1), "some log line here"); err != nil {
			t.Fatalf("WriteRecord %d: %v", i, err)
		}
	}
	if w.SegmentIndex() == startSeg {
		t.Errorf("expected segment rotation, still at %d", startSeg)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	var walFiles int
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".wal" {
			walFiles++
		}
	}
	if walFiles < 2 {
		t.Errorf("expected ≥2 segment files after rotation, got %d", walFiles)
	}
}

func TestLogWAL_CloseIdempotent(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, 1<<20, 1)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := w.WriteRecord(nil, 1, "x"); err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Errorf("second Close should be a no-op, got: %v", err)
	}
}

func TestSegmentPaths_NumericOrderAcrossRollover(t *testing.T) {
	dir := t.TempDir()
	// %06d is a minimum width, so once the index needs a 7th digit a lexical sort
	// misorders: "1000000.wal" < "999999.wal". Create names spanning that boundary
	// out of order and require segmentPaths to return them numerically ascending.
	names := []string{"1000001.wal", "999999.wal", "1000000.wal", "100000.wal", "000001.wal"}
	for _, n := range names {
		if err := os.WriteFile(filepath.Join(dir, n), nil, 0o644); err != nil {
			t.Fatalf("write %s: %v", n, err)
		}
	}
	paths, err := segmentPaths(dir)
	if err != nil {
		t.Fatalf("segmentPaths: %v", err)
	}
	want := []string{"000001.wal", "100000.wal", "999999.wal", "1000000.wal", "1000001.wal"}
	if len(paths) != len(want) {
		t.Fatalf("got %d paths, want %d: %v", len(paths), len(want), paths)
	}
	for i, p := range paths {
		if got := filepath.Base(p); got != want[i] {
			t.Errorf("paths[%d] = %s, want %s", i, got, want[i])
		}
	}
}

func TestLogWAL_WriteAfterClose_ReturnsError(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, 1<<20, 1)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Must return an error, not panic on a nil *os.File dereference.
	if err := w.WriteRecord(nil, 1, "x"); err == nil {
		t.Error("WriteRecord after Close should return an error")
	}
}

func TestLogWAL_SyncAfterClose_ReturnsError(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, 1<<20, 1)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := w.Sync(); err == nil {
		t.Error("Sync after Close should return an error")
	}
}

func TestLogWAL_SyncEveryNAutomaticBoundary(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, 1<<20, 3)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w.Close()
	writeN := func(n int) {
		for i := 0; i < n; i++ {
			if err := w.WriteRecord([]LabelPair{{"service", "api"}}, 1, "line"); err != nil {
				t.Fatalf("WriteRecord: %v", err)
			}
		}
	}

	// Below the syncEveryN=3 threshold: no automatic fsync yet. This asserts the
	// boundary itself, not just eventual durability (which Close would mask).
	writeN(2)
	if w.autoSyncs != 0 {
		t.Fatalf("autoSyncs = %d after 2 writes, want 0 (below boundary)", w.autoSyncs)
	}
	// The 3rd write crosses the boundary → exactly one automatic fsync.
	writeN(1)
	if w.autoSyncs != 1 {
		t.Fatalf("autoSyncs = %d after 3 writes, want 1 (boundary must fire automatically)", w.autoSyncs)
	}
	// Three more writes → a second automatic fsync at the next boundary.
	writeN(3)
	if w.autoSyncs != 2 {
		t.Fatalf("autoSyncs = %d after 6 writes, want 2", w.autoSyncs)
	}
}

func TestLogWAL_FreshSegmentOnReopen(t *testing.T) {
	dir := t.TempDir()
	w1, err := Open(dir, 1<<20, 1)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	seg1 := w1.SegmentIndex()
	if err := w1.WriteRecord(nil, 1, "x"); err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}
	if err := w1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	w2, err := Open(dir, 1<<20, 1)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()
	if w2.SegmentIndex() <= seg1 {
		t.Errorf("reopened segment index %d should be greater than %d", w2.SegmentIndex(), seg1)
	}
}

func TestLogWAL_Checkpoint_DropsFlushedSegments(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, 1<<20, 1)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := w.WriteRecord(nil, 1, "flushed-a"); err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}
	if err := w.WriteRecord(nil, 2, "flushed-b"); err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}
	if err := w.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := w.WriteRecord(nil, 3, "post-checkpoint"); err != nil {
		t.Fatalf("WriteRecord after checkpoint: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var lines []string
	if err := Replay(dir, func(_ []LabelPair, _ int64, line string) {
		lines = append(lines, line)
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(lines) != 1 || lines[0] != "post-checkpoint" {
		t.Fatalf("replay = %v, want only [post-checkpoint]", lines)
	}
}

func TestLogWAL_Checkpoint_OnClosedWAL_ReturnsError(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, 1<<20, 1)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := w.Checkpoint(); err == nil {
		t.Error("Checkpoint after Close should return an error")
	}
}

func TestLogWAL_Checkpoint_SyncFailure_PropagatesBeforeDelete(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, 1<<20, 1)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := w.WriteRecord(nil, 1, "a"); err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}

	// Inject a sync failure on the checkpoint path.
	restore := syncFile
	syncFile = func(*os.File) error { return errors.New("boom") }
	defer func() { syncFile = restore }()

	if err := w.Checkpoint(); err == nil {
		t.Fatal("Checkpoint should fail when the outgoing segment's sync fails")
	}
	// The failure must occur BEFORE close/delete: the segment file must survive.
	if paths, _ := segmentPaths(dir); len(paths) == 0 {
		t.Fatal("no segments should be deleted when the checkpoint sync fails")
	}

	// The WAL must remain usable and the checkpoint retryable once sync recovers.
	syncFile = restore
	if err := w.WriteRecord(nil, 2, "b"); err != nil {
		t.Fatalf("WAL should stay usable after a failed checkpoint: %v", err)
	}
	if err := w.Checkpoint(); err != nil {
		t.Fatalf("retry Checkpoint should succeed once sync recovers: %v", err)
	}
	if err := w.WriteRecord(nil, 3, "c"); err != nil {
		t.Fatalf("WriteRecord after successful checkpoint: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// After the successful checkpoint, replay sees only the post-checkpoint record.
	var lines []string
	if err := Replay(dir, func(_ []LabelPair, _ int64, line string) {
		lines = append(lines, line)
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(lines) != 1 || lines[0] != "c" {
		t.Fatalf("replay = %v, want only [c]", lines)
	}
}

func TestLogWAL_Checkpoint_DropsMultipleSegments(t *testing.T) {
	dir := t.TempDir()
	// Tiny segMaxBytes forces a rotation on every write, so several segments exist
	// on disk before the checkpoint — exercising the delete-ALL-segments loop.
	w, err := Open(dir, 8, 1)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 4; i++ {
		if err := w.WriteRecord(nil, int64(i+1), "flushed"); err != nil {
			t.Fatalf("WriteRecord %d: %v", i, err)
		}
	}
	before, err := segmentPaths(dir)
	if err != nil {
		t.Fatalf("segmentPaths: %v", err)
	}
	if len(before) < 2 {
		t.Fatalf("expected >=2 segments before checkpoint, got %d", len(before))
	}
	if err := w.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := w.WriteRecord(nil, 99, "post"); err != nil {
		t.Fatalf("WriteRecord after checkpoint: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	var lines []string
	if err := Replay(dir, func(_ []LabelPair, _ int64, line string) {
		lines = append(lines, line)
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(lines) != 1 || lines[0] != "post" {
		t.Fatalf("replay = %v, want only [post] (all flushed segments dropped)", lines)
	}
}
