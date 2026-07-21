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
