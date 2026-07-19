package logwal

import (
	"os"
	"path/filepath"
	"testing"
)

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

func TestLogWAL_SyncEveryNDurability(t *testing.T) {
	dir := t.TempDir()
	// syncEveryN=3 means the final <3 records are only flushed by an explicit
	// Sync/Close, not by the per-record boundary. Everything must still be durable.
	w, err := Open(dir, 1<<20, 3)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 5; i++ {
		if err := w.WriteRecord([]LabelPair{{"service", "api"}}, int64(i+1), "line"); err != nil {
			t.Fatalf("WriteRecord %d: %v", i, err)
		}
	}
	if err := w.Sync(); err != nil { // explicit Sync flushes the trailing 2 records
		t.Fatalf("Sync: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var count int
	if err := Replay(dir, func([]LabelPair, int64, string) { count++ }); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if count != 5 {
		t.Errorf("replayed %d records, want 5 (all durable under syncEveryN=3)", count)
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
