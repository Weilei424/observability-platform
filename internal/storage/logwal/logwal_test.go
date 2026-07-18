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
