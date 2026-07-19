package logwal

import (
	"os"
	"path/filepath"
	"testing"
)

func collect(t *testing.T, dir string) []LogRecordSeen {
	t.Helper()
	var seen []LogRecordSeen
	if err := Replay(dir, func(labels []LabelPair, tsNs int64, line string) {
		seen = append(seen, LogRecordSeen{Labels: labels, TsNs: tsNs, Line: line})
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	return seen
}

type LogRecordSeen struct {
	Labels []LabelPair
	TsNs   int64
	Line   string
}

func TestReplay_RestoresRecordsInOrder(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, 1<<20, 1)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := w.WriteRecord([]LabelPair{{"service", "api"}}, int64(i+1), "line"); err != nil {
			t.Fatalf("WriteRecord: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	seen := collect(t, dir)
	if len(seen) != 3 {
		t.Fatalf("replayed %d records, want 3", len(seen))
	}
	for i, r := range seen {
		if r.TsNs != int64(i+1) {
			t.Errorf("record %d tsNs = %d, want %d", i, r.TsNs, i+1)
		}
	}
}

func TestReplay_PartialTrailingRecordDiscarded(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, 1<<20, 1)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := w.WriteRecord([]LabelPair{{"service", "api"}}, 7, "good"); err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}
	seg := w.segmentPath(w.SegmentIndex())
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Append a truncated record (length prefix promising more than is present).
	f, err := os.OpenFile(seg, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open seg: %v", err)
	}
	if _, err := f.Write([]byte{0x00, 0x00, 0x00, 0x50, 0x01, 0x00}); err != nil {
		t.Fatalf("write partial: %v", err)
	}
	f.Close()

	seen := collect(t, dir)
	if len(seen) != 1 {
		t.Fatalf("replayed %d records, want 1 (partial trailing discarded)", len(seen))
	}
}

// appendBytes appends raw bytes to a file (used to simulate a torn write).
func appendBytes(t *testing.T, path string, b []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	if _, err := f.Write(b); err != nil {
		t.Fatalf("append to %s: %v", path, err)
	}
	f.Close()
}

// TestReplay_TornTailSurvivesRepeatedRestart is the regression guard for the
// recovery bug where a tolerated partial tail on the final segment was never
// truncated: once Open started a fresh segment, the torn segment was no longer
// final and the NEXT replay aborted startup. Replay must repair the tail so
// every subsequent restart is clean.
func TestReplay_TornTailSurvivesRepeatedRestart(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir, 1<<20, 1)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := w.WriteRecord([]LabelPair{{"service", "api"}}, 7, "good"); err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}
	seg := w.segmentPath(w.SegmentIndex())
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Simulate a torn write: a length prefix promising more than is present.
	appendBytes(t, seg, []byte{0x00, 0x00, 0x00, 0x50, 0x01, 0x00})

	// Restart #1: replay tolerates and repairs the torn tail.
	if seen := collect(t, dir); len(seen) != 1 {
		t.Fatalf("restart #1 replayed %d records, want 1", len(seen))
	}

	// A fresh Open starts a new segment; the torn segment is no longer final.
	w2, err := Open(dir, 1<<20, 1)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if err := w2.WriteRecord([]LabelPair{{"service", "api"}}, 8, "after"); err != nil {
		t.Fatalf("WriteRecord after reopen: %v", err)
	}
	if err := w2.Close(); err != nil {
		t.Fatalf("Close 2: %v", err)
	}

	// Restart #2: replay must NOT abort on the now-non-final repaired segment.
	seen := collect(t, dir)
	if len(seen) != 2 {
		t.Fatalf("restart #2 replayed %d records, want 2 (torn tail must not abort startup)", len(seen))
	}
	if seen[0].TsNs != 7 || seen[1].TsNs != 8 {
		t.Errorf("restart #2 records = %+v, want tsNs 7 then 8", seen)
	}
}

func TestReplay_OversizedTailTruncatedAndSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, 1<<20, 1)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := w.WriteRecord([]LabelPair{{"service", "api"}}, 1, "good"); err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}
	seg := w.segmentPath(w.SegmentIndex())
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// A length prefix far larger than maxRecordBodyBytes (a corrupt/torn header).
	appendBytes(t, seg, []byte{0xFF, 0xFF, 0xFF, 0xFF})

	if seen := collect(t, dir); len(seen) != 1 {
		t.Fatalf("first replay = %d records, want 1", len(seen))
	}
	// Force the repaired segment to be non-final, then replay again: no error.
	if err := os.WriteFile(filepath.Join(dir, "000009.wal"), nil, 0o644); err != nil {
		t.Fatalf("write later segment: %v", err)
	}
	if seen := collect(t, dir); len(seen) != 1 {
		t.Fatalf("second replay = %d records, want 1 (oversized tail must be repaired)", len(seen))
	}
}

func TestReplay_PartialLengthPrefixTruncatedAndSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, 1<<20, 1)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := w.WriteRecord([]LabelPair{{"service", "api"}}, 1, "good"); err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}
	seg := w.segmentPath(w.SegmentIndex())
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Fewer than 4 bytes: a torn length prefix.
	appendBytes(t, seg, []byte{0x00, 0x00})

	if seen := collect(t, dir); len(seen) != 1 {
		t.Fatalf("first replay = %d records, want 1", len(seen))
	}
	if err := os.WriteFile(filepath.Join(dir, "000009.wal"), nil, 0o644); err != nil {
		t.Fatalf("write later segment: %v", err)
	}
	if seen := collect(t, dir); len(seen) != 1 {
		t.Fatalf("second replay = %d records, want 1 (partial prefix must be repaired)", len(seen))
	}
}

func TestReplay_CorruptNonFinalSegmentErrors(t *testing.T) {
	dir := t.TempDir()
	// Two segments: 000001 (corrupt, non-final) and 000002 (valid, final).
	corrupt := []byte{0x00, 0x00, 0x00, 0x04, 0x01, 0x00, 0x00, 0x00} // body len 4, undecodable body
	if err := os.WriteFile(filepath.Join(dir, "000001.wal"), corrupt, 0o644); err != nil {
		t.Fatalf("write seg1: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "000002.wal"), nil, 0o644); err != nil {
		t.Fatalf("write seg2: %v", err)
	}
	err := Replay(dir, func([]LabelPair, int64, string) {})
	if err == nil {
		t.Error("expected error for corrupt record in a non-final segment")
	}
}
