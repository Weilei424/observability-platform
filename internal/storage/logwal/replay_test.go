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
