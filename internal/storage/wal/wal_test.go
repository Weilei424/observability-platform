package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func sampleLabels(name string) []LabelPair {
	return []LabelPair{{Name: "__name__", Value: name}, {Name: "env", Value: "test"}}
}

func TestWAL_Open_CreatesSegment(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, 128<<20, 1)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w.Close()

	entries, _ := os.ReadDir(dir)
	var walFiles []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".wal") {
			walFiles = append(walFiles, e.Name())
		}
	}
	if len(walFiles) != 1 {
		t.Errorf("expected 1 segment file, got %d: %v", len(walFiles), walFiles)
	}
}

func TestWAL_SegmentRotation(t *testing.T) {
	dir := t.TempDir()
	// segMaxBytes=1 forces rotation after every write (any record is >1 byte).
	w, err := Open(dir, 1, 1)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	for i := 0; i < 3; i++ {
		if err := w.WriteRecord(sampleLabels(fmt.Sprintf("m%d", i)), int64(i*1000), float64(i)); err != nil {
			t.Fatalf("WriteRecord %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	paths, err := segmentPaths(dir)
	if err != nil {
		t.Fatalf("segmentPaths: %v", err)
	}
	if len(paths) < 2 {
		t.Errorf("expected >= 2 segment files after rotation, got %d", len(paths))
	}

	// Replay all segments and verify 3 records come back.
	var count int
	if err := Replay(dir, func(_ []LabelPair, _ int64, _ float64) { count++ }); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if count != 3 {
		t.Errorf("replayed %d records, want 3", count)
	}
}

func TestWAL_SyncEveryN(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, 128<<20, 3)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w.Close()

	for i := 0; i < 6; i++ {
		if err := w.WriteRecord(sampleLabels("m"), int64(i*1000), float64(i)); err != nil {
			t.Fatalf("WriteRecord %d: %v", i, err)
		}
	}

	// Verify that all 6 records are readable (sync happened at N=3 and N=6).
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	var count int
	if err := Replay(dir, func(_ []LabelPair, _ int64, _ float64) { count++ }); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if count != 6 {
		t.Errorf("replayed %d records, want 6", count)
	}
}

func TestWAL_WriteAfterReopen(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir, 128<<20, 1)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := w.WriteRecord(sampleLabels("m"), 1000, 1.0); err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen — Open must start a new segment past the highest existing one.
	w2, err := Open(dir, 128<<20, 1)
	if err != nil {
		t.Fatalf("Open second: %v", err)
	}
	if err := w2.WriteRecord(sampleLabels("m2"), 2000, 2.0); err != nil {
		t.Fatalf("WriteRecord second: %v", err)
	}
	if err := w2.Close(); err != nil {
		t.Fatalf("Close second: %v", err)
	}

	paths, err := segmentPaths(dir)
	if err != nil {
		t.Fatalf("segmentPaths: %v", err)
	}
	// First session writes to 000001.wal, second session must start at 000002.wal.
	if len(paths) < 2 {
		t.Fatalf("expected >= 2 segment files, got %d: %v", len(paths), paths)
	}
	if filepath.Base(paths[len(paths)-1]) == "000001.wal" {
		t.Error("second Open should have started a new segment, not reused 000001.wal")
	}

	var count int
	if err := Replay(dir, func(_ []LabelPair, _ int64, _ float64) { count++ }); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if count != 2 {
		t.Errorf("replayed %d records, want 2", count)
	}
}

func TestReplay_AllRecords(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, 128<<20, 1)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	want := []struct {
		name string
		ts   int64
		val  float64
	}{
		{"cpu_usage", 1000, 0.5},
		{"mem_bytes", 2000, 1024.0},
		{"req_total", 3000, 99.0},
	}
	for _, rec := range want {
		if err := w.WriteRecord(sampleLabels(rec.name), rec.ts, rec.val); err != nil {
			t.Fatalf("WriteRecord: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var got []struct {
		name string
		ts   int64
		val  float64
	}
	if err := Replay(dir, func(labels []LabelPair, ts int64, val float64) {
		var name string
		for _, lp := range labels {
			if lp.Name == "__name__" {
				name = lp.Value
			}
		}
		got = append(got, struct {
			name string
			ts   int64
			val  float64
		}{name, ts, val})
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}

	if len(got) != len(want) {
		t.Fatalf("replayed %d records, want %d", len(got), len(want))
	}
	for i, g := range got {
		if g.name != want[i].name || g.ts != want[i].ts || g.val != want[i].val {
			t.Errorf("record[%d] = {%q, %d, %v}, want {%q, %d, %v}",
				i, g.name, g.ts, g.val, want[i].name, want[i].ts, want[i].val)
		}
	}
}

func TestReplay_PartialTrailingRecord(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, 128<<20, 1)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := w.WriteRecord(sampleLabels("good_metric"), 1000, 1.0); err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Corrupt the segment by appending truncated bytes after the valid record.
	paths, _ := segmentPaths(dir)
	f, err := os.OpenFile(paths[len(paths)-1], os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open segment for corruption: %v", err)
	}
	_, _ = f.Write([]byte{0x00, 0x00, 0x00, 0x20}) // announce 32-byte body...
	_, _ = f.Write([]byte{0x01, 0x02})             // ...but only write 2 bytes
	f.Close()

	var count int
	if err := Replay(dir, func(_ []LabelPair, _ int64, _ float64) { count++ }); err != nil {
		t.Fatalf("Replay returned error for partial trailing record: %v", err)
	}
	if count != 1 {
		t.Errorf("replayed %d records, want 1 (partial record must be discarded)", count)
	}
}

// TestReplay_TornTailSurvivesRepeatedRestart guards the recovery bug where a
// tolerated partial tail on the final segment was never truncated: once Open
// started a fresh segment, the torn segment was no longer final and the NEXT
// replay aborted startup. Replay must repair the tail so every restart is clean.
func TestReplay_TornTailSurvivesRepeatedRestart(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir, 128<<20, 1)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := w.WriteRecord(sampleLabels("good"), 1000, 1.0); err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Simulate a torn write: a length prefix promising more than is present.
	paths, _ := segmentPaths(dir)
	f, err := os.OpenFile(paths[len(paths)-1], os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open segment: %v", err)
	}
	_, _ = f.Write([]byte{0x00, 0x00, 0x00, 0x20, 0x01, 0x02})
	f.Close()

	// Restart #1: replay tolerates and repairs the torn tail.
	var c1 int
	if err := Replay(dir, func(_ []LabelPair, _ int64, _ float64) { c1++ }); err != nil {
		t.Fatalf("restart #1 Replay: %v", err)
	}
	if c1 != 1 {
		t.Fatalf("restart #1 replayed %d records, want 1", c1)
	}

	// A fresh Open starts a new segment; the torn segment is no longer final.
	w2, err := Open(dir, 128<<20, 1)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if err := w2.WriteRecord(sampleLabels("after"), 2000, 2.0); err != nil {
		t.Fatalf("WriteRecord after reopen: %v", err)
	}
	if err := w2.Close(); err != nil {
		t.Fatalf("Close 2: %v", err)
	}

	// Restart #2: replay must NOT abort on the now-non-final repaired segment.
	var c2 int
	if err := Replay(dir, func(_ []LabelPair, _ int64, _ float64) { c2++ }); err != nil {
		t.Fatalf("restart #2 Replay aborted on torn tail: %v", err)
	}
	if c2 != 2 {
		t.Errorf("restart #2 replayed %d records, want 2", c2)
	}
}

func TestReplay_FullyReadCorruptBodyReturnsError(t *testing.T) {
	// A record whose declared length matches the bytes present but whose body
	// fails to decode must always return an error — even on the final segment.
	// Only I/O truncation (io.ErrUnexpectedEOF) is tolerated; decode failures
	// of fully-read bytes are always corrupt.
	dir := t.TempDir()
	w, err := Open(dir, 128<<20, 1)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := w.WriteRecord(sampleLabels("good"), 1000, 1.0); err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Append a record whose length prefix exactly matches the body length, but
	// the body bytes are garbage (wrong type byte → decodeRecord returns ok=false).
	paths, _ := segmentPaths(dir)
	f, err := os.OpenFile(paths[len(paths)-1], os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open segment: %v", err)
	}
	garbage := make([]byte, 20)
	garbage[0] = 0x99 // unknown record type — decodeRecord will return ok=false
	var lenBuf [4]byte
	lenBuf[0] = 0
	lenBuf[1] = 0
	lenBuf[2] = 0
	lenBuf[3] = byte(len(garbage))
	_, _ = f.Write(lenBuf[:])
	_, _ = f.Write(garbage)
	f.Close()

	if err := Replay(dir, func(_ []LabelPair, _ int64, _ float64) {}); err == nil {
		t.Error("expected Replay to return error for fully-read corrupt body on final segment")
	}
}

func TestReplay_OversizedDeclaredLengthOnFinalSegment_Discarded(t *testing.T) {
	// A corrupted length prefix declaring a body larger than maxRecordBodyBytes
	// must be treated as a partial trailing record on the final segment — not
	// allocated and not panicked.
	dir := t.TempDir()
	w, err := Open(dir, 128<<20, 1)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := w.WriteRecord(sampleLabels("good"), 1000, 1.0); err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	paths, _ := segmentPaths(dir)
	f, err := os.OpenFile(paths[len(paths)-1], os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open segment: %v", err)
	}
	// Declare a 4 GB body but write nothing — this would OOM if allocated.
	_, _ = f.Write([]byte{0xff, 0xff, 0xff, 0xff})
	f.Close()

	var count int
	if err := Replay(dir, func(_ []LabelPair, _ int64, _ float64) { count++ }); err != nil {
		t.Fatalf("Replay returned error for oversized length on final segment: %v", err)
	}
	if count != 1 {
		t.Errorf("replayed %d records, want 1", count)
	}
}

func TestWriteRecord_TooManyLabels_Error(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, 128<<20, 1)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w.Close()

	labels := make([]LabelPair, 256)
	for i := range labels {
		labels[i] = LabelPair{Name: fmt.Sprintf("l%d", i), Value: "v"}
	}
	if err := w.WriteRecord(labels, 1000, 1.0); err == nil {
		t.Error("expected error for >255 labels, got nil")
	}
}

func TestWriteRecord_LabelNameTooLong_Error(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, 128<<20, 1)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w.Close()

	labels := []LabelPair{{Name: strings.Repeat("a", 256), Value: "v"}}
	if err := w.WriteRecord(labels, 1000, 1.0); err == nil {
		t.Error("expected error for label name >255 bytes, got nil")
	}
}

func TestWriteRecord_LabelValueTooLong_Error(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, 128<<20, 1)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w.Close()

	labels := []LabelPair{{Name: "lbl", Value: strings.Repeat("v", 65536)}}
	if err := w.WriteRecord(labels, 1000, 1.0); err == nil {
		t.Error("expected error for label value >65535 bytes, got nil")
	}
}

func TestReplay_EmptyDir(t *testing.T) {
	dir := t.TempDir()
	var count int
	if err := Replay(dir, func(_ []LabelPair, _ int64, _ float64) { count++ }); err != nil {
		t.Fatalf("Replay on empty dir: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 records from empty dir, got %d", count)
	}
}
