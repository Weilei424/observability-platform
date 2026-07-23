package logs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/storage/logchunk"
)

func TestReadChunkFileHeader_MatchesWithoutDecompress(t *testing.T) {
	dir := t.TempDir()
	labels, _ := NewStreamLabels(map[string]string{"service": "api", "level": "error"})
	id := StreamIDOf(labels)
	c := logchunk.NewChunk()
	c.Append(300, "late")
	c.Append(100, "early")
	ref, err := writeChunkFile(dir, id, labels, c)
	if err != nil {
		t.Fatalf("writeChunkFile: %v", err)
	}
	gotID, gotLabels, minTs, maxTs, err := readChunkFileHeader(filepath.Join(dir, ref.Name))
	if err != nil {
		t.Fatalf("readChunkFileHeader: %v", err)
	}
	if gotID != id {
		t.Errorf("streamID = %d, want %d", gotID, id)
	}
	if gotLabels.Hash() != labels.Hash() {
		t.Errorf("labels hash mismatch")
	}
	if minTs != 100 || maxTs != 300 {
		t.Errorf("bounds = %d/%d, want 100/300", minTs, maxTs)
	}
}

func TestReadChunkFileHeader_RejectsTamperedMetadata(t *testing.T) {
	dir := t.TempDir()
	// Single label so the header byte offsets are deterministic:
	// magic(4)|ver(1)|streamID(8)|labelCount(1)|nameLen(1)|"service"(7)|valLen(2)|
	// "api"(3) => the file header is 27 bytes, then the logchunk header follows.
	labels, _ := NewStreamLabels(map[string]string{"service": "api"})
	c := logchunk.NewChunk()
	c.Append(100, "x")
	c.Append(200, "y")
	ref, err := writeChunkFile(dir, StreamIDOf(labels), labels, c)
	if err != nil {
		t.Fatalf("writeChunkFile: %v", err)
	}
	path := filepath.Join(dir, ref.Name)
	good, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	const lcStart = 27 // logchunk header begins here; minTs at +5, maxTs at +13

	cases := map[string]int{
		"tampered label byte": 15,           // inside "service" -> id/labels mismatch or invalid label
		"tampered minTs byte": lcStart + 5,  // logchunk header CRC catches it
		"tampered maxTs byte": lcStart + 13, // logchunk header CRC catches it
	}
	for name, off := range cases {
		t.Run(name, func(t *testing.T) {
			bad := append([]byte(nil), good...)
			bad[off] ^= 0xff
			if err := os.WriteFile(path, bad, 0o644); err != nil {
				t.Fatal(err)
			}
			if _, _, _, _, err := readChunkFileHeader(path); err == nil {
				t.Errorf("expected error for %s (unauthenticated metadata must be rejected)", name)
			}
		})
	}
}

func TestReadChunkFileHeader_RejectsTruncatedPayload(t *testing.T) {
	dir := t.TempDir()
	labels, _ := NewStreamLabels(map[string]string{"service": "api"})
	c := logchunk.NewChunk()
	c.Append(100, "x")
	c.Append(200, "y")
	ref, err := writeChunkFile(dir, StreamIDOf(labels), labels, c)
	if err != nil {
		t.Fatalf("writeChunkFile: %v", err)
	}
	path := filepath.Join(dir, ref.Name)
	// Truncate to exactly the file header (27 bytes for a single label) + logchunk
	// header, cutting off the compressed payload while leaving both headers intact.
	if err := os.Truncate(path, 27+int64(logchunk.HeaderLen)); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if _, _, _, _, err := readChunkFileHeader(path); err == nil {
		t.Error("expected error for a chunk truncated after its header (payload missing)")
	}
}

func TestReadChunkFile_RejectsRelabeledChunk(t *testing.T) {
	dir := t.TempDir()
	labels, _ := NewStreamLabels(map[string]string{"service": "api"})
	c := logchunk.NewChunk()
	c.Append(100, "x")
	ref, err := writeChunkFile(dir, StreamIDOf(labels), labels, c)
	if err != nil {
		t.Fatalf("writeChunkFile: %v", err)
	}
	path := filepath.Join(dir, ref.Name)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// The value "api" starts at offset 24 (14 + nameLen(1) + "service"(7) + valLen(2)).
	// Change it to another VALID label value; the embedded ID no longer fingerprints
	// the stored labels, which the full-read path must reject (not just recovery).
	data[24] = 'z'
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := readChunkFile(path); err == nil {
		t.Error("expected error: relabeled chunk's id no longer matches its labels")
	}
}

func TestChunkFile_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	labels, _ := NewStreamLabels(map[string]string{"service": "api", "level": "error"})
	id := StreamIDOf(labels)
	c := logchunk.NewChunk()
	c.Append(100, "boom")
	c.Append(200, "kaboom")

	ref, err := writeChunkFile(dir, id, labels, c)
	if err != nil {
		t.Fatalf("writeChunkFile: %v", err)
	}
	if ref.MinTs != 100 || ref.MaxTs != 200 {
		t.Fatalf("ref bounds = %d/%d, want 100/200", ref.MinTs, ref.MaxTs)
	}

	gotID, gotLabels, gotChunk, err := readChunkFile(filepath.Join(dir, ref.Name))
	if err != nil {
		t.Fatalf("readChunkFile: %v", err)
	}
	if gotID != id {
		t.Fatalf("streamID = %d, want %d", gotID, id)
	}
	if gotLabels.Hash() != labels.Hash() {
		t.Fatalf("labels hash mismatch")
	}
	if gotChunk.NumEntries() != 2 {
		t.Fatalf("entries = %d, want 2", gotChunk.NumEntries())
	}
}

func TestChunkFile_NoTempLeftBehind(t *testing.T) {
	dir := t.TempDir()
	labels, _ := NewStreamLabels(map[string]string{"service": "api"})
	c := logchunk.NewChunk()
	c.Append(1, "x")
	if _, err := writeChunkFile(dir, StreamIDOf(labels), labels, c); err != nil {
		t.Fatalf("writeChunkFile: %v", err)
	}
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".tmp" {
			t.Fatalf("temp file left behind: %s", e.Name())
		}
	}
}

func TestDecodeChunkFileHeader_RejectsCorrupt(t *testing.T) {
	dir := t.TempDir()
	labels, _ := NewStreamLabels(map[string]string{"service": "api", "level": "error"})
	c := logchunk.NewChunk()
	c.Append(1, "x")
	ref, err := writeChunkFile(dir, StreamIDOf(labels), labels, c)
	if err != nil {
		t.Fatalf("writeChunkFile: %v", err)
	}
	good, err := os.ReadFile(filepath.Join(dir, ref.Name))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	badMagic := append([]byte(nil), good...)
	badMagic[0] ^= 0xff
	badVersion := append([]byte(nil), good...)
	badVersion[4] = 0x7f
	cases := map[string][]byte{
		"empty":               {},
		"too short":           good[:10],
		"bad magic":           badMagic,
		"bad version":         badVersion,
		"truncated in labels": good[:16],
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			if _, _, _, err := decodeChunkFileHeader(data); err == nil {
				t.Errorf("expected error for %s", name)
			}
		})
	}
}
