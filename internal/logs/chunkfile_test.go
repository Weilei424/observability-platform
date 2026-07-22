package logs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/storage/logchunk"
)

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
