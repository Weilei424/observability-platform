package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDirSize_SumsSegments(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "000001.wal"), make([]byte, 100), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "000002.wal"), make([]byte, 50), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "ignore.txt"), make([]byte, 999), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := DirSize(dir)
	if err != nil {
		t.Fatalf("DirSize: %v", err)
	}
	if got != 150 {
		t.Fatalf("DirSize = %d, want 150", got)
	}
}

func TestDirSize_MissingDir_ReturnsZero(t *testing.T) {
	got, err := DirSize(filepath.Join(t.TempDir(), "nope"))
	if err != nil || got != 0 {
		t.Fatalf("DirSize(missing) = %d, %v; want 0, nil", got, err)
	}
}

func TestDirStatsCountsOnlyWALSegments(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "00000001.wal"), []byte("abcd"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "00000002.wal"), []byte("efghij"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Neither of these is a segment: a checkpoint marker and a subdirectory.
	if err := os.WriteFile(filepath.Join(dir, "checkpoint"), []byte("7"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "nested.wal"), 0o755); err != nil {
		t.Fatal(err)
	}

	bytes, segments, err := DirStats(dir)
	if err != nil {
		t.Fatalf("DirStats: %v", err)
	}
	if bytes != 10 {
		t.Errorf("bytes = %d, want 10 (4+6, excluding the checkpoint file)", bytes)
	}
	if segments != 2 {
		t.Errorf("segments = %d, want 2 (the .wal directory is not a segment)", segments)
	}
}

func TestDirStatsMissingDirIsNotAnError(t *testing.T) {
	// A backend that has not written its first record yet has no WAL directory.
	// Reporting an error there would make the collector emit a gap on a healthy
	// server, so a missing directory must read as an empty one.
	bytes, segments, err := DirStats(filepath.Join(t.TempDir(), "does-not-exist"))
	if err != nil {
		t.Fatalf("DirStats on missing dir: %v", err)
	}
	if bytes != 0 || segments != 0 {
		t.Errorf("got (%d, %d), want (0, 0)", bytes, segments)
	}
}

func TestDirSizeAgreesWithDirStats(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "00000001.wal"), []byte("12345"), 0o644); err != nil {
		t.Fatal(err)
	}
	size, err := DirSize(dir)
	if err != nil {
		t.Fatalf("DirSize: %v", err)
	}
	bytes, _, err := DirStats(dir)
	if err != nil {
		t.Fatalf("DirStats: %v", err)
	}
	if size != bytes {
		t.Errorf("DirSize = %d but DirStats bytes = %d; they must not drift", size, bytes)
	}
}
