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

// TestDirStatsErrorsOnMissingDir pins the collector-facing contract: main.go
// creates the WAL directory at startup before any collector that calls
// DirStats is registered, so a missing directory at scrape time means
// something deleted it out from under the store -- an operational failure,
// not a healthy empty WAL. Returning (0, 0, nil) there would show a
// confident zero on the dashboard instead of a gap, and
// obs_collector_errors_total would never move.
func TestDirStatsErrorsOnMissingDir(t *testing.T) {
	_, _, err := DirStats(filepath.Join(t.TempDir(), "does-not-exist"))
	if err == nil {
		t.Fatal("DirStats on missing dir: got nil error, want an error so the collector emits a gap instead of a confident zero")
	}
}

// TestDirSize_MissingDir_StillReturnsZero pins that DirSize keeps its own,
// deliberately more tolerant contract even though DirStats (which it is
// implemented over) now errors on ENOENT: DirSize's only caller
// (metrics.WALStore.WALBytes, a compactor flush-size trigger) has no
// startup-ordering guarantee and already treats any error as "skip the
// size-based flush," so there is nothing to gain by hardening it too.
func TestDirSize_MissingDir_StillReturnsZero(t *testing.T) {
	got, err := DirSize(filepath.Join(t.TempDir(), "still-missing"))
	if err != nil || got != 0 {
		t.Fatalf("DirSize(missing) = %d, %v; want 0, nil", got, err)
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
