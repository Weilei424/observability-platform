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
