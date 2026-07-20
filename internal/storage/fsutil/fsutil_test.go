package fsutil

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestMkdirAllSync_FsyncsNewParents(t *testing.T) {
	var synced []string
	restore := SyncDir
	SyncDir = func(dir string) error { synced = append(synced, dir); return restore(dir) }
	defer func() { SyncDir = restore }()

	base := t.TempDir() // exists
	dir := filepath.Join(base, "a", "b", "c")
	if err := MkdirAllSync(dir); err != nil {
		t.Fatalf("MkdirAllSync: %v", err)
	}
	// Every newly created directory's parent must be fsynced so the new entries
	// are durable: base (holds a), base/a (holds b), base/a/b (holds c).
	for _, want := range []string{base, filepath.Join(base, "a"), filepath.Join(base, "a", "b")} {
		found := false
		for _, d := range synced {
			if d == want {
				found = true
			}
		}
		if !found {
			t.Errorf("expected SyncDir(%q), got %v", want, synced)
		}
	}
	if _, err := os.Stat(dir); err != nil {
		t.Errorf("dir not created: %v", err)
	}
}

func TestMkdirAllSync_ExistingDirNoSync(t *testing.T) {
	var calls int
	restore := SyncDir
	SyncDir = func(dir string) error { calls++; return restore(dir) }
	defer func() { SyncDir = restore }()

	dir := t.TempDir() // already exists
	if err := MkdirAllSync(dir); err != nil {
		t.Fatalf("MkdirAllSync: %v", err)
	}
	if calls != 0 {
		t.Errorf("SyncDir called %d times for an existing dir, want 0", calls)
	}
}

func TestMkdirAllSync_SyncFailureRollsBackAndRetrySucceeds(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "x", "y")

	// First attempt: every parent fsync fails. MkdirAllSync must error AND roll
	// back the directories it created, so the state is clean for a retry.
	restore := SyncDir
	SyncDir = func(string) error { return errors.New("sync boom") }
	if err := MkdirAllSync(dir); err == nil {
		SyncDir = restore
		t.Fatal("MkdirAllSync should fail when a parent fsync fails")
	}
	if _, err := os.Stat(filepath.Join(base, "x")); !os.IsNotExist(err) {
		SyncDir = restore
		t.Fatalf("created dirs must be rolled back after a failed sync; base/x still present (err=%v)", err)
	}

	// Retry with a working fsync: because the dirs were rolled back, the parents
	// are seen as missing again and are re-synced before success.
	var synced []string
	SyncDir = func(d string) error { synced = append(synced, d); return restore(d) }
	defer func() { SyncDir = restore }()
	if err := MkdirAllSync(dir); err != nil {
		t.Fatalf("retry MkdirAllSync: %v", err)
	}
	for _, want := range []string{base, filepath.Join(base, "x")} {
		found := false
		for _, d := range synced {
			if d == want {
				found = true
			}
		}
		if !found {
			t.Errorf("retry did not re-sync parent %q; synced=%v", want, synced)
		}
	}
	if _, err := os.Stat(dir); err != nil {
		t.Errorf("dir not created on retry: %v", err)
	}
}
