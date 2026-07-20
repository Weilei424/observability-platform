package fsutil

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
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

// TestMkdirAllSync_PartialFailureKeepsDurablePrefixThenRetrySucceeds is the
// regression guard for the retry-safety gap: when a deep level's parent fsync
// fails, the shallower levels created so far must remain durable, the failing
// level must be rolled back, and a retry must recreate and re-sync it.
func TestMkdirAllSync_PartialFailureKeepsDurablePrefixThenRetrySucceeds(t *testing.T) {
	base := t.TempDir()
	x := filepath.Join(base, "x")
	y := filepath.Join(base, "x", "y")

	restore := SyncDir
	defer func() { SyncDir = restore }()

	// First attempt: syncing x (the parent of y) fails; syncing base succeeds.
	SyncDir = func(dir string) error {
		if dir == x {
			return errors.New("sync x boom")
		}
		return restore(dir)
	}
	if err := MkdirAllSync(y); err == nil {
		t.Fatal("expected error when syncing the deep parent fails")
	}
	if _, err := os.Stat(x); err != nil {
		t.Fatalf("shallow level %s must remain durable, stat err=%v", x, err)
	}
	if _, err := os.Stat(y); !os.IsNotExist(err) {
		t.Fatalf("failing level %s must be rolled back, err=%v", y, err)
	}

	// Retry with a working fsync: y is created and x re-synced before success.
	var synced []string
	SyncDir = func(dir string) error { synced = append(synced, dir); return restore(dir) }
	if err := MkdirAllSync(y); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if _, err := os.Stat(y); err != nil {
		t.Errorf("y not created on retry: %v", err)
	}
	found := false
	for _, d := range synced {
		if d == x {
			found = true
		}
	}
	if !found {
		t.Errorf("retry did not re-sync %q; synced=%v", x, synced)
	}
}

func TestMkdirAllSync_RollbackFailureIsSurfaced(t *testing.T) {
	base := t.TempDir()
	x := filepath.Join(base, "x")
	y := filepath.Join(base, "x", "y")

	restoreSync := SyncDir
	restoreRemove := removeDir
	defer func() { SyncDir = restoreSync; removeDir = restoreRemove }()

	SyncDir = func(dir string) error {
		if dir == x {
			return errors.New("sync boom")
		}
		return restoreSync(dir)
	}
	removeDir = func(string) error { return errors.New("remove boom") }

	err := MkdirAllSync(y)
	if err == nil {
		t.Fatal("expected error when fsync fails and rollback also fails")
	}
	if !strings.Contains(err.Error(), "rollback") {
		t.Errorf("error %q should surface the rollback failure", err.Error())
	}
}
