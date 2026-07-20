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

func TestMkdirAllSync_ExistingDirSyncsBoundaryParent(t *testing.T) {
	var synced []string
	restore := SyncDir
	SyncDir = func(dir string) error { synced = append(synced, dir); return restore(dir) }
	defer func() { SyncDir = restore }()

	dir := t.TempDir() // already exists
	if err := MkdirAllSync(dir); err != nil {
		t.Fatalf("MkdirAllSync: %v", err)
	}
	// The existing boundary's own entry is made durable by fsyncing its parent,
	// so a directory left behind by a prior interrupted run is not trusted blindly.
	want := filepath.Dir(dir)
	if len(synced) != 1 || synced[0] != want {
		t.Errorf("synced = %v, want exactly [%q] (boundary parent)", synced, want)
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

func TestMkdirAllSync_UnreadableBoundaryParentTolerated(t *testing.T) {
	// A pre-provisioned data directory whose parent is execute-only (0111): the
	// service can traverse the parent and fully access the data dir, but cannot
	// read/list the parent. MkdirAllSync must not fail — the readiness contract
	// requires only that the data dir be writable, not that its parent be readable.
	base := t.TempDir()
	parent := filepath.Join(base, "restricted")
	data := filepath.Join(parent, "data")
	if err := os.MkdirAll(data, 0o700); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := os.Chmod(parent, 0o111); err != nil {
		t.Fatalf("chmod parent 0111: %v", err)
	}
	defer os.Chmod(parent, 0o700) // restore so t.TempDir cleanup can remove it

	// Creating a WAL directory under the writable data dir must succeed despite the
	// unreadable parent.
	if err := MkdirAllSync(filepath.Join(data, "metrics", "wal")); err != nil {
		t.Errorf("MkdirAllSync under an unreadable parent should succeed, got: %v", err)
	}
}

func TestMkdirAllSync_RollbackFailureSurfacedThenRetryMakesSurvivorDurable(t *testing.T) {
	base := t.TempDir()
	x := filepath.Join(base, "x")
	y := filepath.Join(base, "x", "y")

	restoreSync := SyncDir
	restoreRemove := removeDir
	defer func() { SyncDir = restoreSync; removeDir = restoreRemove }()

	errSync := errors.New("sync boom")
	errRemove := errors.New("remove boom")

	// Attempt 1: syncing x (the parent of y) fails, and rollback removal of y also
	// fails, so the undurable directory y is left behind.
	SyncDir = func(dir string) error {
		if dir == x {
			return errSync
		}
		return restoreSync(dir)
	}
	removeDir = func(string) error { return errRemove }

	err := MkdirAllSync(y)
	if err == nil {
		t.Fatal("expected error when fsync and rollback both fail")
	}
	// The combined error must let errors.Is recover BOTH underlying causes.
	if !errors.Is(err, errSync) || !errors.Is(err, errRemove) {
		t.Errorf("error must wrap both causes: errors.Is(sync)=%v errors.Is(remove)=%v (err=%v)",
			errors.Is(err, errSync), errors.Is(err, errRemove), err)
	}
	if _, statErr := os.Stat(y); statErr != nil {
		t.Fatalf("survivor y should still exist after a failed rollback: %v", statErr)
	}

	// Retry with working seams: because y is now the existing boundary, its parent
	// x must be fsynced before success — making the previously undurable y durable.
	var synced []string
	SyncDir = func(dir string) error { synced = append(synced, dir); return restoreSync(dir) }
	removeDir = restoreRemove
	if err := MkdirAllSync(y); err != nil {
		t.Fatalf("retry: %v", err)
	}
	found := false
	for _, d := range synced {
		if d == x {
			found = true
		}
	}
	if !found {
		t.Errorf("retry did not fsync the survivor's parent %q; synced=%v", x, synced)
	}
}
