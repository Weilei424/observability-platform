// Package fsutil provides crash-durable filesystem helpers: creating and
// fsyncing directories so newly created entries survive a power loss. A file
// fsync alone does not persist a newly created directory entry — the entry lives
// in the parent directory, which must itself be fsynced.
package fsutil

import (
	"fmt"
	"os"
	"path/filepath"
)

// SyncDir fsyncs a directory so newly created or removed entries within it are
// durable. It is a package var so tests can count calls and inject failures.
var SyncDir = func(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		d.Close()
		return err
	}
	return d.Close()
}

// removeDir removes a directory. It is a package var so tests can simulate a
// rollback (cleanup) failure.
var removeDir = os.Remove

// MkdirAllSync creates dir and any missing parents so that every created
// directory's entry is crash-durable. Each missing level is created and its
// parent fsynced individually, shallowest first, so the entry is persisted
// before descending.
//
// This makes the operation fully retry-safe: any directory that exists is
// guaranteed durable — either it pre-existed this process (assumed durable) or
// it was created here and its parent was fsynced immediately. If a level's
// parent fsync fails, that level (which could not be made durable) is removed so
// a retry recreates and re-syncs it; every shallower level already created
// remains durable. If the rollback removal itself fails, the error is surfaced
// (wrapping both the fsync and removal failures) rather than silently leaving an
// undurable directory. The created directories are empty at this point (no files
// are written until after MkdirAllSync returns), so removing them is safe.
func MkdirAllSync(dir string) error {
	// Walk up collecting the chain of not-yet-existing directories, deepest first,
	// stopping at the first existing ancestor.
	var missing []string
	p := filepath.Clean(dir)
	for {
		fi, err := os.Stat(p)
		if err == nil {
			if !fi.IsDir() {
				return fmt.Errorf("fsutil: %s exists and is not a directory", p)
			}
			break
		}
		if !os.IsNotExist(err) {
			return err
		}
		missing = append(missing, p)
		parent := filepath.Dir(p)
		if parent == p {
			break // reached the filesystem root
		}
		p = parent
	}

	// Create shallowest first so each level's parent already exists.
	for i := len(missing) - 1; i >= 0; i-- {
		level := missing[i]
		if err := os.Mkdir(level, 0o755); err != nil && !os.IsExist(err) {
			return err
		}
		if err := SyncDir(filepath.Dir(level)); err != nil {
			// This level's entry is not durable; remove it so a retry recreates
			// and re-syncs it. Shallower levels are already durable.
			if rmErr := removeDir(level); rmErr != nil {
				return fmt.Errorf("fsutil: fsync parent of %s failed (%v); rollback also failed: %w", level, err, rmErr)
			}
			return fmt.Errorf("fsutil: fsync parent of %s: %w", level, err)
		}
	}
	return nil
}
