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

// MkdirAllSync creates dir and any missing parents, then fsyncs the parent of
// every newly created directory so the new entries survive a power loss. It
// fsyncs up to and including the parent of the shallowest created directory, so
// the created chain is durable relative to the first pre-existing ancestor.
//
// Retry safety: if a parent fsync fails, the directories created by this call
// are removed before returning the error, so a later retry recreates and
// re-syncs them rather than seeing them as already existing and skipping the
// sync. The created directories are empty at this point (no files are written
// until after MkdirAllSync returns), so removing them is safe.
func MkdirAllSync(dir string) error {
	// Walk up collecting the chain of not-yet-existing directories, deepest first,
	// stopping at the first existing ancestor.
	var created []string
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
		created = append(created, p)
		parent := filepath.Dir(p)
		if parent == p {
			break // reached the filesystem root
		}
		p = parent
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	seen := map[string]bool{}
	for _, c := range created {
		parent := filepath.Dir(c)
		if seen[parent] {
			continue
		}
		seen[parent] = true
		if err := SyncDir(parent); err != nil {
			// Roll back this call's directories (deepest first — children before
			// parents) so a retry starts clean and re-syncs.
			for _, d := range created {
				_ = os.Remove(d)
			}
			return fmt.Errorf("fsutil: fsync parent of %s: %w", c, err)
		}
	}
	return nil
}
