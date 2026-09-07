package wal

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"
)

// DirStats returns the total size in bytes and the number of *.wal segment files
// in dir.
//
// The callers that matter for this function (cmd/server/main.go) create dir at
// startup before the collector that calls DirStats is ever registered, so by
// the time DirStats can run, dir must already exist. A missing directory here
// does not mean "no record written yet" -- it means something deleted it out
// from under the store, which is an operational failure. Reporting that as a
// healthy (0, 0, nil) would show a confident zero on the dashboard instead of
// a gap, and obs_collector_errors_total would never move. The collector-error
// policy (ARCHITECTURE_NOTES.md) requires a gap plus a counted error for any
// failed read, ENOENT included, so every ReadDir failure -- ENOENT included --
// is returned as an error rather than special-cased away.
//
// DirSize (below) deliberately disagrees: it has a caller that predates
// startup directory creation and must keep tolerating a missing directory as
// 0. Do not fold that tolerance back into DirStats.
func DirStats(dir string) (bytes int64, segments int, err error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, 0, fmt.Errorf("wal: readdir %s: %w", dir, err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".wal") {
			continue
		}
		fi, err := e.Info()
		if err != nil {
			// The segment was rotated away between ReadDir and Info. That is
			// normal concurrent operation, not a failure.
			if os.IsNotExist(err) {
				continue
			}
			return 0, 0, fmt.Errorf("wal: info %s: %w", e.Name(), err)
		}
		bytes += fi.Size()
		segments++
	}
	return bytes, segments, nil
}

// DirSize returns the total size in bytes of all *.wal segment files in dir.
// A missing directory is not an error; it returns 0.
//
// This tolerates ENOENT deliberately, unlike DirStats: its caller
// (metrics.WALStore.WALBytes, used by the compactor as a size-based flush
// trigger) already treats any error as "don't flush on size yet" and has no
// startup-ordering guarantee that dir exists before it is first called, so
// turning a missing directory into an error here would gain nothing while
// risking a spurious compactor log line. DirStats' ENOENT-is-an-error policy
// does not transfer here.
//
// Implemented over DirStats so the two cannot disagree about what counts as a
// segment; only the missing-directory handling differs, and it is handled
// here rather than in DirStats.
func DirSize(dir string) (int64, error) {
	bytes, _, err := DirStats(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	return bytes, nil
}
