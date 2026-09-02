package wal

import (
	"fmt"
	"os"
	"strings"
)

// DirStats returns the total size in bytes and the number of *.wal segment files
// in dir. A missing directory is not an error; it returns (0, 0, nil), because a
// backend that has not written a record yet has no WAL directory and is healthy.
func DirStats(dir string) (bytes int64, segments int, err error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, 0, nil
		}
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
// Implemented over DirStats so the two cannot disagree about what counts as a
// segment.
func DirSize(dir string) (int64, error) {
	bytes, _, err := DirStats(dir)
	return bytes, err
}
