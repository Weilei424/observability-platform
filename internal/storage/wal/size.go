package wal

import (
	"fmt"
	"os"
	"strings"
)

// DirSize returns the total size in bytes of all *.wal segment files in dir.
// A missing directory is not an error; it returns 0.
func DirSize(dir string) (int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("wal: readdir %s: %w", dir, err)
	}
	var total int64
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".wal") {
			continue
		}
		fi, err := e.Info()
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return 0, fmt.Errorf("wal: info %s: %w", e.Name(), err)
		}
		total += fi.Size()
	}
	return total, nil
}
