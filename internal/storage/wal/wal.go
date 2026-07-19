package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// WAL is an append-only write-ahead log for metric samples.
// Safe for concurrent use.
type WAL struct {
	mu          sync.Mutex
	dir         string
	segMaxBytes int64
	syncEveryN  int
	current     *os.File
	segIndex    int
	written     int64
	sinceSynced int
	broken      bool

	// autoSyncs counts fsyncs triggered by the syncEveryN write boundary (not
	// explicit Sync/Close). A same-package test seam to verify the automatic
	// boundary actually fires, which an in-process replay cannot show.
	autoSyncs int
}

// Open opens or creates a WAL rooted at dir. New writes go to a fresh segment
// numbered one past the highest existing segment index, so previously written
// segments are never appended to.
func Open(dir string, segMaxBytes int64, syncEveryN int) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: mkdir %s: %w", dir, err)
	}

	paths, err := segmentPaths(dir)
	if err != nil {
		return nil, err
	}
	maxIdx := 0
	for _, p := range paths {
		base := strings.TrimSuffix(filepath.Base(p), ".wal")
		if idx, e := strconv.Atoi(base); e == nil && idx > maxIdx {
			maxIdx = idx
		}
	}

	w := &WAL{
		dir:         dir,
		segMaxBytes: segMaxBytes,
		syncEveryN:  syncEveryN,
		segIndex:    maxIdx + 1,
	}
	if err := w.openSegment(w.segIndex); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *WAL) segmentPath(idx int) string {
	return filepath.Join(w.dir, fmt.Sprintf("%06d.wal", idx))
}

func (w *WAL) openSegment(idx int) error {
	f, err := os.OpenFile(w.segmentPath(idx), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("wal: open segment %06d: %w", idx, err)
	}
	// Fsync the directory so the new segment's directory entry is durable before
	// any record is acknowledged. A file fsync alone does not persist a newly
	// created entry on filesystems that require a directory fsync.
	if err := syncDir(w.dir); err != nil {
		f.Close()
		return fmt.Errorf("wal: fsync dir for segment %06d: %w", idx, err)
	}
	w.current = f
	w.segIndex = idx
	w.written = 0
	w.sinceSynced = 0
	return nil
}

// syncDir fsyncs a directory so newly created/removed entries within it are durable.
func syncDir(dir string) error {
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

// WriteRecord encodes and appends a sample record to the active segment.
// Rotates to a new segment when written bytes reach segMaxBytes.
// Fsyncs every syncEveryN records (syncEveryN=1 means sync after every write).
func (w *WAL) WriteRecord(labels []LabelPair, tsMs int64, value float64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.broken {
		return fmt.Errorf("wal: writer is in broken state due to previous rotation failure")
	}

	if err := validateLabels(labels); err != nil {
		return err
	}

	data := encodeRecord(labels, tsMs, value)
	if _, err := w.current.Write(data); err != nil {
		return fmt.Errorf("wal: write record: %w", err)
	}
	w.written += int64(len(data))
	w.sinceSynced++

	if w.syncEveryN > 0 && w.sinceSynced >= w.syncEveryN {
		if err := w.current.Sync(); err != nil {
			return fmt.Errorf("wal: fsync: %w", err)
		}
		w.sinceSynced = 0
		w.autoSyncs++
	}

	if w.written >= w.segMaxBytes {
		// Fsync any records written since the last periodic sync before sealing the
		// segment, so a tail of fewer than syncEveryN records is not left durable
		// only in the OS page cache when syncEveryN > 1.
		if err := w.current.Sync(); err != nil {
			return fmt.Errorf("wal: fsync before rotate: %w", err)
		}
		if err := w.current.Close(); err != nil {
			return fmt.Errorf("wal: close segment on rotate: %w", err)
		}
		w.broken = true // mark broken until new segment is open
		if err := w.openSegment(w.segIndex + 1); err != nil {
			return err
		}
		w.broken = false
	}
	return nil
}

// Sync explicitly fsyncs the current segment.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.broken {
		return fmt.Errorf("wal: writer is in broken state due to previous rotation failure")
	}
	return w.current.Sync()
}

// SegmentIndex returns the index of the currently active WAL segment.
func (w *WAL) SegmentIndex() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.segIndex
}

// Close syncs and closes the current segment.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.current == nil {
		return nil
	}
	syncErr := w.current.Sync()
	closeErr := w.current.Close()
	return errors.Join(syncErr, closeErr)
}

// segmentPaths returns all .wal file paths in dir sorted in ascending order.
// Returns nil (not an error) when dir does not exist.
func segmentPaths(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("wal: readdir %s: %w", dir, err)
	}
	var paths []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".wal") {
			paths = append(paths, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(paths)
	return paths, nil
}
