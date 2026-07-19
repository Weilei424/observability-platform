package logwal

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

// LogWAL is an append-only write-ahead log for log records. Safe for concurrent use.
type LogWAL struct {
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
	// explicit Sync/Close). A test seam that lets a same-package test verify the
	// automatic boundary actually fires, which an in-process replay cannot show.
	autoSyncs int
}

// Open opens or creates a log WAL rooted at dir. New writes go to a fresh segment
// numbered one past the highest existing segment index, so previously written
// segments are never appended to.
func Open(dir string, segMaxBytes int64, syncEveryN int) (*LogWAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("logwal: mkdir %s: %w", dir, err)
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
	w := &LogWAL{
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

func (w *LogWAL) segmentPath(idx int) string {
	return filepath.Join(w.dir, fmt.Sprintf("%06d.wal", idx))
}

func (w *LogWAL) openSegment(idx int) error {
	f, err := os.OpenFile(w.segmentPath(idx), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("logwal: open segment %06d: %w", idx, err)
	}
	// Fsync the directory so the new segment's directory entry is durable before
	// any record is acknowledged. A file fsync alone does not persist a newly
	// created entry on filesystems that require a directory fsync.
	if err := syncDir(w.dir); err != nil {
		f.Close()
		return fmt.Errorf("logwal: fsync dir for segment %06d: %w", idx, err)
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

// WriteRecord encodes and appends a log record to the active segment. Rotates to a
// new segment when written bytes reach segMaxBytes. Fsyncs every syncEveryN records
// (syncEveryN=1 means sync after every write).
func (w *LogWAL) WriteRecord(labels []LabelPair, tsNs int64, line string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.broken {
		return fmt.Errorf("logwal: writer is in broken state due to previous rotation failure")
	}
	if err := validateLabels(labels); err != nil {
		return err
	}

	data := encodeRecord(labels, tsNs, line)
	if _, err := w.current.Write(data); err != nil {
		return fmt.Errorf("logwal: write record: %w", err)
	}
	w.written += int64(len(data))
	w.sinceSynced++

	if w.syncEveryN > 0 && w.sinceSynced >= w.syncEveryN {
		if err := w.current.Sync(); err != nil {
			return fmt.Errorf("logwal: fsync: %w", err)
		}
		w.sinceSynced = 0
		w.autoSyncs++
	}

	if w.written >= w.segMaxBytes {
		// Fsync any records written since the last periodic sync before sealing the
		// segment, so a tail of fewer than syncEveryN records is not left durable
		// only in the OS page cache when syncEveryN > 1.
		if err := w.current.Sync(); err != nil {
			return fmt.Errorf("logwal: fsync before rotate: %w", err)
		}
		if err := w.current.Close(); err != nil {
			return fmt.Errorf("logwal: close segment on rotate: %w", err)
		}
		w.broken = true
		if err := w.openSegment(w.segIndex + 1); err != nil {
			return err
		}
		w.broken = false
	}
	return nil
}

// Sync explicitly fsyncs the current segment.
func (w *LogWAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.broken {
		return fmt.Errorf("logwal: writer is in broken state due to previous rotation failure")
	}
	return w.current.Sync()
}

// SegmentIndex returns the index of the currently active WAL segment.
func (w *LogWAL) SegmentIndex() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.segIndex
}

// Close syncs and closes the current segment.
func (w *LogWAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.current == nil {
		return nil
	}
	syncErr := w.current.Sync()
	closeErr := w.current.Close()
	w.current = nil
	return errors.Join(syncErr, closeErr)
}

// segmentPaths returns all .wal file paths in dir sorted ascending.
// Returns nil (not an error) when dir does not exist.
func segmentPaths(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("logwal: readdir %s: %w", dir, err)
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
