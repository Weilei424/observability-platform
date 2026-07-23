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

	"github.com/masonwheeler/observability-platform/internal/storage/fsutil"
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
	// preRotationSyncs counts fsyncs of an outgoing segment performed just before
	// rotation. A test seam for the rotate-with-unsynced-remainder guarantee.
	preRotationSyncs int
}

// syncFile fsyncs a file. It is a package var so a test can inject a sync failure
// on the checkpoint path and assert the error propagates before any close/delete.
var syncFile = func(f *os.File) error { return f.Sync() }

// Open opens or creates a log WAL rooted at dir. New writes go to a fresh segment
// numbered one past the highest existing segment index, so previously written
// segments are never appended to.
func Open(dir string, segMaxBytes int64, syncEveryN int) (*LogWAL, error) {
	if err := fsutil.MkdirAllSync(dir); err != nil {
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
	if err := fsutil.SyncDir(w.dir); err != nil {
		f.Close()
		return fmt.Errorf("logwal: fsync dir for segment %06d: %w", idx, err)
	}
	w.current = f
	w.segIndex = idx
	w.written = 0
	w.sinceSynced = 0
	return nil
}

// WriteRecord encodes and appends a log record to the active segment. Rotates to a
// new segment when written bytes reach segMaxBytes. Fsyncs every syncEveryN records
// (syncEveryN=1 means sync after every write).
func (w *LogWAL) WriteRecord(labels []LabelPair, tsNs int64, line string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.current == nil {
		return fmt.Errorf("logwal: write on closed WAL")
	}
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
		w.preRotationSyncs++
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
	if w.current == nil {
		return fmt.Errorf("logwal: sync on closed WAL")
	}
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

// Checkpoint discards all current WAL segments and starts a fresh one. It is
// called after a whole-head flush has persisted every buffered record to a chunk,
// so the segments hold only already-flushed data and are safe to delete. The
// caller (logs.Store.flush) holds its own lock, so no append is in flight.
//
// Checkpoint fails closed: if a segment removal fails partway, the WAL is left
// closed (w.current == nil) and unusable rather than in a torn half-state. A
// surviving already-flushed segment that a later restart replays is harmless —
// logs.Store merges head and chunk reads and dedups by (streamID, tsNs, line).
func (w *LogWAL) Checkpoint() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.current == nil {
		return fmt.Errorf("logwal: checkpoint on closed WAL")
	}
	if w.broken {
		return fmt.Errorf("logwal: writer is in broken state due to previous rotation failure")
	}
	// Sync before close, matching Close/rotate and the documented "sync + close"
	// contract: it also surfaces any latent write error on the outgoing segment
	// before we drop it. A sync failure here returns early — the segment is neither
	// closed nor deleted and w.current stays valid, so the checkpoint is retryable.
	if err := syncFile(w.current); err != nil {
		return fmt.Errorf("logwal: sync on checkpoint: %w", err)
	}
	if err := w.current.Close(); err != nil {
		return fmt.Errorf("logwal: close on checkpoint: %w", err)
	}
	w.current = nil
	paths, err := segmentPaths(w.dir)
	if err != nil {
		return err
	}
	for _, p := range paths {
		if err := os.Remove(p); err != nil {
			return fmt.Errorf("logwal: remove segment %s on checkpoint: %w", p, err)
		}
	}
	if err := fsutil.SyncDir(w.dir); err != nil {
		return fmt.Errorf("logwal: fsync dir on checkpoint: %w", err)
	}
	w.broken = false
	// All segments deleted, so numbering restarts at 1. openSegment fsyncs the dir.
	return w.openSegment(1)
}

// segmentPaths returns all .wal file paths in dir sorted by ascending numeric
// segment index. The names use %06d, which is only a minimum width, so a lexical
// sort would misorder once the index needs a 7th digit (1000000.wal would sort
// before 999999.wal) — corrupting replay order and which segment is treated as
// final. Sorting on the parsed integer keeps ordering correct across that
// rollover. Non-numeric .wal names (never produced by this WAL) are skipped.
// Returns nil (not an error) when dir does not exist.
func segmentPaths(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("logwal: readdir %s: %w", dir, err)
	}
	type seg struct {
		idx  int
		path string
	}
	var segs []seg
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".wal") {
			continue
		}
		idx, cerr := strconv.Atoi(strings.TrimSuffix(e.Name(), ".wal"))
		if cerr != nil {
			continue
		}
		segs = append(segs, seg{idx: idx, path: filepath.Join(dir, e.Name())})
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].idx < segs[j].idx })
	paths := make([]string, len(segs))
	for i, s := range segs {
		paths[i] = s.path
	}
	return paths, nil
}
