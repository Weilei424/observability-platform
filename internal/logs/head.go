package logs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"

	"github.com/masonwheeler/observability-platform/internal/storage/fsutil"
	"github.com/masonwheeler/observability-platform/internal/storage/index"
	"github.com/masonwheeler/observability-platform/internal/storage/logwal"
)

// Head is the logs write path's in-memory half: a WAL-backed per-stream buffer
// that flushes the whole head to a ChunkSink at a size threshold and on Close,
// checkpointing the WAL once the sink has everything.
//
// The flush holds the lock from snapshot to reset. LogWAL.Checkpoint deletes
// every segment, so it is only correct when nothing can be appended while the
// flush runs; holding the lock is how that is guaranteed. It also means a
// reader that takes the lock sees either the whole head or an empty one whose
// entries the sink already has — never a moment in which an entry is readable
// nowhere.
type Head struct {
	mu          sync.Mutex
	head        map[StreamID]*memoryStream
	wal         logWAL
	sink        ChunkSink
	headBytes   int64
	flushThresh int64
}

var _ Reader = (*Head)(nil)

// OpenHead replays the WAL in walDir into a new head and opens the WAL for
// appends. Flushes go to sink.
func OpenHead(walDir string, segMaxBytes int64, syncEveryN int, flushThreshold int64, sink ChunkSink) (*Head, error) {
	if err := fsutil.MkdirAllSync(walDir); err != nil {
		return nil, fmt.Errorf("logs: mkdir %s: %w", walDir, err)
	}
	head := make(map[StreamID]*memoryStream)
	var headBytes int64
	if err := logwal.Replay(walDir, func(pairs []logwal.LabelPair, tsNs int64, line string) {
		m := make(map[string]string, len(pairs))
		for _, p := range pairs {
			m[p.Name] = p.Value
		}
		sl, err := NewStreamLabels(m)
		if err != nil {
			// Skip the record, but say so. Log WAL records carry no checksum, so a
			// structurally valid record can still hold semantically corrupt labels
			// and reach this path; dropping it silently makes real data loss
			// invisible to whoever is reading the startup logs. Warning here matches
			// the metrics replay path, and reaches the application logger because
			// main sets slog's default.
			slog.Warn("logs WAL replay: skipping record with invalid stream labels",
				"component", "logs", "error", err.Error())
			return
		}
		id := StreamIDOf(sl)
		hs := head[id]
		if hs == nil {
			hs = &memoryStream{labels: sl}
			head[id] = hs
		}
		hs.entries = append(hs.entries, LogEntry{StreamID: id, TimestampNs: tsNs, Line: line})
		headBytes += int64(8 + len(line))
	}); err != nil {
		return nil, fmt.Errorf("logs: WAL replay: %w", err)
	}
	lw, err := logwal.Open(walDir, segMaxBytes, syncEveryN)
	if err != nil {
		return nil, fmt.Errorf("logs: open WAL: %w", err)
	}
	return &Head{head: head, wal: lw, sink: sink, headBytes: headBytes, flushThresh: flushThreshold}, nil
}

// Append writes the record to the WAL, buffers it in the head, and flushes the
// whole head when buffered bytes cross the threshold.
func (h *Head) Append(labels StreamLabels, tsNs int64, line string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.wal.WriteRecord(labelsToWALPairs(labels), tsNs, line); err != nil {
		return err
	}
	id := StreamIDOf(labels)
	hs := h.head[id]
	if hs == nil {
		hs = &memoryStream{labels: labels}
		h.head[id] = hs
	}
	hs.entries = append(hs.entries, LogEntry{StreamID: id, TimestampNs: tsNs, Line: line})
	h.headBytes += int64(8 + len(line))
	if h.flushThresh > 0 && h.headBytes >= h.flushThresh {
		_, err := h.flushLocked()
		return err
	}
	return nil
}

// Flush drains the head to the sink and checkpoints the WAL. Safe to call when
// the head is empty (no-op).
func (h *Head) Flush() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, err := h.flushLocked()
	return err
}

// Close flushes the head and closes the WAL, returning both errors if both
// fail. The WAL is closed even when the flush fails, and that ordering is the
// point: a failed flush is exactly when the WAL matters most — it is then the
// only durable copy of the head, the one the next start replays from — and
// LogWAL.Close is what fsyncs its tail, which with batched syncing may hold
// records already acknowledged to clients.
func (h *Head) Close() error {
	h.mu.Lock()
	_, flushErr := h.flushLocked()
	h.mu.Unlock()
	return errors.Join(flushErr, h.wal.Close())
}

// flushLocked sends every head stream to the sink, checkpoints the WAL, then
// resets the head, reporting whether there was anything to flush. The caller
// holds h.mu.
func (h *Head) flushLocked() (bool, error) {
	if len(h.head) == 0 {
		return false, nil
	}
	if err := h.sink.IngestStreams(context.Background(), h.snapshotLocked()); err != nil {
		return true, err
	}
	if err := h.wal.Checkpoint(); err != nil {
		return true, err
	}
	h.head = make(map[StreamID]*memoryStream)
	h.headBytes = 0
	return true, nil
}

// snapshotLocked returns every head stream, ascending by ID, with its entries
// in append order. The caller holds h.mu.
func (h *Head) snapshotLocked() []StreamData {
	ids := make([]StreamID, 0, len(h.head))
	for id := range h.head {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	out := make([]StreamData, 0, len(ids))
	for _, id := range ids {
		hs := h.head[id]
		out = append(out, StreamData{Labels: hs.labels, Entries: append([]LogEntry(nil), hs.entries...)})
	}
	return out
}

// headEntries returns a copy of id's buffered entries in append order.
func (h *Head) headEntries(id StreamID) []LogEntry {
	h.mu.Lock()
	defer h.mu.Unlock()
	if hs := h.head[id]; hs != nil {
		return append([]LogEntry(nil), hs.entries...)
	}
	return nil
}

// MatchingStreamIDs returns the sorted IDs of head streams matching all matchers.
func (h *Head) MatchingStreamIDs(matchers []index.Pair) []StreamID {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []StreamID
	for id, hs := range h.head {
		if streamMatches(hs.labels, matchers) {
			out = append(out, id)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// StreamLabelSet returns a head stream's labels.
func (h *Head) StreamLabelSet(id StreamID) (StreamLabels, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if hs := h.head[id]; hs != nil {
		return hs.labels, true
	}
	return StreamLabels{}, false
}

// StreamEntries returns the head's entries of id in [minTs, maxTs], stable-
// sorted by timestamp and deduplicated by (ts, line).
func (h *Head) StreamEntries(_ context.Context, id StreamID, minTs, maxTs int64) ([]LogEntry, error) {
	return mergeEntries(nil, h.headEntries(id), minTs, maxTs), nil
}

// LabelNames returns head stream label names, sorted.
func (h *Head) LabelNames() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	set := make(map[string]struct{})
	for _, hs := range h.head {
		for n := range hs.labels.Map() {
			set[n] = struct{}{}
		}
	}
	return sortedKeys(set)
}

// LabelValues returns head values for name, sorted.
func (h *Head) LabelValues(name string) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	set := make(map[string]struct{})
	for _, hs := range h.head {
		if v, ok := hs.labels.Get(name); ok {
			set[v] = struct{}{}
		}
	}
	return sortedKeys(set)
}

// StreamCount returns the number of streams buffered in the head.
func (h *Head) StreamCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.head)
}

// streamIDs returns the set of head stream IDs.
func (h *Head) streamIDs() map[StreamID]struct{} {
	h.mu.Lock()
	defer h.mu.Unlock()
	ids := make(map[StreamID]struct{}, len(h.head))
	for id := range h.head {
		ids[id] = struct{}{}
	}
	return ids
}

// mergeEntries appends to persisted (sorted, deduplicated) the entries of head
// in [minTs, maxTs] it does not already hold, then stable-sorts by timestamp —
// so at an equal timestamp persisted entries stay ahead of head entries, the
// order Store.StreamEntries has always produced and the engine's tie-breaking
// depends on.
func mergeEntries(persisted, head []LogEntry, minTs, maxTs int64) []LogEntry {
	type key struct {
		ts   int64
		line string
	}
	seen := make(map[key]struct{}, len(persisted))
	for _, e := range persisted {
		seen[key{e.TimestampNs, e.Line}] = struct{}{}
	}
	out := persisted
	for _, e := range head {
		if e.TimestampNs < minTs || e.TimestampNs > maxTs {
			continue
		}
		k := key{e.TimestampNs, e.Line}
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, e)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].TimestampNs < out[j].TimestampNs })
	return out
}
