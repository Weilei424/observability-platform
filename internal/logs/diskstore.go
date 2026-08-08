package logs

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sort"
	"sync"

	"github.com/masonwheeler/observability-platform/internal/storage/fsutil"
	"github.com/masonwheeler/observability-platform/internal/storage/index"
	"github.com/masonwheeler/observability-platform/internal/storage/logchunk"
	"github.com/masonwheeler/observability-platform/internal/storage/logwal"
)

// logWAL is the WAL surface Store needs: durable append, whole-head checkpoint, close.
type logWAL interface {
	WriteRecord(labels []logwal.LabelPair, tsNs int64, line string) error
	Checkpoint() error
	Close() error
}

// Store is the production log store: a WAL-backed in-memory head that flushes the
// whole head to compressed chunk files plus a persisted index at a size threshold
// and on shutdown, checkpointing the WAL on each flush. Safe for concurrent use.
type Store struct {
	mu          sync.Mutex
	head        map[StreamID]*memoryStream
	wal         logWAL
	index       *streamIndex
	chunksDir   string
	indexPath   string
	headBytes   int64
	flushThresh int64
}

// NewStore opens (or creates) a log store rooted at the given directories, loading
// the persisted index (rebuilding from a chunk scan if the manifest is corrupt) and
// replaying the WAL into the head.
func NewStore(walDir, chunksDir, indexDir string, segMaxBytes int64, syncEveryN int, flushThreshold int64) (*Store, error) {
	for _, d := range []string{walDir, chunksDir, indexDir} {
		if err := fsutil.MkdirAllSync(d); err != nil {
			return nil, fmt.Errorf("logs: mkdir %s: %w", d, err)
		}
	}
	indexPath := filepath.Join(indexDir, "streams.index")

	idx, err := loadManifest(indexPath)
	if err != nil {
		// Missing OR corrupt manifest: rebuild from the authoritative chunk headers,
		// then rewrite. The manifest is a rebuildable cache; chunks are the source of
		// truth. (rebuildFromScan on an empty chunks dir yields an empty index.)
		idx, err = rebuildFromScan(chunksDir)
		if err != nil {
			return nil, err
		}
		if err := idx.writeManifest(indexPath); err != nil {
			return nil, err
		}
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
			// the metrics replay path in cmd/server/main.go, and reaches the
			// application logger because main.go calls slog.SetDefault.
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

	return &Store{
		head:        head,
		wal:         lw,
		index:       idx,
		chunksDir:   chunksDir,
		indexPath:   indexPath,
		headBytes:   headBytes,
		flushThresh: flushThreshold,
	}, nil
}

// Append writes the record to the WAL, buffers it in the head, and flushes the
// whole head when buffered bytes cross the threshold.
func (s *Store) Append(labels StreamLabels, tsNs int64, line string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.wal.WriteRecord(labelsToWALPairs(labels), tsNs, line); err != nil {
		return err
	}
	id := StreamIDOf(labels)
	hs := s.head[id]
	if hs == nil {
		hs = &memoryStream{labels: labels}
		s.head[id] = hs
	}
	hs.entries = append(hs.entries, LogEntry{StreamID: id, TimestampNs: tsNs, Line: line})
	s.headBytes += int64(8 + len(line))
	if s.flushThresh > 0 && s.headBytes >= s.flushThresh {
		return s.flushLocked()
	}
	return nil
}

// flushLocked persists every head stream to a chunk, writes the manifest,
// checkpoints the WAL, then resets the head. The caller holds s.mu.
func (s *Store) flushLocked() error {
	if len(s.head) == 0 {
		return nil
	}
	if err := s.writeChunksAndIndexLocked(); err != nil {
		return err
	}
	if err := s.wal.Checkpoint(); err != nil {
		return err
	}
	s.head = make(map[StreamID]*memoryStream)
	s.headBytes = 0
	return nil
}

// writeChunksAndIndexLocked builds and persists a chunk per head stream and writes
// the manifest, without touching the WAL or resetting the head. The caller holds s.mu.
func (s *Store) writeChunksAndIndexLocked() error {
	ids := make([]StreamID, 0, len(s.head))
	for id := range s.head {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		hs := s.head[id]
		// Split so no chunk exceeds the decoder's cap: an oversized chunk would be
		// written and its WAL checkpointed, then be rejected on read (data loss).
		for _, c := range splitIntoChunks(hs.entries, logchunk.MaxUncompressedBytes) {
			ref, err := writeChunkFile(s.chunksDir, id, hs.labels, c)
			if err != nil {
				return err
			}
			s.index.add(id, hs.labels, ref)
		}
	}
	return s.index.writeManifest(s.indexPath)
}

// maxEntryEncodingOverhead bounds a single entry's non-line encoding cost in the
// chunk block: a signed varint timestamp delta plus a uvarint line length, each at
// most binary.MaxVarintLen64 bytes.
const maxEntryEncodingOverhead = 2 * binary.MaxVarintLen64

// splitIntoChunks packs entries (in order) into chunks whose uncompressed size
// stays at or below maxUncompressed, starting a new chunk before an entry would
// push the current one over. A single entry is bounded by logs.MaxLineBytes at
// ingest, which is far below the cap, so every chunk holds at least one entry.
func splitIntoChunks(entries []LogEntry, maxUncompressed int) []*logchunk.Chunk {
	var out []*logchunk.Chunk
	cur := logchunk.NewChunk()
	for _, e := range entries {
		entryMax := len(e.Line) + maxEntryEncodingOverhead
		if cur.NumEntries() > 0 && cur.UncompressedBytes()+entryMax > maxUncompressed {
			out = append(out, cur)
			cur = logchunk.NewChunk()
		}
		cur.Append(e.TimestampNs, e.Line)
	}
	if cur.NumEntries() > 0 {
		out = append(out, cur)
	}
	return out
}

// Close flushes the head (draining it durably) and closes the WAL, returning both
// errors if both fail.
//
// The WAL is closed even when the flush fails, and that ordering is the point: a
// failed flush is exactly when the WAL matters most. It is then the only durable
// copy of the head, the one the next start replays from — and LogWAL.Close is
// what fsyncs its tail, which with batched syncing (WALSyncEveryN) may hold
// records already acknowledged to clients. Returning early on a flush error left
// those unsynced and lost them, inverting the guarantee the flush failure was
// supposed to preserve.
func (s *Store) Close() error {
	s.mu.Lock()
	flushErr := s.flushLocked()
	s.mu.Unlock()
	return errors.Join(flushErr, s.wal.Close())
}

// MatchingStreamIDs returns the sorted stream IDs matching all matchers, across
// both the persisted index and the still-buffered head.
func (s *Store) MatchingStreamIDs(matchers []index.Pair) []StreamID {
	s.mu.Lock()
	defer s.mu.Unlock()
	set := make(map[StreamID]struct{})
	for _, id := range s.index.matchingStreamIDs(matchers) {
		set[id] = struct{}{}
	}
	for id, hs := range s.head {
		if streamMatches(hs.labels, matchers) {
			set[id] = struct{}{}
		}
	}
	out := make([]StreamID, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// StreamEntries returns the stream's entries in [minTs, maxTs] from persisted
// chunks and the head, sorted by timestamp and deduped by (tsNs, line). The dedup
// neutralizes the flush crash window (chunk written, WAL not yet checkpointed).
//
// Only the index lookup and the head copy hold s.mu; chunk files are read and
// decompressed outside it, so a query over cold chunks does not block ingestion.
// That is safe because a chunk file is immutable once written and is never
// deleted or rewritten (logs have no compaction or retention yet) — a ref taken
// under the lock stays readable afterwards. Revisit when logs retention lands.
func (s *Store) StreamEntries(ctx context.Context, id StreamID, minTs, maxTs int64) ([]LogEntry, error) {
	s.mu.Lock()
	refs := append([]ChunkRef(nil), s.index.chunkRefs(id, minTs, maxTs)...)
	var headEntries []LogEntry
	if hs := s.head[id]; hs != nil {
		headEntries = append([]LogEntry(nil), hs.entries...)
	}
	s.mu.Unlock()

	type key struct {
		ts   int64
		line string
	}
	seen := make(map[key]struct{})
	var out []LogEntry

	for _, ref := range refs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		gotID, _, c, err := readChunkFile(filepath.Join(s.chunksDir, ref.Name))
		if err != nil {
			return nil, err
		}
		// Guard against an index ref pointing at another stream's chunk: the chunk
		// file embeds its own stream ID, so verify it matches the one we queried.
		if gotID != id {
			return nil, fmt.Errorf("logs: chunk %s belongs to stream %d, not %d", ref.Name, gotID, id)
		}
		it := c.Iterator()
		for it.Next() {
			ts, line := it.At()
			if ts < minTs || ts > maxTs {
				continue
			}
			k := key{ts, line}
			if _, dup := seen[k]; dup {
				continue
			}
			seen[k] = struct{}{}
			out = append(out, LogEntry{StreamID: id, TimestampNs: ts, Line: line})
		}
	}
	for _, e := range headEntries {
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
	return out, nil
}

// streamMatches reports whether labels contain every matcher name=value pair.
func streamMatches(labels StreamLabels, matchers []index.Pair) bool {
	for _, m := range matchers {
		v, ok := labels.Get(m.Name)
		if !ok || v != m.Value {
			return false
		}
	}
	return true
}

// Flush drains the head to chunks + index and checkpoints the WAL. Safe to call
// when the head is empty (no-op).
func (s *Store) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushLocked()
}

// StreamLabelSet returns a stream's labels from the persisted index, or from the
// still-buffered head. Stream labels are stable for a given id across a concurrent flush.
func (s *Store) StreamLabelSet(id StreamID) (StreamLabels, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if l, ok := s.index.labels[id]; ok {
		return l, true
	}
	if hs := s.head[id]; hs != nil {
		return hs.labels, true
	}
	return StreamLabels{}, false
}

// LabelNames returns all stream label names across head + persisted index, sorted, unique.
func (s *Store) LabelNames() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	set := make(map[string]struct{})
	for _, n := range s.index.postings.LabelNames() {
		set[n] = struct{}{}
	}
	for _, hs := range s.head {
		for n := range hs.labels.Map() {
			set[n] = struct{}{}
		}
	}
	return sortedKeys(set)
}

// LabelValues returns all values for name across head + persisted index, sorted, unique.
func (s *Store) LabelValues(name string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	set := make(map[string]struct{})
	for _, v := range s.index.postings.LabelValues(name) {
		set[v] = struct{}{}
	}
	for _, hs := range s.head {
		if v, ok := hs.labels.Get(name); ok {
			set[v] = struct{}{}
		}
	}
	return sortedKeys(set)
}

func sortedKeys(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

var _ Ingester = (*Store)(nil)
var _ Reader = (*Store)(nil)

// writeChunksAndIndexForTest persists the head to chunks + manifest WITHOUT
// checkpointing the WAL or resetting the head — used only to simulate the flush
// crash window in tests.
func (s *Store) writeChunksAndIndexForTest() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeChunksAndIndexLocked()
}

// closeWALForTest closes only the WAL, leaving chunks/index in place — used with
// writeChunksAndIndexForTest to simulate a crash before checkpoint.
func (s *Store) closeWALForTest() error { return s.wal.Close() }
