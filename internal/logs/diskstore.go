package logs

import (
	"context"
	"encoding/binary"
	"sort"

	"github.com/masonwheeler/observability-platform/internal/storage/index"
	"github.com/masonwheeler/observability-platform/internal/storage/logchunk"
	"github.com/masonwheeler/observability-platform/internal/storage/logwal"
)

// logWAL is the WAL surface Head needs: durable append, whole-head checkpoint, close.
type logWAL interface {
	WriteRecord(labels []logwal.LabelPair, tsNs int64, line string) error
	Checkpoint() error
	Close() error
}

// Store is the all-in-one log store: a Head whose flushes go to a local
// ChunkStore. Safe for concurrent use. Every read reads the head before the
// chunk store (see StreamEntries), and its API is what it was before the two
// halves could run apart.
type Store struct {
	head   *Head
	chunks *ChunkStore
}

// NewStore opens (or creates) a log store rooted at the given directories,
// loading the persisted index (rebuilding from a chunk scan if the manifest is
// corrupt) and replaying the WAL into the head.
func NewStore(walDir, chunksDir, indexDir string, segMaxBytes int64, syncEveryN int, flushThreshold int64) (*Store, error) {
	chunks, err := OpenChunkStore(chunksDir, indexDir)
	if err != nil {
		return nil, err
	}
	head, err := OpenHead(walDir, segMaxBytes, syncEveryN, flushThreshold, chunks)
	if err != nil {
		return nil, err
	}
	return &Store{head: head, chunks: chunks}, nil
}

// Append writes the record to the WAL, buffers it in the head, and flushes the
// whole head when buffered bytes cross the threshold.
func (s *Store) Append(labels StreamLabels, tsNs int64, line string) error {
	return s.head.Append(labels, tsNs, line)
}

// Flush drains the head to chunks + index and checkpoints the WAL. Safe to call
// when the head is empty (no-op).
func (s *Store) Flush() error { return s.head.Flush() }

// Close flushes the head (draining it durably) and closes the WAL, returning
// both errors if both fail.
func (s *Store) Close() error { return s.head.Close() }

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

// MatchingStreamIDs returns the sorted stream IDs matching all matchers, across
// both the persisted index and the still-buffered head.
func (s *Store) MatchingStreamIDs(matchers []index.Pair) []StreamID {
	set := make(map[StreamID]struct{})
	for _, id := range s.head.MatchingStreamIDs(matchers) {
		set[id] = struct{}{}
	}
	for _, id := range s.chunks.MatchingStreamIDs(matchers) {
		set[id] = struct{}{}
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
// The head is read first. A flush holds the head's lock from snapshot to reset
// and makes the chunks readable before resetting, so an entry missing from this
// head read was already in the chunk store when that read began.
func (s *Store) StreamEntries(ctx context.Context, id StreamID, minTs, maxTs int64) ([]LogEntry, error) {
	head := s.head.headEntries(id)
	persisted, err := s.chunks.StreamEntries(ctx, id, minTs, maxTs)
	if err != nil {
		return nil, err
	}
	return mergeEntries(persisted, head, minTs, maxTs), nil
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

// Stats reports distinct stream count, persisted chunk-file count, and the total
// on-disk bytes of those chunk files. It is read at metrics scrape time.
//
// Streams are deduplicated across the head and the index: after a flush the same
// stream exists in both, and counting it twice would make the gauge climb on every
// flush without a single new stream being created.
func (s *Store) Stats() (streams, chunks int, bytes int64, err error) {
	ids := s.head.streamIDs()
	for id := range s.chunks.streamIDs() {
		ids[id] = struct{}{}
	}
	chunks, bytes, err = s.chunks.fileStats()
	if err != nil {
		return 0, 0, 0, err
	}
	return len(ids), chunks, bytes, nil
}

// StreamLabelSet returns a stream's labels. Every read reads the head first,
// falling back to the persisted index: a flush holds the head lock from
// snapshot to reset, so a lookup that blocks on that lock and only proceeds
// once the flush is done still finds the stream in the chunk store, even
// though it is no longer in the head by then. Checking the chunk store first
// would miss it in exactly that window (chunks not yet written) and then find
// the head already cleared. Stream labels are stable for a given id across a
// concurrent flush.
func (s *Store) StreamLabelSet(id StreamID) (StreamLabels, bool) {
	if l, ok := s.head.StreamLabelSet(id); ok {
		return l, true
	}
	return s.chunks.StreamLabelSet(id)
}

// LabelNames returns all stream label names across head + persisted index, sorted, unique.
func (s *Store) LabelNames() []string {
	set := make(map[string]struct{})
	for _, n := range s.head.LabelNames() {
		set[n] = struct{}{}
	}
	for _, n := range s.chunks.LabelNames() {
		set[n] = struct{}{}
	}
	return sortedKeys(set)
}

// LabelValues returns all values for name across head + persisted index, sorted, unique.
func (s *Store) LabelValues(name string) []string {
	set := make(map[string]struct{})
	for _, v := range s.head.LabelValues(name) {
		set[v] = struct{}{}
	}
	for _, v := range s.chunks.LabelValues(name) {
		set[v] = struct{}{}
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
	s.head.mu.Lock()
	defer s.head.mu.Unlock()
	return s.chunks.IngestStreams(context.Background(), s.head.snapshotLocked())
}

// closeWALForTest closes only the WAL, leaving chunks/index in place — used with
// writeChunksAndIndexForTest to simulate a crash before checkpoint.
func (s *Store) closeWALForTest() error { return s.head.wal.Close() }
