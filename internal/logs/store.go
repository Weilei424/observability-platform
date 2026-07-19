package logs

import (
	"sort"
	"sync"

	"github.com/masonwheeler/observability-platform/internal/storage/logwal"
)

// Ingester accepts log entries for storage.
type Ingester interface {
	Append(labels StreamLabels, tsNs int64, line string) error
}

type memoryStream struct {
	labels  StreamLabels
	entries []LogEntry
}

// MemoryStore is an in-memory per-stream buffer of log entries. Safe for
// concurrent use. Out-of-order lines are accepted in insertion order; ordering is
// resolved at query time in Phase 4.4. The real chunk format arrives in Phase 4.3.
type MemoryStore struct {
	mu      sync.RWMutex
	streams map[StreamID]*memoryStream
}

// NewMemoryStore returns an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{streams: make(map[StreamID]*memoryStream)}
}

// Append buffers a log line for the stream identified by labels.
func (s *MemoryStore) Append(labels StreamLabels, tsNs int64, line string) error {
	id := StreamIDOf(labels)
	s.mu.Lock()
	defer s.mu.Unlock()
	ms, ok := s.streams[id]
	if !ok {
		ms = &memoryStream{labels: labels}
		s.streams[id] = ms
	}
	ms.entries = append(ms.entries, LogEntry{StreamID: id, TimestampNs: tsNs, Line: line})
	return nil
}

// StreamEntries returns a defensive copy of the buffered entries for id, or nil
// if the stream is unknown. It is the minimal read surface the Phase 4.4 query
// engine builds on.
func (s *MemoryStore) StreamEntries(id StreamID) []LogEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ms, ok := s.streams[id]
	if !ok {
		return nil
	}
	out := make([]LogEntry, len(ms.entries))
	copy(out, ms.entries)
	return out
}

// StreamCount returns the number of distinct streams buffered.
func (s *MemoryStore) StreamCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.streams)
}

var _ Ingester = (*MemoryStore)(nil)

// WALStore writes each log record to the log WAL before buffering it in memory.
// Reads delegate to the embedded MemoryStore. Safe for concurrent use.
type WALStore struct {
	w     logwal.RecordWriter
	store *MemoryStore
}

// NewWALStore returns a WALStore backed by w for durability and store for buffering.
func NewWALStore(w logwal.RecordWriter, store *MemoryStore) *WALStore {
	return &WALStore{w: w, store: store}
}

// Append writes the WAL record first. If the WAL write fails the entry is not
// buffered and the error is returned.
func (s *WALStore) Append(labels StreamLabels, tsNs int64, line string) error {
	if err := s.w.WriteRecord(labelsToWALPairs(labels), tsNs, line); err != nil {
		return err
	}
	return s.store.Append(labels, tsNs, line)
}

// StreamEntries delegates to the embedded MemoryStore.
func (s *WALStore) StreamEntries(id StreamID) []LogEntry { return s.store.StreamEntries(id) }

// StreamCount delegates to the embedded MemoryStore.
func (s *WALStore) StreamCount() int { return s.store.StreamCount() }

var _ Ingester = (*WALStore)(nil)

func labelsToWALPairs(l StreamLabels) []logwal.LabelPair {
	m := l.Map()
	pairs := make([]logwal.LabelPair, 0, len(m))
	for name, value := range m {
		pairs = append(pairs, logwal.LabelPair{Name: name, Value: value})
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].Name < pairs[j].Name })
	return pairs
}
