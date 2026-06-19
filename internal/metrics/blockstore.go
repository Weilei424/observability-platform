package metrics

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/masonwheeler/observability-platform/internal/storage/block"
)

// BlockStore wraps a MemoryStore and a list of loaded block Readers.
// It implements the Store interface: reads fan out to both memory and
// persisted blocks; writes go to memory only (the WAL layer adds durability).
// FlushBlock drains sealed chunks from memory into a new immutable block.
// Safe for concurrent use.
type BlockStore struct {
	mem      *MemoryStore
	blocks   []*block.Reader
	blockDir string
	tmpDir   string
	mu       sync.RWMutex
	flushMu  sync.Mutex // serializes concurrent FlushBlock calls
}

// NewBlockStore loads existing blocks from dataDir/metrics/blocks/ and prepares
// the temp directory at dataDir/metrics/tmp/. Orphaned temp directories from
// previous incomplete flushes are removed.
func NewBlockStore(dataDir string) (*BlockStore, error) {
	blockDir := filepath.Join(dataDir, "metrics", "blocks")
	tmpDir := filepath.Join(dataDir, "metrics", "tmp")

	for _, d := range []string{blockDir, tmpDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, fmt.Errorf("blockstore: mkdir %s: %w", d, err)
		}
	}

	// Clean up orphaned temp directories.
	entries, err := os.ReadDir(tmpDir)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("blockstore: read tmp dir: %w", err)
	}
	for _, e := range entries {
		if e.IsDir() {
			_ = os.RemoveAll(filepath.Join(tmpDir, e.Name()))
		}
	}

	// Load existing blocks sorted by MinTime.
	blockEntries, err := os.ReadDir(blockDir)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("blockstore: read block dir: %w", err)
	}
	readers := make([]*block.Reader, 0, len(blockEntries))
	for _, e := range blockEntries {
		if !e.IsDir() {
			continue
		}
		r, err := block.OpenReader(filepath.Join(blockDir, e.Name()))
		if err != nil {
			return nil, fmt.Errorf("blockstore: open block %s: %w", e.Name(), err)
		}
		readers = append(readers, r)
	}
	sort.Slice(readers, func(i, j int) bool {
		return readers[i].Meta().MinTime < readers[j].Meta().MinTime
	})

	return &BlockStore{
		mem:      NewMemoryStore(),
		blocks:   readers,
		blockDir: blockDir,
		tmpDir:   tmpDir,
	}, nil
}

// Append adds a sample to the in-memory store.
func (bs *BlockStore) Append(labels Labels, tsMs int64, val float64) error {
	return bs.mem.Append(labels, tsMs, val)
}

// AppendTracked is like Append but records walSeg in the head-chunk fence.
// Called from WALStore.Append so that FlushBlock can compute a safe WAL
// deletion boundary via OldestHeadSegment.
func (bs *BlockStore) AppendTracked(labels Labels, tsMs int64, val float64, walSeg int) error {
	return bs.mem.AppendTracked(labels, tsMs, val, walSeg)
}

// OldestHeadSegment returns the WAL-segment floor across all in-memory head
// chunks. Returns -1 when no series has chunks.
func (bs *BlockStore) OldestHeadSegment() int {
	return bs.mem.OldestHeadSegment()
}

// SelectSeries returns all series matching sel from memory and all loaded
// blocks, resolved via label-index postings. Results are deduplicated by
// series fingerprint (memory wins).
func (bs *BlockStore) SelectSeries(sel Selector) []MatchedSeries {
	result := bs.mem.SelectSeries(sel)
	seen := make(map[SeriesID]struct{}, len(result))
	for _, ms := range result {
		seen[ms.Labels.Fingerprint()] = struct{}{}
	}

	matchers := selectorToIndexMatchers(sel)

	bs.mu.RLock()
	readers := bs.blocks
	bs.mu.RUnlock()

	for _, r := range readers {
		ids, err := r.Postings(matchers)
		if err != nil {
			// Consistent with the existing SelectSeries policy of skipping a
			// block on a per-series decode error: skip this block's postings
			// rather than failing the whole (error-free) SelectSeries contract.
			continue
		}
		if len(ids) == 0 {
			continue
		}
		want := make(map[uint64]struct{}, len(ids))
		for _, id := range ids {
			want[id] = struct{}{}
		}
		for _, se := range r.Series() {
			if _, ok := want[se.ID]; !ok {
				continue
			}
			labels, err := blockPairsToLabels(se.Labels)
			if err != nil {
				continue
			}
			fp := labels.Fingerprint()
			if _, already := seen[fp]; already {
				continue
			}
			seen[fp] = struct{}{}
			result = append(result, MatchedSeries{id: SeriesID(se.ID), Labels: labels})
		}
	}
	return result
}

// LabelNames returns the sorted, deduplicated label names across memory and all
// loaded blocks.
func (bs *BlockStore) LabelNames() []string {
	set := make(map[string]struct{})
	for _, n := range bs.mem.LabelNames() {
		set[n] = struct{}{}
	}
	bs.mu.RLock()
	readers := bs.blocks
	bs.mu.RUnlock()
	for _, r := range readers {
		for _, n := range r.LabelNames() {
			set[n] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for n := range set {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// LabelValues returns the sorted, deduplicated values for name across memory and
// all loaded blocks.
func (bs *BlockStore) LabelValues(name string) []string {
	set := make(map[string]struct{})
	for _, v := range bs.mem.LabelValues(name) {
		set[v] = struct{}{}
	}
	bs.mu.RLock()
	readers := bs.blocks
	bs.mu.RUnlock()
	for _, r := range readers {
		for _, v := range r.LabelValues(name) {
			set[v] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for v := range set {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

// Cardinality returns distinct counts of series, label names, and label pairs
// across memory and all loaded blocks (deduplicated by series fingerprint).
func (bs *BlockStore) Cardinality() (series, names, pairs int) {
	seriesSet := make(map[SeriesID]struct{})
	nameSet := make(map[string]struct{})
	pairSet := make(map[[2]string]struct{})

	add := func(l Labels, id SeriesID) {
		seriesSet[id] = struct{}{}
		for n, v := range l.Map() {
			nameSet[n] = struct{}{}
			pairSet[[2]string{n, v}] = struct{}{}
		}
	}
	for _, ms := range bs.mem.SelectSeries(Selector{}) {
		add(ms.Labels, ms.Labels.Fingerprint())
	}
	bs.mu.RLock()
	readers := bs.blocks
	bs.mu.RUnlock()
	for _, r := range readers {
		for _, se := range r.Series() {
			labels, err := blockPairsToLabels(se.Labels)
			if err != nil {
				continue
			}
			add(labels, labels.Fingerprint())
		}
	}
	return len(seriesSet), len(nameSet), len(pairSet)
}

// QueryInstant returns the latest sample with timestamp ≤ tMs for the given
// series, searching both memory and all loaded blocks. Memory wins for equal
// timestamps. Returns an error if any block chunk cannot be read.
func (bs *BlockStore) QueryInstant(id SeriesID, tMs int64) (Sample, bool, error) {
	best, found, err := bs.mem.QueryInstant(id, tMs)
	if err != nil {
		return Sample{}, false, err
	}

	bs.mu.RLock()
	readers := bs.blocks
	bs.mu.RUnlock()

	for _, r := range readers {
		if r.Meta().MinTime > tMs {
			continue
		}
		for _, se := range r.Series() {
			if SeriesID(se.ID) != id {
				continue
			}
			for _, ref := range se.Chunks {
				c, err := r.ReadChunk(ref)
				if err != nil {
					return Sample{}, false, fmt.Errorf("blockstore: read chunk: %w", err)
				}
				it := c.Iterator()
				for it.Next() {
					ts, val := it.At()
					// Memory wins for equal timestamps; block only wins if strictly better.
					if ts <= tMs && (!found || ts > best.TimestampMs) {
						best = Sample{SeriesID: id, TimestampMs: ts, Value: val}
						found = true
					}
				}
			}
		}
	}
	return best, found, nil
}

// QueryRange returns all samples for series id with startMs ≤ ts ≤ endMs,
// merging memory and block results. Results are sorted by timestamp;
// duplicate timestamps are deduplicated (memory wins over block).
// Returns nil, nil if the series is unknown in both blocks and memory; returns a
// non-nil empty slice if the series is known but has no in-range samples.
// Returns an error if any block chunk cannot be read.
func (bs *BlockStore) QueryRange(id SeriesID, startMs, endMs int64) ([]Sample, error) {
	// Read memory before snapshotting the block list. This closes the window
	// where sealed chunks are discarded from memory but the new block reader
	// hasn't been captured yet: FlushBlock always registers the reader before
	// discarding (see FlushBlock), so if memory is empty the reader is already
	// present and the subsequent block snapshot will include it.
	memResult, err := bs.mem.QueryRange(id, startMs, endMs)
	if err != nil {
		return nil, err
	}

	bs.mu.RLock()
	readers := bs.blocks
	bs.mu.RUnlock()

	// Collect block samples; memory samples appended after so that stable sort
	// + dedup (keep last) still favours memory for equal timestamps.
	var result []Sample
	seriesFoundInBlock := false
	for _, r := range readers {
		meta := r.Meta()
		if meta.MaxTime < startMs || meta.MinTime > endMs {
			continue
		}
		for _, se := range r.Series() {
			if SeriesID(se.ID) != id {
				continue
			}
			seriesFoundInBlock = true
			for _, ref := range se.Chunks {
				c, err := r.ReadChunk(ref)
				if err != nil {
					return nil, fmt.Errorf("blockstore: read chunk: %w", err)
				}
				it := c.Iterator()
				for it.Next() {
					ts, val := it.At()
					if ts >= startMs && ts <= endMs {
						result = append(result, Sample{SeriesID: id, TimestampMs: ts, Value: val})
					}
				}
			}
		}
	}

	result = append(result, memResult...)

	// Ensure non-nil for known series with no in-range samples.
	if result == nil && (seriesFoundInBlock || memResult != nil) {
		result = []Sample{}
	}

	if len(result) <= 1 {
		return result, nil
	}

	sort.SliceStable(result, func(i, j int) bool {
		return result[i].TimestampMs < result[j].TimestampMs
	})

	// Dedup: for equal timestamps keep the last occurrence (memory wins).
	deduped := result[:1]
	for i := 1; i < len(result); i++ {
		if result[i].TimestampMs == deduped[len(deduped)-1].TimestampMs {
			deduped[len(deduped)-1] = result[i]
		} else {
			deduped = append(deduped, result[i])
		}
	}
	return deduped, nil
}

// FlushBlock writes all sealed chunks from memory into a new immutable block.
// Returns (false, nil) immediately if no sealed chunks exist. Returns (true, nil)
// on success. On write failure the memory store is unchanged. Concurrent calls
// are serialized.
func (bs *BlockStore) FlushBlock() (bool, error) {
	bs.flushMu.Lock()
	defer bs.flushMu.Unlock()

	snapshot := bs.mem.SealedChunksSnapshot()
	if len(snapshot) == 0 {
		return false, nil
	}

	w, err := block.NewWriter(bs.blockDir, bs.tmpDir)
	if err != nil {
		return false, fmt.Errorf("blockstore: new writer: %w", err)
	}

	for _, sc := range snapshot {
		pairs := labelsToPairs(sc.Labels)
		if err := w.AddSeries(uint64(sc.ID), pairs, sc.Chunks); err != nil {
			_ = w.Abort()
			return false, fmt.Errorf("blockstore: add series: %w", err)
		}
	}

	meta, err := w.Commit()
	if err != nil {
		_ = w.Abort()
		return false, fmt.Errorf("blockstore: commit block: %w", err)
	}

	// Re-open the committed block as a reader using the block ID from Commit.
	newReader, err := block.OpenReader(filepath.Join(bs.blockDir, meta.BlockID))
	if err != nil {
		return false, fmt.Errorf("blockstore: open new block %s: %w", meta.BlockID, err)
	}

	// Register the new reader before discarding sealed chunks from memory.
	// This ensures no query window where the data is visible in neither source.
	// Queries that snapshot the block list before this point still see the sealed
	// chunks in memory; queries that snapshot after may briefly see both, which
	// is handled correctly by the existing dedup pass (memory wins on ties).
	bs.mu.Lock()
	bs.blocks = append(bs.blocks, newReader)
	bs.mu.Unlock()
	bs.mem.DiscardSealedChunks(snapshot)

	return true, nil
}

// Close releases file descriptors held by all loaded block readers.
func (bs *BlockStore) Close() error {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	var firstErr error
	for _, r := range bs.blocks {
		if err := r.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// MemStore returns the underlying MemoryStore. Used in tests to inspect
// chunk counts after flush.
func (bs *BlockStore) MemStore() *MemoryStore { return bs.mem }

// ReadCheckpoint reads the WAL segment number from the checkpoint file at
// dataDir/metrics/checkpoint. Returns 0 if the file does not exist.
func ReadCheckpoint(dataDir string) int {
	path := filepath.Join(dataDir, "metrics", "checkpoint")
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0
	}
	return n
}

// deleteWALSegmentsUpTo removes .wal files in walDir with numeric index ≤ maxIdx.
func deleteWALSegmentsUpTo(walDir string, maxIdx int) error {
	entries, err := os.ReadDir(walDir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("blockstore: readdir %s: %w", walDir, err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".wal") {
			continue
		}
		base := strings.TrimSuffix(e.Name(), ".wal")
		idx, err := strconv.Atoi(base)
		if err != nil {
			continue
		}
		if idx <= maxIdx {
			if err := os.Remove(filepath.Join(walDir, e.Name())); err != nil {
				return fmt.Errorf("blockstore: remove WAL segment %s: %w", e.Name(), err)
			}
		}
	}
	return nil
}

// --- helpers ---

func blockPairsToLabels(pairs []block.LabelPair) (Labels, error) {
	m := make(map[string]string, len(pairs))
	for _, p := range pairs {
		m[p.Name] = p.Value
	}
	return NewLabels(m)
}

func labelsToPairs(l Labels) []block.LabelPair {
	m := l.Map()
	pairs := make([]block.LabelPair, 0, len(m))
	for name, val := range m {
		pairs = append(pairs, block.LabelPair{Name: name, Value: val})
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].Name < pairs[j].Name })
	return pairs
}

// Ensure BlockStore satisfies the Store interface at compile time.
var _ Store = (*BlockStore)(nil)
