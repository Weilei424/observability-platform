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

// SelectSeries returns all series matching sel from memory and all loaded blocks.
// Results are deduplicated by series fingerprint.
func (bs *BlockStore) SelectSeries(sel Selector) []MatchedSeries {
	result := bs.mem.SelectSeries(sel)
	seen := make(map[SeriesID]struct{}, len(result))
	for _, ms := range result {
		seen[ms.Labels.Fingerprint()] = struct{}{}
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
			fp := labels.Fingerprint()
			if _, already := seen[fp]; already {
				continue
			}
			if !labelsMatchSelector(labels, sel) {
				continue
			}
			seen[fp] = struct{}{}
			result = append(result, MatchedSeries{id: SeriesID(se.ID), Labels: labels})
		}
	}
	return result
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
	bs.mu.RLock()
	readers := bs.blocks
	bs.mu.RUnlock()

	// Collect block samples first; memory samples appended after so that
	// stable sort + dedup (keep last) favours memory for equal timestamps.
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

	// Append memory samples (already sorted and deduped by MemoryStore).
	memResult, err := bs.mem.QueryRange(id, startMs, endMs)
	if err != nil {
		return nil, err
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

	// Remove only the snapshotted sealed chunks from memory and register the new
	// reader. Passing the snapshot ensures chunks that sealed after the snapshot
	// was taken (concurrent Append racing with the block write) are retained.
	bs.mem.DiscardSealedChunks(snapshot)
	bs.mu.Lock()
	bs.blocks = append(bs.blocks, newReader)
	bs.mu.Unlock()

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

func labelsMatchSelector(l Labels, sel Selector) bool {
	if sel.MetricName != "" {
		name, _ := l.Get("__name__")
		if name != sel.MetricName {
			return false
		}
	}
	for _, m := range sel.Matchers {
		val, ok := l.Get(m.Name)
		if !ok || val != m.Value {
			return false
		}
	}
	return true
}

// Ensure BlockStore satisfies the Store interface at compile time.
var _ Store = (*BlockStore)(nil)
