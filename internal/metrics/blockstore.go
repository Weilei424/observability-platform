package metrics

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

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

	// Startup GC: a crash may have left a compacted block beside its sources.
	// Delete any block listed as a source of a surviving block before opening
	// readers (queries would dedup them harmlessly, but this keeps the set minimal
	// and prevents recompaction thrash).
	superseded := make(map[string]struct{})
	for _, e := range blockEntries {
		if !e.IsDir() {
			continue
		}
		m, err := block.ReadMeta(filepath.Join(blockDir, e.Name()))
		if err != nil {
			continue // unreadable meta is surfaced when the reader is opened below
		}
		for _, src := range m.Sources {
			superseded[src] = struct{}{}
		}
	}
	for id := range superseded {
		src := filepath.Join(blockDir, id)
		dst := filepath.Join(tmpDir, id)
		_ = os.RemoveAll(dst)
		if err := os.Rename(src, dst); err == nil {
			_ = os.RemoveAll(dst)
		}
	}

	readers := make([]*block.Reader, 0, len(blockEntries))
	for _, e := range blockEntries {
		if !e.IsDir() {
			continue
		}
		if _, gone := superseded[e.Name()]; gone {
			continue // GC'd above
		}
		r, err := block.OpenReader(filepath.Join(blockDir, e.Name()))
		if err != nil {
			closeReaders(readers)
			return nil, fmt.Errorf("blockstore: open block %s: %w", e.Name(), err)
		}
		if err := validateBlockSeries(r); err != nil {
			r.Close()
			closeReaders(readers)
			return nil, fmt.Errorf("blockstore: block %s: %w", e.Name(), err)
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
// series fingerprint (memory wins). A postings read failure in any block is
// propagated rather than skipped, so a query never silently returns results
// missing a block's data.
func (bs *BlockStore) SelectSeries(sel Selector) ([]MatchedSeries, error) {
	bs.mu.RLock()
	defer bs.mu.RUnlock()

	result, _ := bs.mem.SelectSeries(sel) // MemoryStore never errors
	seen := make(map[SeriesID]struct{}, len(result))
	for _, ms := range result {
		seen[ms.Labels.Fingerprint()] = struct{}{}
	}

	matchers := selectorToIndexMatchers(sel)

	for _, r := range bs.blocks {
		ids, err := r.Postings(matchers)
		if err != nil {
			return nil, fmt.Errorf("blockstore: select series in block: %w", err)
		}
		if len(ids) == 0 {
			continue
		}
		for _, id := range ids {
			se, ok := r.SeriesByID(id)
			if !ok {
				continue
			}
			labels, err := blockPairsToLabels(se.Labels)
			if err != nil {
				return nil, fmt.Errorf("blockstore: decode series %d labels: %w", se.ID, err)
			}
			fp := labels.Fingerprint()
			if _, already := seen[fp]; already {
				continue
			}
			seen[fp] = struct{}{}
			result = append(result, MatchedSeries{id: SeriesID(se.ID), Labels: labels})
		}
	}
	return result, nil
}

// LabelNames returns the sorted, deduplicated label names across memory and all
// loaded blocks.
func (bs *BlockStore) LabelNames() []string {
	bs.mu.RLock()
	defer bs.mu.RUnlock()
	set := make(map[string]struct{})
	for _, n := range bs.mem.LabelNames() {
		set[n] = struct{}{}
	}
	for _, r := range bs.blocks {
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
	bs.mu.RLock()
	defer bs.mu.RUnlock()
	set := make(map[string]struct{})
	for _, v := range bs.mem.LabelValues(name) {
		set[v] = struct{}{}
	}
	for _, r := range bs.blocks {
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
	bs.mu.RLock()
	defer bs.mu.RUnlock()
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
	memSeries, _ := bs.mem.SelectSeries(Selector{}) // MemoryStore never errors
	for _, ms := range memSeries {
		add(ms.Labels, ms.Labels.Fingerprint())
	}
	for _, r := range bs.blocks {
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

// BlockInfos returns a snapshot of all loaded blocks for compaction planning and
// storage metrics, ordered by MinTime.
func (bs *BlockStore) BlockInfos() []block.BlockInfo {
	bs.mu.RLock()
	readers := make([]*block.Reader, len(bs.blocks))
	copy(readers, bs.blocks)
	bs.mu.RUnlock()

	infos := make([]block.BlockInfo, 0, len(readers))
	for _, r := range readers {
		m := r.Meta()
		infos = append(infos, block.BlockInfo{
			ID:        m.BlockID,
			Level:     m.EffectiveLevel(),
			MinTime:   m.MinTime,
			MaxTime:   m.MaxTime,
			SizeBytes: dirSizeBytes(filepath.Join(bs.blockDir, m.BlockID)),
		})
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].MinTime < infos[j].MinTime })
	return infos
}

// StorageStats returns the number of loaded blocks and their total on-disk size.
func (bs *BlockStore) StorageStats() (blocks int, bytes int64) {
	for _, info := range bs.BlockInfos() {
		blocks++
		bytes += info.SizeBytes
	}
	return blocks, bytes
}

// dirSizeBytes sums the sizes of the regular files directly inside dir. Best
// effort: unreadable entries are skipped (size metrics must never fail a query).
func dirSizeBytes(dir string) int64 {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	var total int64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if fi, err := e.Info(); err == nil {
			total += fi.Size()
		}
	}
	return total
}

// QueryInstant returns the latest sample with timestamp ≤ tMs for the given
// series, searching both memory and all loaded blocks. Memory wins for equal
// timestamps. Returns an error if any block chunk cannot be read.
func (bs *BlockStore) QueryInstant(id SeriesID, tMs int64) (Sample, bool, error) {
	bs.mu.RLock()
	defer bs.mu.RUnlock()

	best, found, err := bs.mem.QueryInstant(id, tMs)
	if err != nil {
		return Sample{}, false, err
	}

	for _, r := range bs.blocks {
		if r.Meta().MinTime > tMs {
			continue
		}
		se, ok := r.SeriesByID(uint64(id))
		if !ok {
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
				if ts <= tMs && (!found || ts > best.TimestampMs) {
					best = Sample{SeriesID: id, TimestampMs: ts, Value: val}
					found = true
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
	defer bs.mu.RUnlock()

	// The read lock is held for the whole query so compaction/retention (which
	// take the write lock to close+delete readers) cannot reclaim a block while
	// it is being read.
	memResult, err := bs.mem.QueryRange(id, startMs, endMs)
	if err != nil {
		return nil, err
	}

	var result []Sample
	seriesFoundInBlock := false
	for _, r := range bs.blocks {
		meta := r.Meta()
		if meta.MaxTime < startMs || meta.MinTime > endMs {
			continue
		}
		se, ok := r.SeriesByID(uint64(id))
		if !ok {
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

// CompactOnce applies plan to the current block set. For each returned group of
// ≥2 still-present block IDs it merges those blocks into one new block, registers
// the new reader, and safe-deletes the sources. Returns the number of groups
// compacted. Taking the write lock for the swap drains in-flight queries before
// any source reader is closed. Concurrent calls are serialized via flushMu so
// CompactOnce and FlushBlock never race on the block set.
func (bs *BlockStore) CompactOnce(plan func([]block.BlockInfo) [][]string) (int, error) {
	bs.flushMu.Lock()
	defer bs.flushMu.Unlock()

	groups := plan(bs.BlockInfos())
	compacted := 0
	for _, group := range groups {
		if len(group) < 2 {
			continue
		}

		bs.mu.RLock()
		sources := make([]*block.Reader, 0, len(group))
		ok := true
		for _, id := range group {
			r := bs.readerByID(id)
			if r == nil {
				ok = false
				break
			}
			sources = append(sources, r)
		}
		bs.mu.RUnlock()
		if !ok || len(sources) < 2 {
			continue
		}

		meta, err := block.Compact(bs.blockDir, bs.tmpDir, sources)
		if err != nil {
			return compacted, fmt.Errorf("blockstore: compact: %w", err)
		}
		newReader, err := block.OpenReader(filepath.Join(bs.blockDir, meta.BlockID))
		if err != nil {
			_ = bs.safeDeleteBlock(meta.BlockID)
			return compacted, fmt.Errorf("blockstore: open compacted block %s: %w", meta.BlockID, err)
		}
		if err := validateBlockSeries(newReader); err != nil {
			_ = newReader.Close()
			_ = bs.safeDeleteBlock(meta.BlockID)
			return compacted, fmt.Errorf("blockstore: compacted block %s: %w", meta.BlockID, err)
		}

		inGroup := make(map[string]struct{}, len(group))
		for _, id := range group {
			inGroup[id] = struct{}{}
		}
		var removed []*block.Reader
		bs.mu.Lock()
		kept := make([]*block.Reader, 0, len(bs.blocks))
		for _, r := range bs.blocks {
			if _, drop := inGroup[r.Meta().BlockID]; drop {
				removed = append(removed, r)
			} else {
				kept = append(kept, r)
			}
		}
		kept = append(kept, newReader)
		bs.blocks = kept
		bs.mu.Unlock()

		for _, r := range removed {
			id := r.Meta().BlockID
			_ = r.Close()
			if err := bs.safeDeleteBlock(id); err != nil {
				return compacted, fmt.Errorf("blockstore: delete source block %s: %w", id, err)
			}
		}
		compacted++
	}
	return compacted, nil
}

// ApplyRetention removes every block whose MaxTime is strictly older than
// now-retention, closing its reader and safe-deleting its directory. A
// non-positive retention disables retention (no-op). Returns the number deleted.
// The write lock drains in-flight queries before any reader is closed.
func (bs *BlockStore) ApplyRetention(now time.Time, retention time.Duration) (int, error) {
	if retention <= 0 {
		return 0, nil
	}
	cutoff := now.UnixMilli() - retention.Milliseconds()

	bs.flushMu.Lock()
	defer bs.flushMu.Unlock()

	bs.mu.Lock()
	kept := make([]*block.Reader, 0, len(bs.blocks))
	var expired []*block.Reader
	for _, r := range bs.blocks {
		if r.Meta().MaxTime < cutoff {
			expired = append(expired, r)
		} else {
			kept = append(kept, r)
		}
	}
	if len(expired) > 0 {
		bs.blocks = kept
	}
	bs.mu.Unlock()

	for _, r := range expired {
		id := r.Meta().BlockID
		_ = r.Close()
		if err := bs.safeDeleteBlock(id); err != nil {
			return len(expired), err
		}
	}
	return len(expired), nil
}

// readerByID returns the loaded reader for id, or nil. Caller holds bs.mu.
func (bs *BlockStore) readerByID(id string) *block.Reader {
	for _, r := range bs.blocks {
		if r.Meta().BlockID == id {
			return r
		}
	}
	return nil
}

// safeDeleteBlock removes a block directory crash-safely: rename into tmpDir
// (atomic, same filesystem) then RemoveAll. A crash between the two leaves only a
// tmp entry, which NewBlockStore wipes on startup. A missing source is not an error.
func (bs *BlockStore) safeDeleteBlock(id string) error {
	src := filepath.Join(bs.blockDir, id)
	dst := filepath.Join(bs.tmpDir, id)
	_ = os.RemoveAll(dst)
	if err := os.Rename(src, dst); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("blockstore: rename block %s to tmp: %w", id, err)
	}
	if err := os.RemoveAll(dst); err != nil {
		return fmt.Errorf("blockstore: remove tmp block %s: %w", id, err)
	}
	return nil
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

// SealedChunkCount reports the number of sealed chunks held in memory (not yet
// flushed to a block).
func (bs *BlockStore) SealedChunkCount() int {
	return bs.mem.SealedChunkCount()
}

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

// closeReaders closes every reader, used to release already-opened blocks when
// NewBlockStore aborts partway through loading.
func closeReaders(readers []*block.Reader) {
	for _, r := range readers {
		_ = r.Close()
	}
}

// validateBlockSeries checks that every series in a freshly opened block has a
// well-formed label set whose fingerprint equals the stored series ID. The
// fingerprint check lives here (not in block.OpenReader) because fingerprinting
// is a metrics-layer concern; the block package must not depend on it. Catching
// the mismatch at load means the per-series decode in SelectSeries cannot
// silently drop a corrupt or malformed series at query time.
func validateBlockSeries(r *block.Reader) error {
	for _, se := range r.Series() {
		labels, err := blockPairsToLabels(se.Labels)
		if err != nil {
			return fmt.Errorf("series %d: %w", se.ID, err)
		}
		if uint64(labels.Fingerprint()) != se.ID {
			return fmt.Errorf("series %d label set fingerprints to %d", se.ID, uint64(labels.Fingerprint()))
		}
	}
	return nil
}

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
