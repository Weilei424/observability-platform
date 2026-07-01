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

	// Load existing blocks.
	blockEntries, err := os.ReadDir(blockDir)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("blockstore: read block dir: %w", err)
	}

	// Open and validate every block BEFORE reclaiming any compaction sources. A
	// compacted block lists its sources in meta.json, but those sources are the
	// only recoverable copy of the data if the compacted block itself is corrupt.
	// So validate survivors first and trust a Sources list only when the block
	// that declares it actually opened and validated.
	opened := make(map[string]*block.Reader)
	failed := make(map[string]error)
	for _, e := range blockEntries {
		if !e.IsDir() {
			continue
		}
		r, err := block.OpenReader(filepath.Join(blockDir, e.Name()))
		if err != nil {
			failed[e.Name()] = err
			continue
		}
		if err := validateBlockSeries(r); err != nil {
			_ = r.Close()
			failed[e.Name()] = err
			continue
		}
		opened[e.Name()] = r
	}

	// Deep-validate every opened block's chunks and reconstruct its true maximum
	// generation from them. Index and postings can be intact while the chunks file
	// is truncated or corrupt, and chunks are read lazily at query time. Doing this
	// for every block (a) lets a corrupt survivor be demoted before its Sources are
	// trusted, and (b) makes the generation floor depend on the generations actually
	// stored, never on a possibly-corrupt Meta.MaxGen. A block whose chunks cannot be
	// decoded is demoted to failed.
	maxGenByBlock := make(map[string]int64, len(opened))
	var corrupt []string
	for name, r := range opened {
		st, err := validateBlockChunks(r)
		if err != nil {
			_ = r.Close()
			failed[name] = err
			corrupt = append(corrupt, name)
			continue
		}
		// Override meta's derived time/count fields with the chunk-decoded truth so
		// corrupt or tampered values can no longer silently drop data (queries skip
		// by MinTime) or trigger wrongful retention deletion (by MaxTime). This also
		// makes the time bounds trustworthy for the supersede check below.
		if err := reconcileBlockMeta(r, st); err != nil {
			_ = r.Close()
			failed[name] = err
			corrupt = append(corrupt, name)
			continue
		}
		// A survivor authorizes deleting its sources, so require its independently
		// persisted MaxGen to agree with the generations reconstructed from its
		// chunks. Syntactically valid generation corruption would otherwise pass and
		// destroy recoverable source blocks. (Ordinary flush blocks rely on the
		// reconstructed value only; a stale MaxGen there is harmless.)
		if len(r.Meta().Sources) > 0 && st.maxGen != r.Meta().MaxGen {
			_ = r.Close()
			failed[name] = fmt.Errorf("survivor MaxGen %d disagrees with stored generations (max %d)", r.Meta().MaxGen, st.maxGen)
			corrupt = append(corrupt, name)
			continue
		}
		maxGenByBlock[name] = st.maxGen
	}
	for _, name := range corrupt {
		delete(opened, name)
	}

	// Superseded sources come only from survivors that opened AND fully validated.
	// supersededBy records, per source ID, the survivors that list it, so a present
	// source can be checked for genuine containment before it is deleted.
	superseded := make(map[string]struct{})
	supersededBy := make(map[string][]*block.Reader)
	for _, r := range opened {
		for _, src := range r.Meta().Sources {
			superseded[src] = struct{}{}
			supersededBy[src] = append(supersededBy[src], r)
		}
	}

	// A block that failed to open/validate and is NOT replaced by a validated
	// survivor is unrecoverable corruption: fail fast WITHOUT reclaiming anything,
	// so still-needed data is never deleted ahead of a failed startup.
	for name, ferr := range failed {
		if _, replaced := superseded[name]; replaced {
			continue // corrupt source that a validated survivor supersedes; reclaimed below
		}
		for _, r := range opened {
			_ = r.Close()
		}
		return nil, fmt.Errorf("blockstore: block %s: %w", name, ferr)
	}

	// Reclaim superseded sources now that their survivor is proven good. This
	// converges a crash that left a compacted block beside its sources.
	//
	// A source that is still present and valid on disk is only deleted when a
	// survivor provably contains its samples at ≥ generation (survivorSupersedesSource);
	// otherwise the "source" is an unrelated/self/older block wrongly named in a
	// tampered or corrupt Sources list, and deleting it would destroy live data or
	// resurrect a stale value — so we keep it as a live block. A source that failed to
	// open (corrupt) cannot be inspected; it is reclaimed on the survivor's say-so as
	// before (deleting already-unreadable data loses nothing and preserves
	// crash-recovery convergence).
	gensCache := make(map[string]map[uint64]map[int64]int64, len(opened))
	survivorGens := func(sv *block.Reader) (map[uint64]map[int64]int64, error) {
		id := sv.Meta().BlockID
		if g, ok := gensCache[id]; ok {
			return g, nil
		}
		g, err := decodeBlockGens(sv)
		if err != nil {
			return nil, err
		}
		gensCache[id] = g
		return g, nil
	}
	for name := range superseded {
		if r, ok := opened[name]; ok {
			verified := false
			for _, sv := range supersededBy[name] {
				gens, err := survivorGens(sv)
				if err != nil {
					continue // cannot verify against this survivor; try the next
				}
				ok, err := survivorSupersedesSource(gens, sv.Meta().BlockID, r)
				if err == nil && ok {
					verified = true
					break
				}
			}
			if !verified {
				continue // keep the unverified block live; do not delete it
			}
			_ = r.Close()
			delete(opened, name)
		}
		src := filepath.Join(blockDir, name)
		dst := filepath.Join(tmpDir, name)
		_ = os.RemoveAll(dst)
		if err := os.Rename(src, dst); err == nil {
			_ = os.RemoveAll(dst)
		}
	}

	readers := make([]*block.Reader, 0, len(opened))
	for _, r := range opened {
		readers = append(readers, r)
	}
	sort.Slice(readers, func(i, j int) bool { return readers[i].Meta().MinTime < readers[j].Meta().MinTime })

	// Seed the in-memory generation counter past the highest generation actually
	// stored in any kept block (reconstructed above from chunks, not trusted from
	// meta), so replayed and newly appended samples always outrank stored block data
	// for last-write-wins.
	var maxGen int64
	for _, r := range readers {
		if g := maxGenByBlock[r.Meta().BlockID]; g > maxGen {
			maxGen = g
		}
	}

	bs := &BlockStore{
		mem:      NewMemoryStore(),
		blocks:   readers,
		blockDir: blockDir,
		tmpDir:   tmpDir,
	}
	bs.mem.EnsureGenFloor(maxGen + 1)
	return bs, nil
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

// GenerationExhausted reports whether the in-memory write-generation counter is
// exhausted (see MemoryStore.GenerationExhausted).
func (bs *BlockStore) GenerationExhausted() bool {
	return bs.mem.GenerationExhausted()
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
// series, searching both memory and all loaded blocks. For an equal timestamp the
// highest write generation wins (memory outranks blocks because its generations
// are seeded past every block's). Returns an error if any block chunk cannot be read.
func (bs *BlockStore) QueryInstant(id SeriesID, tMs int64) (Sample, bool, error) {
	bs.mu.RLock()
	defer bs.mu.RUnlock()

	best, found, err := bs.mem.QueryInstant(id, tMs)
	if err != nil {
		return Sample{}, false, err
	}

	// Latest timestamp wins; an equal timestamp is broken by highest generation.
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
				if ts > tMs {
					continue
				}
				gen := it.Gen()
				if !found || ts > best.TimestampMs || (ts == best.TimestampMs && gen > best.Gen) {
					best = Sample{SeriesID: id, TimestampMs: ts, Value: val, Gen: gen}
					found = true
				}
			}
		}
	}
	return best, found, nil
}

// QueryRange returns all samples for series id with startMs ≤ ts ≤ endMs,
// merging memory and block results. Results are sorted by timestamp; duplicate
// timestamps are deduplicated keeping the highest write generation (last-write-wins).
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
					result = append(result, Sample{SeriesID: id, TimestampMs: ts, Value: val, Gen: it.Gen()})
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

	// Dedup: for equal timestamps keep the highest generation (last-write-wins).
	deduped := result[:1]
	for i := 1; i < len(result); i++ {
		last := &deduped[len(deduped)-1]
		if result[i].TimestampMs == last.TimestampMs {
			if result[i].Gen > last.Gen {
				*last = result[i]
			}
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
	// is handled correctly by the existing dedup pass (highest generation wins).
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
		// Deep-validate the merged output by decoding every chunk before any source
		// is deleted: validateBlockSeries only checks label fingerprints, so a
		// truncated/corrupt chunks file would otherwise pass and we would delete the
		// sources that still hold the real data.
		st, err := validateBlockChunks(newReader)
		if err != nil {
			_ = newReader.Close()
			_ = bs.safeDeleteBlock(meta.BlockID)
			return compacted, fmt.Errorf("blockstore: compacted block %s chunks: %w", meta.BlockID, err)
		}
		// Apply the same integrity checks startup uses before a block authorizes
		// deleting sources: its persisted MaxGen must equal the reconstructed one, and
		// its meta must be reconciled to the chunks. Otherwise a syntactically valid
		// metadata corruption could be installed here, the sources deleted, and the
		// only remaining block then rejected by the next restart.
		if st.maxGen != newReader.Meta().MaxGen {
			_ = newReader.Close()
			_ = bs.safeDeleteBlock(meta.BlockID)
			return compacted, fmt.Errorf("blockstore: compacted block %s MaxGen %d disagrees with stored generations (max %d)", meta.BlockID, newReader.Meta().MaxGen, st.maxGen)
		}
		if err := reconcileBlockMeta(newReader, st); err != nil {
			_ = newReader.Close()
			_ = bs.safeDeleteBlock(meta.BlockID)
			return compacted, fmt.Errorf("blockstore: compacted block %s reconcile meta: %w", meta.BlockID, err)
		}
		// Guarantee the output supersedes each source at ≥ generation for every
		// sample before reclaiming them — defense in depth against a compaction bug
		// silently dropping a source's data or a resolved correction.
		newGens, err := decodeBlockGens(newReader)
		if err != nil {
			_ = newReader.Close()
			_ = bs.safeDeleteBlock(meta.BlockID)
			return compacted, fmt.Errorf("blockstore: compacted block %s decode: %w", meta.BlockID, err)
		}
		for _, src := range sources {
			ok, err := survivorSupersedesSource(newGens, meta.BlockID, src)
			if err != nil {
				_ = newReader.Close()
				_ = bs.safeDeleteBlock(meta.BlockID)
				return compacted, fmt.Errorf("blockstore: compacted block %s verify source %s: %w", meta.BlockID, src.Meta().BlockID, err)
			}
			if !ok {
				_ = newReader.Close()
				_ = bs.safeDeleteBlock(meta.BlockID)
				return compacted, fmt.Errorf("blockstore: compacted block %s does not supersede source %s", meta.BlockID, src.Meta().BlockID)
			}
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
// now-retention. A non-positive retention disables retention (no-op). Returns the
// number of blocks actually reclaimed and the first deletion error, if any.
//
// Deletion is crash-safe and readable-on-failure: each expired block is reclaimed
// by renaming its directory into tmpDir under the write lock (which drains
// in-flight queries first, so no query can lazily reopen the moved files). The
// rename is the step that ends readability, so a block whose rename fails stays in
// the live set and fully readable, and is retried on the next maintenance pass.
// The slower RemoveAll and reader Close run afterwards outside the lock; a crash
// or RemoveAll failure leaves only a tmp entry, which NewBlockStore wipes on
// startup, so the block still counts as deleted.
func (bs *BlockStore) ApplyRetention(now time.Time, retention time.Duration) (int, error) {
	if retention <= 0 {
		return 0, nil
	}
	cutoff := now.UnixMilli() - retention.Milliseconds()

	bs.flushMu.Lock()
	defer bs.flushMu.Unlock()

	// Retry physical cleanup of any tmp leftovers a prior pass could not remove.
	sweepTmpDir(bs.tmpDir)

	var reclaimed []*block.Reader
	var firstErr error
	bs.mu.Lock()
	kept := make([]*block.Reader, 0, len(bs.blocks))
	for _, r := range bs.blocks {
		// Once a deletion fails, keep every remaining block (expired or not) so the
		// failed block stays readable and the rest retry next pass.
		if firstErr != nil || r.Meta().MaxTime >= cutoff {
			kept = append(kept, r)
			continue
		}
		id := r.Meta().BlockID
		dst := filepath.Join(bs.tmpDir, id)
		_ = os.RemoveAll(dst) // clear a stale tmp entry so the rename target is free
		if err := os.Rename(filepath.Join(bs.blockDir, id), dst); err != nil && !os.IsNotExist(err) {
			firstErr = fmt.Errorf("blockstore: rename block %s to tmp: %w", id, err)
			kept = append(kept, r) // rename failed: dir intact, block stays readable
			continue
		}
		reclaimed = append(reclaimed, r)
	}
	bs.blocks = kept
	bs.mu.Unlock()

	// Finish reclaimed blocks outside the lock. A physical-cleanup failure is
	// surfaced (not swallowed): the block is already gone from the live set and
	// counts as reclaimed, and the next pass's sweep plus startup GC retry removal.
	var cleanupErr error
	for _, r := range reclaimed {
		_ = r.Close()
		if err := os.RemoveAll(filepath.Join(bs.tmpDir, r.Meta().BlockID)); err != nil && cleanupErr == nil {
			cleanupErr = fmt.Errorf("blockstore: remove reclaimed block %s from tmp: %w", r.Meta().BlockID, err)
		}
	}
	if firstErr != nil {
		return len(reclaimed), firstErr
	}
	return len(reclaimed), cleanupErr
}

// sweepTmpDir best-effort removes leftover subdirectories under tmpDir: orphaned
// flush/compaction work dirs and reclaimed blocks whose final RemoveAll failed on
// an earlier pass. Safe to call while holding flushMu, when no writer is using
// tmpDir.
func sweepTmpDir(tmpDir string) {
	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() {
			_ = os.RemoveAll(filepath.Join(tmpDir, e.Name()))
		}
	}
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

// blockChunkStats holds the ground-truth figures reconstructed by decoding every
// chunk in a block: they are what meta.json's derived fields are reconciled
// against (see reconcileBlockMeta).
type blockChunkStats struct {
	maxGen     int64
	minTs      int64
	maxTs      int64
	numSamples int
	numSeries  int
	hasSamples bool
}

// validateBlockChunks decodes every chunk in the block and reconstructs its true
// stats (max generation, time bounds, sample/series counts). OpenReader and
// validateBlockSeries only check the index/postings; chunks are read lazily at
// query time. Run for every block at load so (a) a corrupt survivor is demoted
// before its Sources are trusted (and deleted), (b) the generation floor is
// reconstructed from real generations rather than a possibly-corrupt Meta.MaxGen,
// and (c) meta's time/count fields can be reconciled against reality. ReadChunk
// validates each payload via chunk.FromBytes.
func validateBlockChunks(r *block.Reader) (blockChunkStats, error) {
	st := blockChunkStats{numSeries: len(r.Series())}
	for _, se := range r.Series() {
		for _, ref := range se.Chunks {
			c, err := r.ReadChunk(ref)
			if err != nil {
				return blockChunkStats{}, fmt.Errorf("series %d chunk: %w", se.ID, err)
			}
			if c.NumSamples() == 0 {
				continue
			}
			if g := c.MaxGen(); g > st.maxGen {
				st.maxGen = g
			}
			if !st.hasSamples {
				st.minTs, st.maxTs, st.hasSamples = c.MinTs(), c.MaxTs(), true
			} else {
				if c.MinTs() < st.minTs {
					st.minTs = c.MinTs()
				}
				if c.MaxTs() > st.maxTs {
					st.maxTs = c.MaxTs()
				}
			}
			st.numSamples += c.NumSamples()
		}
	}
	return st, nil
}

// reconcileBlockMeta brings a block's in-memory metadata into agreement with the
// stats decoded from its chunks. meta.json's MinTime/MaxTime/NumSamples/NumSeries
// are written from the chunks at flush time (block.Writer), so for an intact block
// they already match and this is a no-op; any disagreement means the metadata is
// corrupt or tampered. Because queries skip blocks by MinTime, retention deletes by
// MaxTime, and source-reclamation trusts these fields, a stale value would silently
// drop data or delete a block — so we override with the chunk-derived truth
// (the chunks being authoritative) rather than trust the file.
func reconcileBlockMeta(r *block.Reader, st blockChunkStats) error {
	want := r.Meta()
	if st.hasSamples {
		want.MinTime, want.MaxTime = st.minTs, st.maxTs
	} else {
		want.MinTime, want.MaxTime = 0, 0
	}
	want.NumSamples = st.numSamples
	want.NumSeries = st.numSeries
	return r.SetReconciledMeta(want)
}

// decodeBlockGens decodes every chunk in r into per-series maps of
// timestamp -> highest write generation stored at that timestamp. This is the
// ground truth used to prove one block supersedes another (survivorSupersedesSource):
// presence and generation, not merely metadata, decide reclamation.
func decodeBlockGens(r *block.Reader) (map[uint64]map[int64]int64, error) {
	out := make(map[uint64]map[int64]int64, len(r.Series()))
	for _, se := range r.Series() {
		m := make(map[int64]int64)
		for _, ref := range se.Chunks {
			c, err := r.ReadChunk(ref)
			if err != nil {
				return nil, fmt.Errorf("series %d chunk: %w", se.ID, err)
			}
			it := c.Iterator()
			for it.Next() {
				ts, _ := it.At()
				g := it.Gen()
				if cur, ok := m[ts]; !ok || g > cur {
					m[ts] = g
				}
			}
		}
		out[se.ID] = m
	}
	return out, nil
}

// survivorSupersedesSource reports whether survivor provably contains every sample
// in source at an equal-or-higher write generation — the only condition under which
// deleting source cannot lose data or resurrect a stale value under last-write-wins.
// survivorGens must be decodeBlockGens(survivor).
//
// This is deliberately stricter than metadata/time/series-ID heuristics, which are
// forgeable from mutable meta.json:
//   - a self-reference (survivor lists its own ID) is rejected, so a block cannot
//     authorize deleting itself;
//   - an older block cannot supersede a newer same-series correction, because the
//     correction's higher generation is absent from the older block.
//
// A genuine compaction survivor is built from its sources with last-write-wins, so
// it always holds every source sample at ≥ its generation and this passes.
func survivorSupersedesSource(survivorGens map[uint64]map[int64]int64, survivorID string, source *block.Reader) (bool, error) {
	if survivorID == source.Meta().BlockID {
		return false, nil // a block can never be its own source
	}
	for _, se := range source.Series() {
		sg, ok := survivorGens[se.ID]
		if !ok {
			return false, nil // survivor lacks this series entirely
		}
		for _, ref := range se.Chunks {
			c, err := source.ReadChunk(ref)
			if err != nil {
				return false, fmt.Errorf("source series %d chunk: %w", se.ID, err)
			}
			it := c.Iterator()
			for it.Next() {
				ts, _ := it.At()
				if g, ok := sg[ts]; !ok || g < it.Gen() {
					return false, nil // sample missing, or survivor's is older
				}
			}
		}
	}
	return true, nil
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
