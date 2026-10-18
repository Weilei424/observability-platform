package logs

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/masonwheeler/observability-platform/internal/labels"
	"github.com/masonwheeler/observability-platform/internal/storage/fsutil"
	"github.com/masonwheeler/observability-platform/internal/storage/index"
	"github.com/masonwheeler/observability-platform/internal/storage/logchunk"
	"github.com/masonwheeler/observability-platform/internal/tenant"
)

// ChunkSink persists head streams as chunks and makes them readable before
// returning. Each stream's entries arrive in the order they were appended and
// are written in that order. *ChunkStore is the local sink; internal/rpc's
// client is the remote one the ingester uses.
type ChunkSink interface {
	IngestStreams(ctx context.Context, streams []StreamData) error
}

// ChunkStore owns persisted logs: compressed chunk files and the streams.index
// manifest mapping label pairs to streams and streams to chunk refs. The store
// component runs one on its own; Store pairs one with a Head.
//
// Chunk files are immutable once written. Retention is the only thing that
// deletes them, and reads decode chunks outside the lock, so a ref taken under
// the lock may name a file retention removed since: StreamEntries skips such a
// file rather than failing. Retention only removes data already past its
// window, so the skipped entries are ones the reader was about to lose anyway.
type ChunkStore struct {
	mu        sync.Mutex
	index     *streamIndex
	chunksDir string
	indexPath string
	// tmpDir receives expired chunk files under the lock before they are
	// removed outside it. It is a sibling of chunksDir, never inside it, so
	// rebuildFromScan cannot pick a half-deleted chunk back up.
	tmpDir string

	// testBeforeChunkRead, if non-nil, runs in StreamEntries between the ref
	// lookup and each chunk-file read. Tests use it to race a deletion.
	testBeforeChunkRead func()
}

var _ ChunkSink = (*ChunkStore)(nil)
var _ Reader = (*ChunkStore)(nil)

// OpenChunkStore opens (or creates) a chunk store, loading the persisted index
// and rebuilding it from the chunk headers if the manifest is missing or
// corrupt: the chunks are the source of truth, the manifest a rebuildable cache.
//
// Chunk files a crash left in the retention tmp dir are swept: their refs, if
// the manifest still lists them, read as missing and the next ApplyRetention
// drops them.
func OpenChunkStore(chunksDir, indexDir string) (*ChunkStore, error) {
	tmpDir := chunksTmpDir(chunksDir)
	for _, d := range []string{chunksDir, indexDir, tmpDir} {
		if err := fsutil.MkdirAllSync(d); err != nil {
			return nil, fmt.Errorf("logs: mkdir %s: %w", d, err)
		}
	}
	if err := sweepChunksTmp(tmpDir); err != nil {
		return nil, err
	}
	indexPath := filepath.Join(indexDir, "streams.index")
	idx, err := loadManifest(indexPath)
	if err != nil {
		idx, err = rebuildFromScan(chunksDir)
		if err != nil {
			return nil, err
		}
		if err := idx.writeManifest(indexPath); err != nil {
			return nil, err
		}
	}
	return &ChunkStore{index: idx, chunksDir: chunksDir, indexPath: indexPath, tmpDir: tmpDir}, nil
}

// chunksTmpDir is the retention tmp dir for chunksDir: "chunks-tmp" beside it.
func chunksTmpDir(chunksDir string) string {
	return filepath.Join(filepath.Dir(chunksDir), "chunks-tmp")
}

// retentionCutoffNs is now minus ret in Unix nanoseconds, saturating at the
// int64 bounds: time.UnixNano is undefined outside them (beyond year 2262),
// and a wrapped cutoff would keep or delete chunks arbitrarily.
func retentionCutoffNs(now time.Time, ret time.Duration) int64 {
	const maxSec = math.MaxInt64/int64(time.Second) - 1
	var nowNs int64
	switch sec := now.Unix(); {
	case sec > maxSec:
		nowNs = math.MaxInt64
	case sec < -maxSec:
		nowNs = math.MinInt64
	default:
		nowNs = now.UnixNano()
	}
	if nowNs < math.MinInt64+int64(ret) {
		return math.MinInt64
	}
	return nowNs - int64(ret)
}

// sweepChunksTmp removes everything in tmpDir: chunk files a retention pass
// moved out of chunksDir but did not get to remove.
func sweepChunksTmp(tmpDir string) error {
	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		return fmt.Errorf("logs: read chunks tmp dir: %w", err)
	}
	var first error
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(tmpDir, e.Name())); err != nil && first == nil {
			first = fmt.Errorf("logs: remove %s from chunks tmp dir: %w", e.Name(), err)
		}
	}
	return first
}

// ApplyRetention deletes every chunk file whose newest entry is strictly older
// than now minus its stream's tenant retention; a tenant whose retention is
// non-positive keeps its chunks forever. It returns the number of chunk refs
// dropped and the first error.
//
// Under the lock, each expired file is renamed into tmpDir and its ref dropped
// (a stream left with no ref leaves the index entirely); the manifest is then
// rewritten once. A rename failure keeps that ref, stops the pass, and leaves
// every remaining chunk readable for the next one. A ref whose file is already
// gone (a crash between rename and manifest write) is dropped like any other.
// The files are removed outside the lock; a crash before that leaves them in
// tmpDir for OpenChunkStore to sweep.
func (c *ChunkStore) ApplyRetention(now time.Time, policy tenant.RetentionPolicy) (int, error) {
	var moved []string
	var firstErr error

	c.mu.Lock()
	ids := make([]StreamID, 0, len(c.index.refs))
	for id := range c.index.refs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	dropped := 0
scan:
	for _, id := range ids {
		ret := policy.For(labels.TenantOf(c.index.labels[id]))
		if ret <= 0 {
			continue
		}
		cutoff := retentionCutoffNs(now, ret)
		for _, ref := range append([]ChunkRef(nil), c.index.refs[id]...) {
			if ref.MaxTs >= cutoff {
				continue
			}
			err := os.Rename(filepath.Join(c.chunksDir, ref.Name), filepath.Join(c.tmpDir, ref.Name))
			switch {
			case err == nil:
				moved = append(moved, ref.Name)
			case errors.Is(err, fs.ErrNotExist):
				// Already gone: only the stale ref is left to drop.
			default:
				firstErr = fmt.Errorf("logs: rename chunk %s to tmp: %w", ref.Name, err)
				break scan
			}
			c.index.removeRef(id, ref.Name)
			dropped++
		}
	}
	if dropped > 0 {
		if err := c.index.writeManifest(c.indexPath); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	c.mu.Unlock()

	for _, name := range moved {
		if err := os.Remove(filepath.Join(c.tmpDir, name)); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("logs: remove expired chunk %s from tmp: %w", name, err)
		}
	}
	return dropped, firstErr
}

// IngestStreams writes each stream's entries as one or more chunk files, then
// rewrites the manifest, returning only once both are durable — so a caller
// that then drops those entries from its head leaves no moment in which they
// are readable nowhere.
func (c *ChunkStore) IngestStreams(_ context.Context, streams []StreamData) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, sd := range streams {
		id := StreamIDOf(sd.Labels)
		// Split so no chunk exceeds the decoder's cap: an oversized chunk would be
		// written and its WAL checkpointed, then be rejected on read (data loss).
		for _, ch := range splitIntoChunks(sd.Entries, logchunk.MaxUncompressedBytes) {
			ref, err := writeChunkFile(c.chunksDir, id, sd.Labels, ch)
			if err != nil {
				return err
			}
			c.index.add(id, sd.Labels, ref)
		}
	}
	return c.index.writeManifest(c.indexPath)
}

// MatchingStreamIDs returns the sorted IDs of persisted streams matching all matchers.
func (c *ChunkStore) MatchingStreamIDs(matchers []index.Pair) []StreamID {
	c.mu.Lock()
	ids := c.index.matchingStreamIDs(matchers)
	c.mu.Unlock()
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// StreamLabelSet returns a persisted stream's labels.
func (c *ChunkStore) StreamLabelSet(id StreamID) (StreamLabels, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	l, ok := c.index.labels[id]
	return l, ok
}

// StreamEntries returns the persisted entries of id in [minTs, maxTs], stable-
// sorted by timestamp and deduplicated by (ts, line). Only the ref lookup holds
// the lock; chunk files are read and decompressed outside it.
func (c *ChunkStore) StreamEntries(ctx context.Context, id StreamID, minTs, maxTs int64) ([]LogEntry, error) {
	c.mu.Lock()
	refs := append([]ChunkRef(nil), c.index.chunkRefs(id, minTs, maxTs)...)
	c.mu.Unlock()

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
		if c.testBeforeChunkRead != nil {
			c.testBeforeChunkRead()
		}
		gotID, _, ch, err := readChunkFile(filepath.Join(c.chunksDir, ref.Name))
		if errors.Is(err, fs.ErrNotExist) {
			// Retention deleted the chunk after the refs were taken (or a crash
			// left a stale ref the next retention pass drops).
			continue
		}
		if err != nil {
			return nil, err
		}
		// Guard against an index ref pointing at another stream's chunk: the chunk
		// file embeds its own stream ID, so verify it matches the one we queried.
		if gotID != id {
			return nil, fmt.Errorf("logs: chunk %s belongs to stream %d, not %d", ref.Name, gotID, id)
		}
		it := ch.Iterator()
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
	sort.SliceStable(out, func(i, j int) bool { return out[i].TimestampNs < out[j].TimestampNs })
	return out, nil
}

// LabelNames returns persisted stream label names, sorted.
func (c *ChunkStore) LabelNames() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.index.postings.LabelNames()
}

// LabelValues returns persisted values for name, sorted.
func (c *ChunkStore) LabelValues(name string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.index.postings.LabelValues(name)
}

// streamIDs returns the set of persisted stream IDs.
func (c *ChunkStore) streamIDs() map[StreamID]struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	ids := make(map[StreamID]struct{}, len(c.index.labels))
	for id := range c.index.labels {
		ids[id] = struct{}{}
	}
	return ids
}

// Stats reports persisted stream count, chunk-file count, and chunk bytes. It
// is read at scrape time.
func (c *ChunkStore) Stats() (streams, chunks int, bytes int64, err error) {
	ids := c.streamIDs()
	chunks, bytes, err = c.fileStats()
	if err != nil {
		return 0, 0, 0, err
	}
	return len(ids), chunks, bytes, nil
}

// fileStats counts chunk files and their bytes from a directory walk.
//
// The walk is deliberately outside the lock: it is the slow part, and holding
// the mutex through it would stall ingest for the length of a scrape.
//
// OpenChunkStore creates chunksDir unconditionally (MkdirAllSync), so by the
// time Stats can run it must already exist. A missing directory therefore does
// not mean "not flushed yet" -- it means something deleted it out from under
// the store, which is an operational failure. Reporting that as zero with a nil
// error would show a confident zero on the dashboard instead of a gap, and
// obs_collector_errors_total would never move. The collector-error policy
// (ARCHITECTURE_NOTES.md) requires a gap plus a counted error for any failed
// read, ENOENT included.
func (c *ChunkStore) fileStats() (chunks int, bytes int64, err error) {
	entries, rerr := os.ReadDir(c.chunksDir)
	if rerr != nil {
		return 0, 0, fmt.Errorf("logs: readdir %s: %w", c.chunksDir, rerr)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".chunk") {
			continue
		}
		fi, ferr := e.Info()
		if ferr != nil {
			if os.IsNotExist(ferr) {
				continue
			}
			return 0, 0, fmt.Errorf("logs: info %s: %w", e.Name(), ferr)
		}
		chunks++
		bytes += fi.Size()
	}
	return chunks, bytes, nil
}

// ExportStreams calls fn with every persisted stream and all its entries, in
// batches of at most batchBytes estimated wire bytes (BatchStreams), and
// returns the number of distinct streams exported. ChunkStore has no block
// structure, so each stream is exported once.
func (c *ChunkStore) ExportStreams(ctx context.Context, batchBytes int, fn func([]StreamData) error) (streams int, err error) {
	ids := make([]StreamID, 0)
	for id := range c.streamIDs() {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(a, b int) bool { return ids[a] < ids[b] })
	var all []StreamData
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		labels, ok := c.StreamLabelSet(id)
		if !ok {
			continue
		}
		entries, err := c.StreamEntries(ctx, id, math.MinInt64, math.MaxInt64)
		if err != nil {
			return 0, err
		}
		if len(entries) == 0 {
			continue
		}
		all = append(all, StreamData{Labels: labels, Entries: entries})
	}
	for _, batch := range BatchStreams(all, batchBytes) {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if err := fn(batch); err != nil {
			return 0, err
		}
	}
	return len(all), nil
}
