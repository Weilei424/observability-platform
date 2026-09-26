package logs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/masonwheeler/observability-platform/internal/storage/fsutil"
	"github.com/masonwheeler/observability-platform/internal/storage/index"
	"github.com/masonwheeler/observability-platform/internal/storage/logchunk"
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
// Chunk files are immutable once written and never deleted (logs have no
// compaction or retention yet), so a ref taken under the lock stays readable
// after it is released. That is what lets reads decode chunks outside the lock.
// Revisit when logs retention lands.
type ChunkStore struct {
	mu        sync.Mutex
	index     *streamIndex
	chunksDir string
	indexPath string
}

var _ ChunkSink = (*ChunkStore)(nil)
var _ Reader = (*ChunkStore)(nil)

// OpenChunkStore opens (or creates) a chunk store, loading the persisted index
// and rebuilding it from the chunk headers if the manifest is missing or
// corrupt: the chunks are the source of truth, the manifest a rebuildable cache.
func OpenChunkStore(chunksDir, indexDir string) (*ChunkStore, error) {
	for _, d := range []string{chunksDir, indexDir} {
		if err := fsutil.MkdirAllSync(d); err != nil {
			return nil, fmt.Errorf("logs: mkdir %s: %w", d, err)
		}
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
	return &ChunkStore{index: idx, chunksDir: chunksDir, indexPath: indexPath}, nil
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
		gotID, _, ch, err := readChunkFile(filepath.Join(c.chunksDir, ref.Name))
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
