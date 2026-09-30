package metrics

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/masonwheeler/observability-platform/internal/storage/block"
	"github.com/masonwheeler/observability-platform/internal/storage/fsutil"
)

const (
	// DefaultFlushBatchBytes caps the encoded bytes (base64 chunks, label
	// JSON, and framing — see chunkWireBytes and seriesOpenWireBytes) one
	// flush request carries — well under the store's 64 MiB body limit, so a backlog after a long
	// store outage drains in several requests instead of one refused one.
	DefaultFlushBatchBytes = 16 << 20
	// DefaultFlushTimeout bounds one flush request.
	DefaultFlushTimeout = 30 * time.Second
)

// HeadStoreOptions tune a HeadStore's flushes. Zero values take the defaults.
type HeadStoreOptions struct {
	// BatchBytes caps the encoded request bytes of one flush batch; it
	// defaults to DefaultFlushBatchBytes.
	BatchBytes int
	Timeout    time.Duration
}

// HeadStore is a head-only metrics store for a process that does not own
// blocks: the ingester. Appends land in an in-memory head; FlushBlock sends the
// sealed chunks to a BlockSink and drops them only once the sink has them.
//
// The blocks that would otherwise seed the head's generation counter live in
// another process, so the HeadStore persists its own floor (GenFloorPath)
// before every flush sends anything: no block it ever ships can then outrank
// what it assigns after a restart.
type HeadStore struct {
	mem        *MemoryStore
	sink       BlockSink
	floorPath  string
	batchBytes int
	timeout    time.Duration
	flushMu    sync.Mutex
}

var _ walHead = (*HeadStore)(nil)

// GenFloorPath is where a HeadStore rooted at dataDir keeps its generation floor.
func GenFloorPath(dataDir string) string {
	return filepath.Join(dataDir, "metrics", "genfloor")
}

// OpenHeadStore opens an empty head for dataDir, seeding its generation counter
// from GenFloorPath. A missing file means a fresh ingester (floor 1). An
// unreadable or malformed one is an error naming the file: a guessed floor
// could let a stale value outrank a newer one.
func OpenHeadStore(dataDir string, sink BlockSink, opts HeadStoreOptions) (*HeadStore, error) {
	floorPath := GenFloorPath(dataDir)
	if err := fsutil.MkdirAllSync(filepath.Dir(floorPath)); err != nil {
		return nil, fmt.Errorf("metrics: mkdir %s: %w", filepath.Dir(floorPath), err)
	}
	floor, err := readGenFloor(floorPath)
	if err != nil {
		return nil, err
	}
	if opts.BatchBytes <= 0 {
		opts.BatchBytes = DefaultFlushBatchBytes
	}
	if opts.Timeout <= 0 {
		opts.Timeout = DefaultFlushTimeout
	}
	mem := NewMemoryStore()
	mem.EnsureGenFloor(floor)
	return &HeadStore{mem: mem, sink: sink, floorPath: floorPath, batchBytes: opts.BatchBytes, timeout: opts.Timeout}, nil
}

func readGenFloor(path string) (int64, error) {
	raw, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return 1, nil
	}
	if err != nil {
		return 0, fmt.Errorf("metrics: read generation floor %s: %w", path, err)
	}
	v, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
	if err != nil || v < 1 {
		return 0, fmt.Errorf("metrics: generation floor %s is malformed (%q)", path, strings.TrimSpace(string(raw)))
	}
	return v, nil
}

// writeGenFloor publishes v durably: tmp → fsync → rename → dir fsync.
func writeGenFloor(path string, v int64) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("metrics: create %s: %w", tmp, err)
	}
	if _, err := f.WriteString(strconv.FormatInt(v, 10) + "\n"); err != nil {
		f.Close()
		return fmt.Errorf("metrics: write %s: %w", tmp, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("metrics: fsync %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("metrics: close %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("metrics: publish %s: %w", path, err)
	}
	return fsutil.SyncDir(filepath.Dir(path))
}

// FlushBlock sends the head's sealed chunks to the sink in batches of at most
// the configured bytes, dropping each batch from memory once the sink has
// registered it, then drops series left with no chunks. It stops at the first
// failed batch: acknowledged batches stay dropped (the store has them) and the
// rest stay in memory for the next attempt. The error makes WALStore skip the
// checkpoint for this round; the per-chunk fence keeps a later one correct.
func (h *HeadStore) FlushBlock() (bool, error) {
	h.flushMu.Lock()
	defer h.flushMu.Unlock()

	snapshot := h.mem.SealedChunksSnapshot()
	if len(snapshot) == 0 {
		return false, nil
	}
	if err := writeGenFloor(h.floorPath, h.mem.NextGeneration()); err != nil {
		return false, err
	}
	sent := false
	for _, batch := range batchSeriesChunks(snapshot, h.batchBytes, block.MaxChunksPerSeries) {
		ctx, cancel := context.WithTimeout(context.Background(), h.timeout)
		_, err := h.sink.IngestSeriesChunks(ctx, batch)
		cancel()
		if err != nil {
			return sent, fmt.Errorf("metrics: flush to block sink: %w", err)
		}
		h.mem.DiscardSealedChunks(batch)
		sent = true
	}
	for _, id := range h.mem.EmptyHeadSeriesIDs() {
		h.mem.RemoveEmptySeries(id)
	}
	return true, nil
}

// batchSeriesChunks splits series into groups whose estimated encoded request
// bytes (chunkWireBytes per chunk, plus seriesOpenWireBytes once per series
// part) stay within limit, with no group holding more than maxChunksPerSeries chunks for
// any one series — block.OpenReader refuses a block whose index declares more
// than block.MaxChunksPerSeries chunks for a series, and it only discovers
// that after the store has written and fsynced the block, so the cap is
// enforced here instead. A series whose chunks exceed either bound is split
// across groups, continuing in the next one (reopening it, so the labels are
// charged again); a single chunk, or series-open plus chunk, larger than limit
// travels alone. A series never appears twice in one group, because
// IngestSeriesChunks refuses duplicate IDs.
func batchSeriesChunks(series []SeriesChunks, limit, maxChunksPerSeries int) [][]SeriesChunks {
	var batches [][]SeriesChunks
	var cur []SeriesChunks
	size := 0
	for _, sc := range series {
		part := SeriesChunks{ID: sc.ID, Labels: sc.Labels}
		open := seriesOpenWireBytes(sc.Labels) // charged once per part, to its first chunk
		for _, c := range sc.Chunks {
			n := chunkWireBytes(len(c.Bytes())) + open
			if (size > 0 && size+n > limit) || len(part.Chunks) >= maxChunksPerSeries {
				if len(part.Chunks) > 0 {
					cur = append(cur, part)
					part = SeriesChunks{ID: sc.ID, Labels: sc.Labels}
					open = seriesOpenWireBytes(sc.Labels)
					n = chunkWireBytes(len(c.Bytes())) + open
				}
				batches = append(batches, cur)
				cur, size = nil, 0
			}
			part.Chunks = append(part.Chunks, c)
			size += n
			open = 0
		}
		if len(part.Chunks) > 0 {
			cur = append(cur, part)
		}
	}
	if len(cur) > 0 {
		batches = append(batches, cur)
	}
	return batches
}

// chunkFramingBytes is one chunk's JSON framing in a flush body beyond its
// base64 text: the two quotes and a trailing comma (a deliberate overcount for
// the last chunk of a series).
const chunkFramingBytes = 2 + 1

// seriesFramingBytes is a conservative estimate of one series part's own JSON
// framing, `{"labels":{...},"chunks":[...]},`, independent of its label and
// chunk counts (26 bytes, rounded up). The exact wire format belongs to
// internal/rpc, which this package must not import.
const seriesFramingBytes = 32

// labelFramingBytes is one label's framing inside the "labels" object, beyond
// its name and value: the colon and comma in `"name":"value",`.
const labelFramingBytes = 1 + 1

// chunkWireBytes is one chunk's encoded cost in a flush body: the standard
// base64 text encoding/json emits for a []byte, plus framing.
func chunkWireBytes(rawLen int) int {
	return base64.StdEncoding.EncodedLen(rawLen) + chunkFramingBytes
}

// seriesOpenWireBytes is a series part's one-time wire cost when it opens in a
// batch: its framing plus every label's exact JSON size.
func seriesOpenWireBytes(l Labels) int {
	n := seriesFramingBytes
	for name, value := range l.Map() {
		n += labelFramingBytes + jsonStringBytes(name) + jsonStringBytes(value)
	}
	return n
}

// jsonStringBytes returns exactly how many bytes encoding/json emits for s as
// a JSON string, quotes included, with HTML escaping off (the RPC client's
// setting). It is a copy of internal/logs's jsonStringBytes (this package must
// not import logs), mirroring encoding/json's appendString: with HTML escaping
// off, '\b' '\t' '\n' '\f' '\r' cost 2, other bytes < 0x20 cost 6, '"' and
// '\\' cost 2, U+2028/U+2029 cost 6, every other byte costs 1. s is assumed
// valid UTF-8 (label values are validated at ingest).
func jsonStringBytes(s string) int {
	n := 2
	for i := 0; i < len(s); {
		if b := s[i]; b < utf8.RuneSelf {
			switch {
			case b == '\b' || b == '\t' || b == '\n' || b == '\f' || b == '\r':
				n += 2
			case b == '"' || b == '\\':
				n += 2
			case b < 0x20:
				n += 6
			default:
				n++
			}
			i++
			continue
		}
		c, size := utf8.DecodeRuneInString(s[i:])
		if c == '\u2028' || c == '\u2029' {
			n += 6
		} else {
			n += size
		}
		i += size
	}
	return n
}

// Append adds a sample without WAL tracking; WAL replay uses it.
func (h *HeadStore) Append(labels Labels, tsMs int64, val float64) error {
	return h.mem.Append(labels, tsMs, val)
}

// AppendTracked adds a sample and records its WAL segment for the fence.
func (h *HeadStore) AppendTracked(labels Labels, tsMs int64, val float64, walSeg int) error {
	return h.mem.AppendTracked(labels, tsMs, val, walSeg)
}

func (h *HeadStore) GenerationExhausted() bool               { return h.mem.GenerationExhausted() }
func (h *HeadStore) OldestHeadSegment() int                  { return h.mem.OldestHeadSegment() }
func (h *HeadStore) SetHeadFence(walSeg int)                 { h.mem.SetHeadFence(walSeg) }
func (h *HeadStore) SealedChunkCount() int                   { return h.mem.SealedChunkCount() }
func (h *HeadStore) Cardinality() (series, names, pairs int) { return h.mem.Cardinality() }
func (h *HeadStore) SelectSeries(sel Selector) ([]MatchedSeries, error) {
	return h.mem.SelectSeries(sel)
}
func (h *HeadStore) QueryInstant(id SeriesID, tMs int64) (Sample, bool, error) {
	return h.mem.QueryInstant(id, tMs)
}
func (h *HeadStore) QueryRange(id SeriesID, startMs, endMs int64) ([]Sample, error) {
	return h.mem.QueryRange(id, startMs, endMs)
}
func (h *HeadStore) LabelNames() []string          { return h.mem.LabelNames() }
func (h *HeadStore) LabelValues(n string) []string { return h.mem.LabelValues(n) }
func (h *HeadStore) Select(ctx context.Context, p SelectParams) ([]SeriesData, error) {
	return h.mem.Select(ctx, p)
}
func (h *HeadStore) SelectLabelNames(ctx context.Context) ([]string, error) {
	return h.mem.SelectLabelNames(ctx)
}
func (h *HeadStore) SelectLabelValues(ctx context.Context, name string) ([]string, error) {
	return h.mem.SelectLabelValues(ctx, name)
}
