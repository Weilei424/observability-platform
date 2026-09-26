package logs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/masonwheeler/observability-platform/internal/storage/fsutil"
	"github.com/masonwheeler/observability-platform/internal/storage/index"
	"github.com/masonwheeler/observability-platform/internal/storage/logwal"
)

const (
	// DefaultLogFlushBackoff is how long a tolerant head waits after a failed
	// flush before trying again, so a store outage costs one attempt per
	// interval rather than one per push.
	DefaultLogFlushBackoff = 30 * time.Second
	// DefaultLogFlushTimeout bounds one sink call from a tolerant head. A flush
	// holds the head lock across every batch it sends, so a store that hangs on
	// every request can still stall a push for up to this long per batch, not
	// just once per flush.
	DefaultLogFlushTimeout = 10 * time.Second
	// DefaultLogFlushBatchBytes caps the estimated wire bytes one sink call
	// carries — well under the store's 64 MiB body limit.
	DefaultLogFlushBatchBytes = 16 << 20
)

// HeadOptions tune how a Head flushes. The zero value is all-in-one's
// behavior: one unbounded sink call per flush, and a flush error returned to
// the push that triggered it.
type HeadOptions struct {
	// TolerateFlushErrors keeps a failed threshold flush from failing the push
	// that triggered it — the entry is already durable in the WAL and the head —
	// and pauses further threshold flushes for FlushBackoff. The ingester sets
	// it. Explicit Flush and Close still return the error.
	TolerateFlushErrors bool
	// FlushBackoff defaults to DefaultLogFlushBackoff when TolerateFlushErrors is set.
	FlushBackoff time.Duration
	// FlushTimeout bounds each sink call. Zero means none in strict/all-in-one
	// mode; it defaults to DefaultLogFlushTimeout when TolerateFlushErrors is
	// set, so a tolerant head can never hold its lock forever on a hung store.
	FlushTimeout time.Duration
	// BatchBytes caps the estimated wire bytes — the JSON an IngestStreams
	// request would encode, not raw line length — per sink call. Zero means one
	// call in strict/all-in-one mode; it defaults to DefaultLogFlushBatchBytes
	// when TolerateFlushErrors is set, keeping every request under the store's
	// body limit.
	BatchBytes int
	// Now is the clock for the backoff; nil means time.Now.
	Now func() time.Time
}

// Head is the logs write path's in-memory half: a WAL-backed per-stream buffer
// that flushes the whole head to a ChunkSink at a size threshold and on Close,
// checkpointing the WAL once the sink has everything.
//
// The flush holds the lock from snapshot to reset. LogWAL.Checkpoint deletes
// every segment, so it is only correct when nothing can be appended while the
// flush runs; holding the lock is how that is guaranteed. It also means a
// reader that takes the lock sees either the whole head or an empty one whose
// entries the sink already has — never a moment in which an entry is readable
// nowhere.
type Head struct {
	mu          sync.Mutex
	head        map[StreamID]*memoryStream
	wal         logWAL
	sink        ChunkSink
	headBytes   int64
	flushThresh int64

	opts         HeadOptions
	backoffUntil time.Time
	onFlush      func(error)
}

var _ Reader = (*Head)(nil)

// OpenHead replays the WAL in walDir into a new head and opens the WAL for
// appends. Flushes go to sink, tuned by opts.
func OpenHead(walDir string, segMaxBytes int64, syncEveryN int, flushThreshold int64, sink ChunkSink, opts HeadOptions) (*Head, error) {
	if err := fsutil.MkdirAllSync(walDir); err != nil {
		return nil, fmt.Errorf("logs: mkdir %s: %w", walDir, err)
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
			// the metrics replay path, and reaches the application logger because
			// main sets slog's default.
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
	if opts.TolerateFlushErrors {
		// All three defaults apply only in tolerant mode: an all-in-one head
		// (zero HeadOptions, not tolerant) must keep today's behavior exactly —
		// no timeout, one unbounded batch — since it fails the push on any
		// flush error and so never needs bounding for its own sake.
		if opts.FlushBackoff <= 0 {
			opts.FlushBackoff = DefaultLogFlushBackoff
		}
		if opts.FlushTimeout <= 0 {
			opts.FlushTimeout = DefaultLogFlushTimeout
		}
		if opts.BatchBytes <= 0 {
			opts.BatchBytes = DefaultLogFlushBatchBytes
		}
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Head{head: head, wal: lw, sink: sink, headBytes: headBytes, flushThresh: flushThreshold, opts: opts}, nil
}

// Append writes the record to the WAL, buffers it in the head, and, once
// buffered bytes cross the threshold, attempts to flush the whole head. A
// strict (all-in-one) head returns that attempt's error; a tolerant head may
// instead skip the attempt during its backoff window or swallow its failure —
// the flush hook, not this return value, is what reports it in that case.
func (h *Head) Append(labels StreamLabels, tsNs int64, line string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.wal.WriteRecord(labelsToWALPairs(labels), tsNs, line); err != nil {
		return err
	}
	id := StreamIDOf(labels)
	hs := h.head[id]
	if hs == nil {
		hs = &memoryStream{labels: labels}
		h.head[id] = hs
	}
	hs.entries = append(hs.entries, LogEntry{StreamID: id, TimestampNs: tsNs, Line: line})
	h.headBytes += int64(8 + len(line))
	if h.flushThresh > 0 && h.headBytes >= h.flushThresh {
		return h.thresholdFlushLocked()
	}
	return nil
}

// thresholdFlushLocked runs the flush a threshold crossing triggers. A strict
// head (all-in-one) returns its error to the push; a tolerant one (the
// ingester) reports it through the hook, backs off, and lets the push succeed.
func (h *Head) thresholdFlushLocked() error {
	if h.opts.TolerateFlushErrors && h.opts.Now().Before(h.backoffUntil) {
		return nil
	}
	flushed, err := h.flushLocked()
	h.report(flushed, err)
	if err != nil && h.opts.TolerateFlushErrors {
		h.backoffUntil = h.opts.Now().Add(h.opts.FlushBackoff)
		return nil
	}
	return err
}

// SetFlushHook installs fn, called with nil after every flush that moved data
// and with the error after every failed one. Call before concurrent use.
//
// fn runs with the head lock held, so it must not call back into h — that
// would deadlock — and should return quickly: until it does, every Append,
// Flush, Close, and read blocks behind it.
func (h *Head) SetFlushHook(fn func(err error)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.onFlush = fn
}

func (h *Head) report(flushed bool, err error) {
	if h.onFlush != nil && (flushed || err != nil) {
		h.onFlush(err)
	}
}

// Flush drains the head to the sink and checkpoints the WAL. Safe to call when
// the head is empty (no-op). Unlike a threshold-triggered flush, Flush always
// attempts the sink even during a tolerant head's backoff window, and a
// failure here never arms that backoff — only a failed threshold flush does.
func (h *Head) Flush() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	flushed, err := h.flushLocked()
	h.report(flushed, err)
	return err
}

// Close flushes the head and closes the WAL, returning both errors if both
// fail. The WAL is closed even when the flush fails, and that ordering is the
// point: a failed flush is exactly when the WAL matters most — it is then the
// only durable copy of the head, the one the next start replays from — and
// LogWAL.Close is what fsyncs its tail, which with batched syncing may hold
// records already acknowledged to clients.
func (h *Head) Close() error {
	h.mu.Lock()
	flushed, flushErr := h.flushLocked()
	h.report(flushed, flushErr)
	h.mu.Unlock()
	return errors.Join(flushErr, h.wal.Close())
}

// flushLocked sends every head stream to the sink, checkpoints the WAL, then
// resets the head, reporting whether there was anything to flush. The caller
// holds h.mu.
func (h *Head) flushLocked() (bool, error) {
	if len(h.head) == 0 {
		return false, nil
	}
	for _, batch := range batchStreams(h.snapshotLocked(), h.opts.BatchBytes) {
		ctx, cancel := context.Background(), context.CancelFunc(func() {})
		if h.opts.FlushTimeout > 0 {
			ctx, cancel = context.WithTimeout(context.Background(), h.opts.FlushTimeout)
		}
		err := h.sink.IngestStreams(ctx, batch)
		cancel()
		if err != nil {
			return true, err
		}
	}
	if err := h.wal.Checkpoint(); err != nil {
		return true, err
	}
	h.head = make(map[StreamID]*memoryStream)
	h.headBytes = 0
	return true, nil
}

// snapshotLocked returns every head stream, ascending by ID, with its entries
// in append order. The caller holds h.mu.
func (h *Head) snapshotLocked() []StreamData {
	ids := make([]StreamID, 0, len(h.head))
	for id := range h.head {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	out := make([]StreamData, 0, len(ids))
	for _, id := range ids {
		hs := h.head[id]
		out = append(out, StreamData{Labels: hs.labels, Entries: append([]LogEntry(nil), hs.entries...)})
	}
	return out
}

// headEntries returns a copy of id's buffered entries in append order.
func (h *Head) headEntries(id StreamID) []LogEntry {
	h.mu.Lock()
	defer h.mu.Unlock()
	if hs := h.head[id]; hs != nil {
		return append([]LogEntry(nil), hs.entries...)
	}
	return nil
}

// MatchingStreamIDs returns the sorted IDs of head streams matching all matchers.
func (h *Head) MatchingStreamIDs(matchers []index.Pair) []StreamID {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []StreamID
	for id, hs := range h.head {
		if streamMatches(hs.labels, matchers) {
			out = append(out, id)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// StreamLabelSet returns a head stream's labels.
func (h *Head) StreamLabelSet(id StreamID) (StreamLabels, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if hs := h.head[id]; hs != nil {
		return hs.labels, true
	}
	return StreamLabels{}, false
}

// StreamEntries returns the head's entries of id in [minTs, maxTs], stable-
// sorted by timestamp and deduplicated by (ts, line).
func (h *Head) StreamEntries(_ context.Context, id StreamID, minTs, maxTs int64) ([]LogEntry, error) {
	return mergeEntries(nil, h.headEntries(id), minTs, maxTs), nil
}

// LabelNames returns head stream label names, sorted.
func (h *Head) LabelNames() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	set := make(map[string]struct{})
	for _, hs := range h.head {
		for n := range hs.labels.Map() {
			set[n] = struct{}{}
		}
	}
	return sortedKeys(set)
}

// LabelValues returns head values for name, sorted.
func (h *Head) LabelValues(name string) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	set := make(map[string]struct{})
	for _, hs := range h.head {
		if v, ok := hs.labels.Get(name); ok {
			set[v] = struct{}{}
		}
	}
	return sortedKeys(set)
}

// StreamCount returns the number of streams buffered in the head.
func (h *Head) StreamCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.head)
}

// streamIDs returns the set of head stream IDs.
func (h *Head) streamIDs() map[StreamID]struct{} {
	h.mu.Lock()
	defer h.mu.Unlock()
	ids := make(map[StreamID]struct{}, len(h.head))
	for id := range h.head {
		ids[id] = struct{}{}
	}
	return ids
}

// mergeEntries appends to persisted (sorted, deduplicated) the entries of head
// in [minTs, maxTs] it does not already hold, then stable-sorts by timestamp —
// so at an equal timestamp persisted entries stay ahead of head entries, the
// order Store.StreamEntries has always produced and the engine's tie-breaking
// depends on.
func mergeEntries(persisted, head []LogEntry, minTs, maxTs int64) []LogEntry {
	type key struct {
		ts   int64
		line string
	}
	seen := make(map[key]struct{}, len(persisted))
	for _, e := range persisted {
		seen[key{e.TimestampNs, e.Line}] = struct{}{}
	}
	out := persisted
	for _, e := range head {
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
	return out
}

// jsonStringBytes returns exactly how many bytes encoding/json emits for s as
// a JSON string, quotes included, with HTML escaping off
// (json.Encoder.SetEscapeHTML(false)) — the setting the RPC client will use.
// It mirrors encoding/json's appendString (GOROOT src/encoding/json/encode.go,
// checked against this toolchain, go1.26.0): with HTML escaping off, only the
// ASCII control characters (0-31), '"', and '\\' need escaping — '<', '>',
// and '&' are left as plain, 1-byte-each bytes. The costs:
//
//   - '\b' '\t' '\n' '\f' '\r' cost 2 (their short escape, e.g. "\n")
//   - any other byte < 0x20 costs 6 (its "\u00XX" escape)
//   - '"' and '\\' cost 2 ("\"" or "\\")
//   - U+2028 and U+2029 cost 6 each ("\u2028" / "\u2029"), replacing their 3
//     UTF-8 bytes — appendString escapes them unconditionally, HTML-escaping
//     setting aside, because they break JSONP
//   - every other byte costs 1
//
// s is assumed valid UTF-8: the push path rejects invalid UTF-8 before an
// entry line or a label value (validated as UTF-8 at ingest, see
// internal/labels) can reach the head, so this does not special-case it.
func jsonStringBytes(s string) int {
	n := 2 // the surrounding quotes
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

// logEntryFramingBytes is one entry's own JSON framing cost in an
// IngestStreams request body, beyond the line's own encoded bytes (charged
// separately, via jsonStringBytes, quotes included): "[" (1) + a
// nanosecond-epoch timestamp, at most 19 digits (int64's max,
// 9223372036854775807, has 19) (19) + "," separating it from the line (1) +
// a trailing "," charged to every entry as a stand-in for the separator
// before the next one, or the closing "]" for the last (1) + that closing
// "]" itself (1) = 23. Charging every entry a comma it might not need is a
// deliberate overcount: it only ever shrinks a batch, never grows one past
// limit.
const logEntryFramingBytes = 1 + 19 + 1 + 1 + 1 // 23

// wireStreamFramingBytes is a conservative estimate of a stream part's own
// JSON object framing in an IngestStreams request body — the braces, keys,
// and colons of something shaped like `{"labels":{...},"entries":[...]},` —
// independent of its label count or entry count, which are charged
// separately. Rounded up for headroom: the exact wire format belongs to
// internal/rpc, not this package, and entries dominate a batch's size anyway.
const wireStreamFramingBytes = 32

// wireLabelFramingBytes is one label's own JSON framing inside a stream's
// "labels" object, beyond its name and value's own encoded bytes (charged
// separately, via jsonStringBytes, quotes included): the colon and comma in
// `"name":"value",`.
const wireLabelFramingBytes = 1 + 1 // ':' + ','

// entryWireBytes estimates one entry's encoded size in an IngestStreams
// request body: its JSON framing plus its line's exact encoded size.
func entryWireBytes(e LogEntry) int {
	return logEntryFramingBytes + jsonStringBytes(e.Line)
}

// streamOpenWireBytes estimates a stream part's one-time wire cost when it
// opens within a batch: its own JSON framing plus every label's framing and
// its name and value's exact encoded size. Label values may legally hold up
// to 65535 bytes of arbitrary valid UTF-8 (internal/labels validation), so
// escaping-heavy values are charged the same way entry lines are — a raw
// byte count would undercount them by up to 6x, which is exactly the
// underestimate this whole scheme exists to close. Charged once per part —
// every continuation of a stream that a split pushes into a new batch
// reopens it, and pays this again, because the new request must re-encode
// the labels too.
func streamOpenWireBytes(labels StreamLabels) int {
	n := wireStreamFramingBytes
	for name, value := range labels.Map() {
		n += wireLabelFramingBytes + jsonStringBytes(name) + jsonStringBytes(value)
	}
	return n
}

// batchStreams splits streams into groups whose estimated wire size — what an
// IngestStreams request body would actually encode, not raw line length —
// stays at or below limit, splitting a stream across groups when it alone
// exceeds the limit; a single oversized entry travels alone. limit <= 0 means
// one group. A failure part-way leaves the head intact, so the next attempt
// resends batches the sink already has — duplicates the (ts, line) dedup on
// read absorbs, as it already absorbs the crash window between chunk write
// and checkpoint.
func batchStreams(streams []StreamData, limit int) [][]StreamData {
	if limit <= 0 {
		return [][]StreamData{streams}
	}
	var batches [][]StreamData
	var cur []StreamData
	size := 0
	for _, sd := range streams {
		part := StreamData{Labels: sd.Labels}
		openCost := streamOpenWireBytes(sd.Labels) // charged once, to this part's first entry
		for _, e := range sd.Entries {
			n := entryWireBytes(e) + openCost
			if size > 0 && size+n > limit {
				if len(part.Entries) > 0 {
					cur = append(cur, part)
					part = StreamData{Labels: sd.Labels}
					openCost = streamOpenWireBytes(sd.Labels)
					n = entryWireBytes(e) + openCost
				}
				batches = append(batches, cur)
				cur, size = nil, 0
			}
			part.Entries = append(part.Entries, e)
			size += n
			openCost = 0
		}
		if len(part.Entries) > 0 {
			cur = append(cur, part)
		}
	}
	if len(cur) > 0 {
		batches = append(batches, cur)
	}
	return batches
}
