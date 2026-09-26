package logs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// flakySink fails while down is true and records every call.
type flakySink struct {
	mu      sync.Mutex
	down    bool
	calls   int
	batches [][]StreamData
	target  ChunkSink
	block   chan struct{} // when non-nil, IngestStreams waits for ctx
}

func (s *flakySink) IngestStreams(ctx context.Context, streams []StreamData) error {
	s.mu.Lock()
	s.calls++
	s.batches = append(s.batches, streams)
	down := s.down
	s.mu.Unlock()
	if s.block != nil {
		<-ctx.Done()
		return ctx.Err()
	}
	if down {
		return errors.New("simulated store outage")
	}
	return s.target.IngestStreams(ctx, streams)
}

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time { return c.t }

func openPolicyHead(t *testing.T, sink ChunkSink, opts HeadOptions, threshold int64) (*Head, string) {
	t.Helper()
	walDir := filepath.Join(t.TempDir(), "wal")
	h, err := OpenHead(walDir, 1<<20, 1, threshold, sink, opts)
	if err != nil {
		t.Fatalf("OpenHead: %v", err)
	}
	return h, walDir
}

func TestTolerantHeadKeepsAcceptingWhileTheStoreIsDown(t *testing.T) {
	dir := t.TempDir()
	cs, err := OpenChunkStore(filepath.Join(dir, "chunks"), filepath.Join(dir, "index"))
	if err != nil {
		t.Fatal(err)
	}
	sink := &flakySink{down: true, target: cs}
	clock := &fakeClock{t: time.Unix(1_000, 0)}
	var hookErrs []error
	h, walDir := openPolicyHead(t, sink, HeadOptions{TolerateFlushErrors: true, Now: clock.now}, 1)
	h.SetFlushHook(func(err error) { hookErrs = append(hookErrs, err) })
	l := mustLabels(t, map[string]string{"service": "api"})

	if err := h.Append(l, 1, "a"); err != nil {
		t.Fatalf("Append with the store down = %v, want nil: the entry is durable in the WAL", err)
	}
	if sink.calls != 1 || len(hookErrs) != 1 || hookErrs[0] == nil {
		t.Fatalf("calls=%d hook=%v, want one failed attempt reported", sink.calls, hookErrs)
	}
	if err := h.Append(l, 2, "b"); err != nil {
		t.Fatal(err)
	}
	if sink.calls != 1 {
		t.Fatalf("calls=%d during backoff, want still 1", sink.calls)
	}

	clock.t = clock.t.Add(DefaultLogFlushBackoff)
	sink.mu.Lock()
	sink.down = false
	sink.mu.Unlock()
	if err := h.Append(l, 3, "c"); err != nil {
		t.Fatal(err)
	}
	if sink.calls != 2 || hookErrs[len(hookErrs)-1] != nil {
		t.Fatalf("calls=%d last hook=%v, want a successful retry after the backoff", sink.calls, hookErrs[len(hookErrs)-1])
	}
	if got, _ := cs.StreamEntries(context.Background(), StreamIDOf(l), 0, 10); len(got) != 3 {
		t.Fatalf("store holds %d entries after recovery, want all 3", len(got))
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}

	// Nothing was lost to the outage: the checkpoint happened only after the
	// store had everything, so a fresh head replays nothing.
	h2, err := OpenHead(walDir, 1<<20, 1, 1<<30, cs, HeadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer h2.Close()
	if n := h2.StreamCount(); n != 0 {
		t.Fatalf("replayed %d streams after a completed flush, want 0", n)
	}
}

func TestStrictHeadFailsThePushOnAFlushError(t *testing.T) {
	h, _ := openPolicyHead(t, &flakySink{down: true}, HeadOptions{}, 1)
	defer func() { _ = h.Close() }()
	if err := h.Append(mustLabels(t, map[string]string{"service": "api"}), 1, "a"); err == nil {
		t.Fatal("all-in-one Append must surface the flush error, as before")
	}
}

func TestHeadBatchesAFlushAndResetsOnlyWhenEveryBatchLanded(t *testing.T) {
	dir := t.TempDir()
	cs, err := OpenChunkStore(filepath.Join(dir, "chunks"), filepath.Join(dir, "index"))
	if err != nil {
		t.Fatal(err)
	}
	sink := &flakySink{target: cs}
	h, _ := openPolicyHead(t, sink, HeadOptions{BatchBytes: 4}, 1<<30)
	defer func() { _ = h.Close() }()
	for i, svc := range []string{"a", "b", "c"} {
		if err := h.Append(mustLabels(t, map[string]string{"service": svc}), int64(i+1), "12345"); err != nil {
			t.Fatal(err)
		}
	}
	if err := h.Flush(); err != nil {
		t.Fatal(err)
	}
	if sink.calls != 3 {
		t.Fatalf("calls = %d, want one per stream at a 4-byte batch limit", sink.calls)
	}
	if n := h.StreamCount(); n != 0 {
		t.Fatalf("head holds %d streams, want 0", n)
	}
}

func TestHeadFlushTimesOut(t *testing.T) {
	sink := &flakySink{block: make(chan struct{})}
	h, _ := openPolicyHead(t, sink, HeadOptions{TolerateFlushErrors: true, FlushTimeout: 50 * time.Millisecond}, 1)
	var hookErr error
	h.SetFlushHook(func(err error) { hookErr = err })
	start := time.Now()
	if err := h.Append(mustLabels(t, map[string]string{"service": "api"}), 1, "a"); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 2*time.Second || !errors.Is(hookErr, context.DeadlineExceeded) {
		t.Fatalf("hung flush reported %v after %v, want a deadline error within the timeout", hookErr, time.Since(start))
	}
	_ = h.wal.Close()
}

func TestFlushHookIgnoresAnEmptyHead(t *testing.T) {
	h, _ := openPolicyHead(t, &flakySink{}, HeadOptions{}, 1<<30)
	calls := 0
	h.SetFlushHook(func(error) { calls++ })
	if err := h.Flush(); err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatalf("hook called %d times for an empty head, want 0", calls)
	}
	_ = h.Close()
}

// failNthSink fails exactly the call numbered failOn (1-indexed) and
// delegates to target on every other call, until cleared is set true, after
// which every call delegates regardless of count. It records every call like
// flakySink.
type failNthSink struct {
	mu      sync.Mutex
	failOn  int
	calls   int
	cleared bool
	target  ChunkSink
}

func (s *failNthSink) IngestStreams(ctx context.Context, streams []StreamData) error {
	s.mu.Lock()
	s.calls++
	call := s.calls
	cleared := s.cleared
	s.mu.Unlock()
	if !cleared && call == s.failOn {
		return errors.New("simulated store outage on one batch")
	}
	return s.target.IngestStreams(ctx, streams)
}

// TestHeadFlushLeavesTheHeadIntactWhenABatchFails is the regression for spec
// §12: "a partial batch failure leaves the head intact." Three streams flush
// as three one-entry batches (BatchBytes: 4 forces one stream per batch, as
// in TestHeadBatchesAFlushAndResetsOnlyWhenEveryBatchLanded); the sink fails
// only the 2nd. flushLocked must return that error without checkpointing or
// resetting — even though the 1st batch already reached the store — so a
// fresh head on the same WAL still replays all 3 streams. Once the failure
// clears and the retry succeeds, the store must hold each entry exactly once:
// that is the (ts, line) dedup on read absorbing the batch the first,
// partially-failed attempt already delivered.
func TestHeadFlushLeavesTheHeadIntactWhenABatchFails(t *testing.T) {
	dir := t.TempDir()
	cs, err := OpenChunkStore(filepath.Join(dir, "chunks"), filepath.Join(dir, "index"))
	if err != nil {
		t.Fatal(err)
	}
	sink := &failNthSink{failOn: 2, target: cs}
	h, walDir := openPolicyHead(t, sink, HeadOptions{BatchBytes: 4}, 1<<30)
	labels := []StreamLabels{
		mustLabels(t, map[string]string{"service": "a"}),
		mustLabels(t, map[string]string{"service": "b"}),
		mustLabels(t, map[string]string{"service": "c"}),
	}
	for i, l := range labels {
		if err := h.Append(l, int64(i+1), "12345"); err != nil {
			t.Fatal(err)
		}
	}

	if err := h.Flush(); err == nil {
		t.Fatal("Flush with the 2nd of 3 batches failing = nil, want an error")
	}
	if n := h.StreamCount(); n != 3 {
		t.Fatalf("head holds %d streams after a partial batch failure, want 3 (untouched, no checkpoint)", n)
	}

	// Nothing was checkpointed: a fresh head on the same WAL replays all 3
	// streams, including the one batch 1 already delivered to the store.
	if err := h.wal.Close(); err != nil {
		t.Fatal(err)
	}
	h2, err := OpenHead(walDir, 1<<20, 1, 1<<30, sink, HeadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if n := h2.StreamCount(); n != 3 {
		t.Fatalf("replayed %d streams after a partial batch failure, want 3", n)
	}

	sink.mu.Lock()
	sink.cleared = true
	sink.mu.Unlock()
	if err := h2.Flush(); err != nil {
		t.Fatalf("Flush after clearing the failure: %v", err)
	}
	if err := h2.Close(); err != nil {
		t.Fatal(err)
	}

	for i, l := range labels {
		got, err := cs.StreamEntries(context.Background(), StreamIDOf(l), 0, 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0].Line != "12345" || got[0].TimestampNs != int64(i+1) {
			t.Fatalf("stream %d: store holds %v after the retry, want exactly one (%d,\"12345\")", i, got, i+1)
		}
	}
}

// flatEntry names an entry by its stream's "service" label instead of a
// StreamID, so batchStreams's output (fresh StreamData values sharing the
// input's Labels) can be compared to its input by value.
type flatEntry struct {
	svc  string
	ts   int64
	line string
}

func flattenStreams(streams []StreamData) []flatEntry {
	var out []flatEntry
	for _, sd := range streams {
		svc, _ := sd.Labels.Get("service")
		for _, e := range sd.Entries {
			out = append(out, flatEntry{svc, e.TimestampNs, e.Line})
		}
	}
	return out
}

func flattenBatches(batches [][]StreamData) []flatEntry {
	var out []flatEntry
	for _, b := range batches {
		out = append(out, flattenStreams(b)...)
	}
	return out
}

// batchWireBytes sums a batch's estimated wire size the same way batchStreams
// does: each part's one-time open cost plus every one of its entries.
func batchWireBytes(batch []StreamData) int {
	n := 0
	for _, sd := range batch {
		n += streamOpenWireBytes(sd.Labels)
		for _, e := range sd.Entries {
			n += entryWireBytes(e)
		}
	}
	return n
}

// TestBatchStreamsSizesByEstimatedWireBytes is the regression for spec §12's
// body-limit requirement: batching must bound the request body it actually
// sends, not raw line length. Every case asserts the batches concatenate back
// to the input, in order, and that each batch's estimated wire size fits the
// limit — except a batch that is a single oversized entry, which cannot be
// made to fit by construction.
func TestBatchStreamsSizesByEstimatedWireBytes(t *testing.T) {
	svc := func(name string) StreamLabels { return mustLabels(t, map[string]string{"service": name}) }

	tests := []struct {
		name         string
		streams      []StreamData
		limit        int
		wantBatches  int
		oneOversized bool // every batch in this case is allowed to exceed limit
	}{
		{
			name: "a stream splits mid-way across several batches",
			streams: []StreamData{{Labels: svc("a"), Entries: []LogEntry{
				{TimestampNs: 1, Line: strings.Repeat("a", 10)},
				{TimestampNs: 2, Line: strings.Repeat("a", 10)},
				{TimestampNs: 3, Line: strings.Repeat("a", 10)},
			}}},
			limit:       100,
			wantBatches: 3,
		},
		{
			name: "a lone oversized entry travels alone",
			streams: []StreamData{{Labels: svc("a"), Entries: []LogEntry{
				{TimestampNs: 1, Line: strings.Repeat("x", 500)},
			}}},
			limit:        100,
			wantBatches:  1,
			oneOversized: true,
		},
		{
			// A byte-length-only estimate never grows past 0 for empty lines
			// (len("") == 0), so it would never split these no matter how many
			// there are. Estimating by wire bytes must split them anyway, since
			// each still costs its JSON framing.
			name: "empty lines still cost their framing and must split",
			streams: []StreamData{{Labels: svc("a"), Entries: []LogEntry{
				{TimestampNs: 1, Line: ""},
				{TimestampNs: 2, Line: ""},
				{TimestampNs: 3, Line: ""},
				{TimestampNs: 4, Line: ""},
				{TimestampNs: 5, Line: ""},
			}}},
			limit:       80,
			wantBatches: 5,
		},
		{
			// Two entries whose lines are all control characters and quotes:
			// counting raw bytes (naively escape-unaware) would total well
			// under limit and keep both in one batch; the correct escaped cost
			// (6 per control byte, 2 per quote) must force a split instead.
			name: "control characters and quotes cost their escaped length, not their raw length",
			streams: []StreamData{{Labels: svc("a"), Entries: []LogEntry{
				{TimestampNs: 1, Line: strings.Repeat("\x01\"", 10)},
				{TimestampNs: 2, Line: strings.Repeat("\x01\"", 10)},
			}}},
			limit:       200,
			wantBatches: 2,
		},
		{
			name: "limit <= 0 means one batch, as today",
			streams: []StreamData{
				{Labels: svc("a"), Entries: []LogEntry{{TimestampNs: 1, Line: "x"}}},
				{Labels: svc("b"), Entries: []LogEntry{{TimestampNs: 2, Line: "y"}}},
			},
			limit:       0,
			wantBatches: 1,
		},
		{
			// Many streams, each with a short line but an escape-heavy label
			// VALUE (control characters and quotes, legal per internal/labels —
			// values may hold up to 65535 bytes of arbitrary UTF-8). A raw byte
			// count on the label undercounts each stream's open cost by up to
			// 6x — enough to wrongly pack all 3 into one batch under the
			// limit below; the real, escape-aware cost must instead force 3
			// separate batches, each still comfortably within the limit (no
			// oversized exception needed here — unlike the lone-oversized-entry
			// case above, three well-estimated opens simply don't fit together).
			name: "escape-heavy label values force splits even with short lines",
			streams: []StreamData{
				{Labels: mustLabels(t, map[string]string{"service": "a", "payload": strings.Repeat("\x01\"", 20)}), Entries: []LogEntry{{TimestampNs: 1, Line: "x"}}},
				{Labels: mustLabels(t, map[string]string{"service": "b", "payload": strings.Repeat("\x01\"", 20)}), Entries: []LogEntry{{TimestampNs: 2, Line: "y"}}},
				{Labels: mustLabels(t, map[string]string{"service": "c", "payload": strings.Repeat("\x01\"", 20)}), Entries: []LogEntry{{TimestampNs: 3, Line: "z"}}},
			},
			// 400 sits strictly between what raw byte-length accounting for the
			// "payload" value would total for all 3 streams together (well
			// under 400: 20 escape-worthy bytes naively cost 20, not 20*8) and
			// what the correct, escape-aware cost totals (well over 400): a
			// naive accounting would wrongly keep all 3 in one batch.
			limit:       400,
			wantBatches: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batches := batchStreams(tt.streams, tt.limit)

			if len(batches) != tt.wantBatches {
				t.Fatalf("got %d batches, want %d", len(batches), tt.wantBatches)
			}
			if got, want := flattenBatches(batches), flattenStreams(tt.streams); !slices.Equal(got, want) {
				t.Fatalf("batches concatenate to %v, want %v (the input, in order)", got, want)
			}
			for i, b := range batches {
				size := batchWireBytes(b)
				if size > tt.limit && tt.limit > 0 && !tt.oneOversized {
					t.Fatalf("batch %d = %d estimated wire bytes, want <= %d (limit)", i, size, tt.limit)
				}
			}
		})
	}
}

// TestJSONStringBytesMatchesEncodingJSON pins jsonStringBytes to the actual
// bytes encoding/json emits (HTML escaping off, matching the RPC client's
// setting), for every character class its doc comment specifies: the two
// short-vs-\u00XX control-escape classes, quotes/backslashes, HTML-special
// bytes left plain, multi-byte UTF-8, the two runes encoding/json always
// escapes regardless of the HTML-escaping setting, and a 64 KiB label-value-
// sized input.
func TestJSONStringBytesMatchesEncodingJSON(t *testing.T) {
	allControlBytes := make([]byte, 0x20)
	for i := range allControlBytes {
		allControlBytes[i] = byte(i)
	}
	bigControlValue := strings.Repeat("\x00\x01\x02\x1f\"\\", 65536/6+1)[:65536]

	tests := []struct {
		name string
		s    string
	}{
		{"empty", ""},
		{"plain ASCII", "hello, world 123"},
		{"every byte 0x00-0x1F", string(allControlBytes)},
		{"quotes and backslashes", `she said "hi" \ then left`},
		{"angle brackets and ampersand stay plain", "<script>a&b</script>"},
		{"multi-byte UTF-8: e-acute", "café"},
		{"multi-byte UTF-8: CJK", "日本語"},
		{"multi-byte UTF-8: emoji", "hello \U0001F600 world"},
		{"U+2028 alone", "\u2028"},
		{"U+2029 alone", "\u2029"},
		{"U+2028 and U+2029 inside text", "line one\u2028line two\u2029line three"},
		{"64 KiB label-value-like control-byte string", bigControlValue},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			enc := json.NewEncoder(&buf)
			enc.SetEscapeHTML(false)
			if err := enc.Encode(tt.s); err != nil {
				t.Fatalf("Encode: %v", err)
			}
			want := len(bytes.TrimSuffix(buf.Bytes(), []byte("\n")))
			if got := jsonStringBytes(tt.s); got != want {
				t.Fatalf("jsonStringBytes(%q) = %d, want %d (encoding/json's actual output)", tt.s, got, want)
			}
		})
	}
}
