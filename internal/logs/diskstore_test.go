package logs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/storage/index"
	"github.com/masonwheeler/observability-platform/internal/storage/logwal"
)

func newTestStore(t *testing.T, dir string, flushThreshold int64) *Store {
	t.Helper()
	s, err := NewStore(
		filepath.Join(dir, "wal"),
		filepath.Join(dir, "chunks"),
		filepath.Join(dir, "index"),
		1<<20, 1, flushThreshold,
	)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	return s
}

func TestStore_RebuildsIndexWhenManifestMissing(t *testing.T) {
	dir := t.TempDir()
	labels := mustLabels(t, map[string]string{"service": "api"})
	id := StreamIDOf(labels)

	s := newTestStore(t, dir, 1<<30) // flush only on Close
	if err := s.Append(labels, 100, "a"); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := s.Append(labels, 200, "b"); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := s.Close(); err != nil { // flush + checkpoint: WAL now holds nothing
		t.Fatalf("Close: %v", err)
	}

	// Delete the manifest but keep the chunk files. A missing manifest MUST rebuild
	// from chunk headers, not silently hide the persisted logs.
	if err := os.Remove(filepath.Join(dir, "index", "streams.index")); err != nil {
		t.Fatalf("remove manifest: %v", err)
	}

	s2 := newTestStore(t, dir, 1<<30)
	defer s2.Close()
	got, err := s2.StreamEntries(context.Background(), id, 0, 1000)
	if err != nil {
		t.Fatalf("StreamEntries: %v", err)
	}
	if len(got) != 2 || got[0].Line != "a" || got[1].Line != "b" {
		t.Fatalf("after manifest deletion entries = %+v, want a,b (must rebuild from chunks)", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "index", "streams.index")); err != nil {
		t.Errorf("expected manifest to be rewritten after rebuild: %v", err)
	}
}

func TestStore_RebuildsIndexWhenManifestCorrupt(t *testing.T) {
	dir := t.TempDir()
	labels := mustLabels(t, map[string]string{"service": "api"})
	id := StreamIDOf(labels)

	s := newTestStore(t, dir, 1<<30)
	if err := s.Append(labels, 100, "x"); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Same-length body corruption the manifest CRC must catch, routing to rebuild.
	mpath := filepath.Join(dir, "index", "streams.index")
	data, err := os.ReadFile(mpath)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xff
	if err := os.WriteFile(mpath, data, 0o644); err != nil {
		t.Fatal(err)
	}

	s2 := newTestStore(t, dir, 1<<30)
	defer s2.Close()
	got, err := s2.StreamEntries(context.Background(), id, 0, 1000)
	if err != nil {
		t.Fatalf("StreamEntries: %v", err)
	}
	if len(got) != 1 || got[0].Line != "x" {
		t.Fatalf("corrupt manifest not recovered from chunks: %+v", got)
	}
}

func TestStore_RebuildRejectsTamperedChunkHeader(t *testing.T) {
	dir := t.TempDir()
	labels := mustLabels(t, map[string]string{"service": "api"})

	s := newTestStore(t, dir, 1<<30)
	if err := s.Append(labels, 100, "x"); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Remove the manifest so reopening must rebuild from chunk headers.
	if err := os.Remove(filepath.Join(dir, "index", "streams.index")); err != nil {
		t.Fatalf("remove manifest: %v", err)
	}
	// Tamper the logchunk minTs byte in the one chunk file (single-label header is 27
	// bytes, minTs at +5). This must be rejected on rebuild, not laundered into a new
	// checksum-valid manifest.
	chunksDir := filepath.Join(dir, "chunks")
	entries, _ := os.ReadDir(chunksDir)
	var cpath string
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".chunk" {
			cpath = filepath.Join(chunksDir, e.Name())
		}
	}
	data, err := os.ReadFile(cpath)
	if err != nil {
		t.Fatal(err)
	}
	data[27+5] ^= 0xff
	if err := os.WriteFile(cpath, data, 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := NewStore(
		filepath.Join(dir, "wal"), chunksDir, filepath.Join(dir, "index"),
		1<<20, 1, 1<<30,
	); err == nil {
		t.Error("NewStore should fail rebuilding from a tampered chunk header, not launder it")
	}
}

func TestStore_RebuildRejectsUnsupportedChunkVersion(t *testing.T) {
	dir := t.TempDir()
	labels := mustLabels(t, map[string]string{"service": "api"})

	s := newTestStore(t, dir, 1<<30)
	if err := s.Append(labels, 100, "x"); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := os.Remove(filepath.Join(dir, "index", "streams.index")); err != nil {
		t.Fatalf("remove manifest: %v", err)
	}
	// The single-label chunk file header is 27 bytes; the embedded logchunk version
	// byte is at 27+4. Force it to an unsupported version 1. A rebuild MUST fail
	// (fail-closed) rather than laundering an unreadable chunk into a new manifest.
	chunksDir := filepath.Join(dir, "chunks")
	entries, _ := os.ReadDir(chunksDir)
	var cpath string
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".chunk" {
			cpath = filepath.Join(chunksDir, e.Name())
		}
	}
	data, err := os.ReadFile(cpath)
	if err != nil {
		t.Fatal(err)
	}
	data[27+4] = 1
	if err := os.WriteFile(cpath, data, 0o644); err != nil {
		t.Fatal(err)
	}

	_, err = NewStore(
		filepath.Join(dir, "wal"), chunksDir, filepath.Join(dir, "index"),
		1<<20, 1, 1<<30,
	)
	if err == nil {
		t.Fatal("NewStore should fail rebuilding a chunk with an unsupported version")
	}
	if !strings.Contains(err.Error(), "version") {
		t.Errorf("rebuild error = %q, want it to name the unsupported version", err.Error())
	}
}

func TestSplitIntoChunks_CapsUncompressedSize(t *testing.T) {
	var entries []LogEntry
	for i := 0; i < 10; i++ {
		entries = append(entries, LogEntry{TimestampNs: int64(100 + i), Line: "hello"})
	}
	const cap = 50
	chunks := splitIntoChunks(entries, cap)
	if len(chunks) < 2 {
		t.Fatalf("expected splitting into multiple chunks, got %d", len(chunks))
	}
	total := 0
	var flat []int64
	for _, c := range chunks {
		if c.UncompressedBytes() > cap {
			t.Errorf("chunk uncompressed %d exceeds cap %d", c.UncompressedBytes(), cap)
		}
		total += c.NumEntries()
		it := c.Iterator()
		for it.Next() {
			ts, _ := it.At()
			flat = append(flat, ts)
		}
	}
	if total != len(entries) {
		t.Fatalf("entries preserved = %d, want %d", total, len(entries))
	}
	for i, ts := range flat {
		if ts != int64(100+i) {
			t.Fatalf("entry %d ts=%d, want %d (order not preserved across split)", i, ts, 100+i)
		}
	}
}

func TestStore_ThresholdFlushWritesChunksAndManifest(t *testing.T) {
	dir := t.TempDir()
	s := newTestStore(t, dir, 1) // tiny threshold: flush after first append
	labels := mustLabels(t, map[string]string{"service": "api"})
	if err := s.Append(labels, 100, "hello"); err != nil {
		t.Fatalf("Append: %v", err)
	}

	chunks, _ := os.ReadDir(filepath.Join(dir, "chunks"))
	if len(chunks) == 0 {
		t.Fatal("expected a chunk file after threshold flush")
	}
	if _, err := os.Stat(filepath.Join(dir, "index", "streams.index")); err != nil {
		t.Fatalf("expected manifest: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestStore_SurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	labels := mustLabels(t, map[string]string{"service": "api"})

	s := newTestStore(t, dir, 1<<30) // no threshold flush; flush happens on Close
	if err := s.Append(labels, 100, "a"); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := s.Append(labels, 200, "b"); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := s.Close(); err != nil { // flushes head to chunks + checkpoints WAL
		t.Fatalf("Close: %v", err)
	}

	s2 := newTestStore(t, dir, 1<<30)
	defer s2.Close()
	id := StreamIDOf(labels)
	got, err := s2.StreamEntries(context.Background(), id, 0, 1000)
	if err != nil {
		t.Fatalf("StreamEntries: %v", err)
	}
	if len(got) != 2 || got[0].Line != "a" || got[1].Line != "b" {
		t.Fatalf("after restart entries = %+v, want a,b", got)
	}
}

func TestStore_RecoversUnflushedEntriesFromWAL(t *testing.T) {
	// A crash BEFORE any flush leaves entries only in the WAL (no chunks). Restart
	// must recover them purely from WAL replay. The flush-then-restart tests can
	// pass from persisted chunks even if replay were broken; this one cannot.
	dir := t.TempDir()
	labels := mustLabels(t, map[string]string{"service": "api"})
	id := StreamIDOf(labels)

	s := newTestStore(t, dir, 1<<30) // high threshold: no auto-flush
	if err := s.Append(labels, 100, "a"); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := s.Append(labels, 200, "b"); err != nil {
		t.Fatalf("Append: %v", err)
	}
	// Simulate a crash: close ONLY the WAL, with no flush to chunks.
	if err := s.closeWALForTest(); err != nil {
		t.Fatalf("closeWALForTest: %v", err)
	}

	// Precondition: nothing was flushed, so no chunk files exist.
	chunks, _ := os.ReadDir(filepath.Join(dir, "chunks"))
	for _, e := range chunks {
		if filepath.Ext(e.Name()) == ".chunk" {
			t.Fatalf("expected no chunk files before flush, found %s", e.Name())
		}
	}

	// Restart: recovery must come purely from WAL replay.
	s2 := newTestStore(t, dir, 1<<30)
	defer s2.Close()
	got, err := s2.StreamEntries(context.Background(), id, 0, 1000)
	if err != nil {
		t.Fatalf("StreamEntries: %v", err)
	}
	if len(got) != 2 || got[0].Line != "a" || got[1].Line != "b" {
		t.Fatalf("WAL-only recovery entries = %+v, want a,b", got)
	}
}

func TestStore_CheckpointPreventsDoubleCount(t *testing.T) {
	dir := t.TempDir()
	labels := mustLabels(t, map[string]string{"service": "api"})

	s := newTestStore(t, dir, 1) // flush after each append
	if err := s.Append(labels, 100, "a"); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2 := newTestStore(t, dir, 1<<30)
	defer s2.Close()
	got, err := s2.StreamEntries(context.Background(), StreamIDOf(labels), 0, 1000)
	if err != nil {
		t.Fatalf("StreamEntries: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("entries = %d, want 1 (no double count from WAL + chunk)", len(got))
	}
}

func TestStore_CheckpointTruncatesFlushedWAL(t *testing.T) {
	// Prove the flush actually checkpoints the WAL — not via the deduping read path
	// (which would hide a missing checkpoint by merging chunk/WAL duplicates), but by
	// replaying the WAL directly and asserting only the post-flush record remains.
	dir := t.TempDir()
	labels := mustLabels(t, map[string]string{"service": "api"})

	// headBytes accrues 8+len(line) per append. Threshold 15 flushes on the second
	// append (9 -> 19), checkpointing the WAL; the third append stays in the head.
	s := newTestStore(t, dir, 15)
	if err := s.Append(labels, 100, "a"); err != nil { // headBytes 9
		t.Fatalf("Append a: %v", err)
	}
	if err := s.Append(labels, 200, "bb"); err != nil { // headBytes 19 >= 15 -> flush + checkpoint
		t.Fatalf("Append bb: %v", err)
	}
	if err := s.Append(labels, 300, "c"); err != nil { // headBytes 9 -> unflushed
		t.Fatalf("Append c: %v", err)
	}

	var lines []string
	if err := logwal.Replay(filepath.Join(dir, "wal"), func(_ []logwal.LabelPair, _ int64, line string) {
		lines = append(lines, line)
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(lines) != 1 || lines[0] != "c" {
		t.Fatalf("direct WAL replay = %v, want only [c] (a/bb must have been checkpointed away)", lines)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestStore_LabelFilterNarrows(t *testing.T) {
	dir := t.TempDir()
	s := newTestStore(t, dir, 1<<30)
	defer s.Close()
	api := mustLabels(t, map[string]string{"service": "api"})
	web := mustLabels(t, map[string]string{"service": "web"})
	_ = s.Append(api, 100, "x")
	_ = s.Append(web, 100, "y")

	got := s.MatchingStreamIDs([]index.Pair{{Name: "service", Value: "api"}})
	if len(got) != 1 || got[0] != StreamIDOf(api) {
		t.Fatalf("matching = %v, want [api]", got)
	}
}

func TestStore_LabelAccessors_HeadAndFlushed(t *testing.T) {
	s := newTestStore(t, t.TempDir(), 8<<20)
	api := mustLabels(t, map[string]string{"service": "api", "level": "info"})
	web := mustLabels(t, map[string]string{"service": "web"})
	if err := s.Append(api, 100, "a"); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(web, 100, "w"); err != nil {
		t.Fatal(err)
	}

	// Head-only state.
	if got, ok := s.StreamLabelSet(StreamIDOf(api)); !ok || got.Hash() != api.Hash() {
		t.Fatalf("StreamLabelSet(head) = %v,%v", got, ok)
	}
	assertStringSet(t, s.LabelNames(), []string{"level", "service"})
	assertStringSet(t, s.LabelValues("service"), []string{"api", "web"})

	// Flush → same answers from the persisted index.
	if err := s.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if _, ok := s.StreamLabelSet(StreamIDOf(web)); !ok {
		t.Fatalf("StreamLabelSet(flushed) not found")
	}
	assertStringSet(t, s.LabelNames(), []string{"level", "service"})
	assertStringSet(t, s.LabelValues("service"), []string{"api", "web"})

	// Unknown id / name.
	if _, ok := s.StreamLabelSet(StreamID(999999)); ok {
		t.Fatalf("expected unknown stream id to be absent")
	}
	if v := s.LabelValues("nope"); len(v) != 0 {
		t.Fatalf("expected no values for unknown label, got %v", v)
	}
}

func assertStringSet(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("set = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("set = %v, want %v (sorted)", got, want)
		}
	}
}

func TestStore_LabelAccessors_MixedHeadAndIndex(t *testing.T) {
	s := newTestStore(t, t.TempDir(), 8<<20) // high threshold, no auto-flush

	// api is flushed: its labels live only in the persisted index, head cleared.
	api := mustLabels(t, map[string]string{"service": "api", "level": "info"})
	if err := s.Append(api, 100, "a"); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// web is appended after the flush: its labels live only in the head. It carries
	// its own label name ("region") that api does not have, so a LabelNames() that
	// silently dropped the head would be caught (api's index-only names alone are
	// {level, service}, a strict subset of the expected union) — not just a
	// LabelValues()/StreamLabelSet() short-circuit.
	web := mustLabels(t, map[string]string{"service": "web", "region": "us-east"})
	if err := s.Append(web, 200, "w"); err != nil {
		t.Fatal(err)
	}

	// Mixed state: api is served from the persisted index, web from the head. Each
	// accessor must consult both sources, not short-circuit to just one.
	if got, ok := s.StreamLabelSet(StreamIDOf(api)); !ok || got.Hash() != api.Hash() {
		t.Fatalf("StreamLabelSet(index-only stream) = %v,%v", got, ok)
	}
	if got, ok := s.StreamLabelSet(StreamIDOf(web)); !ok || got.Hash() != web.Hash() {
		t.Fatalf("StreamLabelSet(head-only stream) = %v,%v", got, ok)
	}
	assertStringSet(t, s.LabelNames(), []string{"level", "region", "service"})
	assertStringSet(t, s.LabelValues("service"), []string{"api", "web"})
}

func TestStore_MergeDedupsCrashWindow(t *testing.T) {
	// Simulate the crash window: a chunk was written but the WAL was NOT
	// checkpointed, so a WAL replay reintroduces the same entry. The merged read
	// must dedup by (streamID, tsNs, line).
	dir := t.TempDir()
	labels := mustLabels(t, map[string]string{"service": "api"})
	id := StreamIDOf(labels)

	s := newTestStore(t, dir, 1<<30)
	// One entry in the head, backed by the WAL.
	if err := s.Append(labels, 100, "dup"); err != nil {
		t.Fatalf("Append: %v", err)
	}
	// Force just the chunk write + index (no checkpoint) to mimic a crash after
	// chunks but before WAL checkpoint.
	if err := s.writeChunksAndIndexForTest(); err != nil {
		t.Fatalf("writeChunksAndIndexForTest: %v", err)
	}
	if err := s.closeWALForTest(); err != nil {
		t.Fatalf("closeWALForTest: %v", err)
	}

	s2 := newTestStore(t, dir, 1<<30) // manifest has the chunk; WAL still has "dup"
	defer s2.Close()
	got, err := s2.StreamEntries(context.Background(), id, 0, 1000)
	if err != nil {
		t.Fatalf("StreamEntries: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("entries = %d, want 1 (crash-window duplicate must be deduped)", len(got))
	}
}

// TestStreamEntries_ConcurrentWithAppend covers the read path that decodes chunk
// files outside s.mu. Appends (and the flushes they trigger) must run
// concurrently with queries without racing and without a reader observing a
// partial view. Run with -race for this to mean anything.
func TestStreamEntries_ConcurrentWithAppend(t *testing.T) {
	// A small flush threshold guarantees real chunk files get written mid-test,
	// so readers hit the file-decode path rather than only the head.
	s := newTestStore(t, t.TempDir(), 256)
	t.Cleanup(func() { _ = s.Close() })
	labels := mustLabels(t, map[string]string{"service": "api"})
	id := StreamIDOf(labels)

	const writes = 150
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 1; i <= writes; i++ {
			if err := s.Append(labels, int64(i), fmt.Sprintf("line-%d", i)); err != nil {
				t.Errorf("append %d: %v", i, err)
				return
			}
		}
	}()

	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var last int
			for i := 0; i < 50; i++ {
				got, err := s.StreamEntries(context.Background(), id, 0, int64(writes))
				if err != nil {
					t.Errorf("StreamEntries: %v", err)
					return
				}
				// Entries only ever accumulate, so a later read must never see
				// fewer than an earlier one — that would mean a flush briefly
				// dropped data from the reader's view.
				if len(got) < last {
					t.Errorf("entry count went backwards: %d then %d", last, len(got))
					return
				}
				last = len(got)
			}
		}()
	}
	wg.Wait()

	got, err := s.StreamEntries(context.Background(), id, 0, int64(writes))
	if err != nil {
		t.Fatalf("final StreamEntries: %v", err)
	}
	if len(got) != writes {
		t.Fatalf("final entry count = %d, want %d", len(got), writes)
	}
	for i, e := range got {
		if e.TimestampNs != int64(i+1) {
			t.Fatalf("entry %d ts = %d, want %d", i, e.TimestampNs, i+1)
		}
	}
}

// errAfterNCtx reports itself cancelled only after Err has been consulted n
// times. StreamEntries checks ctx.Err() once per chunk, so this makes
// "cancelled part-way through a multi-chunk read" deterministic instead of
// racing a timer.
type errAfterNCtx struct {
	context.Context
	mu    sync.Mutex
	calls int
	after int
}

func (c *errAfterNCtx) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if c.calls > c.after {
		return context.Canceled
	}
	return nil
}

func (c *errAfterNCtx) seen() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

// TestStreamEntries_CancelledBetweenChunkReads covers the per-chunk ctx check in
// StreamEntries, which a context cancelled before the call never reaches — the
// engine bails out first.
func TestStreamEntries_CancelledBetweenChunkReads(t *testing.T) {
	dir := t.TempDir()
	s := newTestStore(t, dir, 1) // flush per append: one chunk file per entry
	t.Cleanup(func() { _ = s.Close() })
	labels := mustLabels(t, map[string]string{"service": "api"})
	id := StreamIDOf(labels)

	const chunks = 4
	for i := 1; i <= chunks; i++ {
		if err := s.Append(labels, int64(i), fmt.Sprintf("line-%d", i)); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	files, err := os.ReadDir(filepath.Join(dir, "chunks"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) < 2 {
		t.Fatalf("got %d chunk files, need >= 2 to cancel between reads", len(files))
	}

	// A plain background context reads everything: the baseline the cancelled
	// run has to differ from.
	all, err := s.StreamEntries(context.Background(), id, 0, int64(chunks))
	if err != nil {
		t.Fatalf("baseline StreamEntries: %v", err)
	}
	if len(all) != chunks {
		t.Fatalf("baseline entries = %d, want %d", len(all), chunks)
	}

	ctx := &errAfterNCtx{Context: context.Background(), after: 1}
	got, err := s.StreamEntries(ctx, id, 0, int64(chunks))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("StreamEntries err = %v, want context.Canceled", err)
	}
	if got != nil {
		t.Errorf("cancelled read returned %d entries, want nil (no partial result)", len(got))
	}
	// One check passed, the next cancelled: the loop really did run per chunk.
	if ctx.seen() != 2 {
		t.Errorf("ctx.Err() consulted %d times, want 2 (once per chunk until cancelled)", ctx.seen())
	}
}

// walCloseSpy records whether Close reached the underlying WAL. Embedding the
// interface passes WriteRecord and Checkpoint straight through.
type walCloseSpy struct {
	logWAL
	closed bool
}

func (w *walCloseSpy) Close() error {
	w.closed = true
	return w.logWAL.Close()
}

// TestStore_CloseClosesWALEvenWhenFlushFails pins the durability contract for the
// one case that matters most. LogWAL.Close is what fsyncs the WAL tail, and with
// batched syncing that tail can hold records already acknowledged to a client. If
// Close returns as soon as the flush fails, those records are never synced — so a
// failed flush, which is supposed to leave the WAL as the durable recovery copy,
// would instead be the thing that destroys it.
func TestStore_CloseClosesWALEvenWhenFlushFails(t *testing.T) {
	dir := t.TempDir()
	chunksDir := filepath.Join(dir, "chunks")
	s := newTestStore(t, dir, 0) // 0 = never auto-flush, so the head survives to Close

	lbls := mustLabels(t, map[string]string{"service": "api"})
	if err := s.Append(lbls, 100, "acknowledged, not yet synced"); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Make the flush fail: replace the chunks directory with a regular file, so
	// writing a chunk into it cannot succeed.
	if err := os.RemoveAll(chunksDir); err != nil {
		t.Fatalf("RemoveAll: %v", err)
	}
	if err := os.WriteFile(chunksDir, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	spy := &walCloseSpy{logWAL: s.wal}
	s.wal = spy

	err := s.Close()
	if err == nil {
		t.Fatal("Close() = nil, want the flush error surfaced")
	}
	if !spy.closed {
		t.Error("Close() skipped the WAL after a failed flush: the tail is left unsynced, losing acknowledged records")
	}
}

// TestNewStore_ReplayWarnsOnInvalidLabels covers the recovery warning the 4.2
// design requires. Log WAL records carry no checksum, so a structurally valid
// record can hold semantically corrupt labels and land here; skipping it silently
// makes real data loss invisible in the startup logs.
func TestNewStore_ReplayWarnsOnInvalidLabels(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")

	w, err := logwal.Open(walDir, 1<<20, 1)
	if err != nil {
		t.Fatalf("logwal.Open: %v", err)
	}
	// No label pairs at all: structurally fine as a record, but NewStreamLabels
	// rejects it, which is the branch under test.
	if err := w.WriteRecord(nil, 100, "orphaned line"); err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var logged bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelWarn})))
	defer slog.SetDefault(prev)

	s := newTestStore(t, dir, 0)
	defer func() { _ = s.Close() }()

	if got := logged.String(); !strings.Contains(got, "invalid stream labels") {
		t.Errorf("replay logged %q, want a warning naming the skipped record", got)
	}
}

func appendLine(t *testing.T, s *Store, labels map[string]string, tsNs int64, line string) {
	t.Helper()
	if err := s.Append(mustLabels(t, labels), tsNs, line); err != nil {
		t.Fatalf("Append: %v", err)
	}
}

func TestStoreStatsCountsHeadAndPersistedStreams(t *testing.T) {
	s := newTestStore(t, t.TempDir(), 1<<30)
	defer s.Close()

	// Two streams in the head, nothing flushed yet: streams counted, no chunks.
	appendLine(t, s, map[string]string{"service": "api"}, 1_000, "one")
	appendLine(t, s, map[string]string{"service": "web"}, 2_000, "two")

	streams, chunks, bytes, err := s.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if streams != 2 {
		t.Errorf("streams = %d, want 2 (both still in the head)", streams)
	}
	if chunks != 0 || bytes != 0 {
		t.Errorf("chunks/bytes = %d/%d, want 0/0 before any flush", chunks, bytes)
	}

	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	streams, chunks, bytes, err = s.Stats()
	if err != nil {
		t.Fatalf("Stats after flush: %v", err)
	}
	// The head is drained into the index. The same two streams must still be
	// counted once each — double-counting across head and index is the bug this
	// asserts against.
	if streams != 2 {
		t.Errorf("streams = %d after flush, want 2", streams)
	}
	if chunks != 2 {
		t.Errorf("chunks = %d, want 2 (one chunk file per stream)", chunks)
	}
	if bytes <= 0 {
		t.Errorf("bytes = %d, want > 0 once chunk files exist", bytes)
	}
}

func TestStoreStatsDoesNotDoubleCountAStreamInBothHeadAndIndex(t *testing.T) {
	s := newTestStore(t, t.TempDir(), 1<<30)
	defer s.Close()

	appendLine(t, s, map[string]string{"service": "api"}, 1_000, "before flush")
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	// Same labels again: now the stream exists in the index AND in the head.
	appendLine(t, s, map[string]string{"service": "api"}, 3_000, "after flush")

	streams, _, _, err := s.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if streams != 1 {
		t.Errorf("streams = %d, want 1; the same stream in head and index is one stream", streams)
	}
}

func TestStoreStatsWithMissingChunksDirectory(t *testing.T) {
	dir := t.TempDir()
	chunksDir := filepath.Join(dir, "chunks")
	
	s := newTestStore(t, dir, 1<<30)
	defer s.Close()
	
	// Append some lines so we have in-memory streams
	appendLine(t, s, map[string]string{"service": "api"}, 1_000, "one")
	appendLine(t, s, map[string]string{"service": "web"}, 2_000, "two")
	
	// Remove the chunks directory entirely
	if err := os.RemoveAll(chunksDir); err != nil {
		t.Fatalf("RemoveAll: %v", err)
	}
	
	// Stats should still report in-memory streams, with bytes=0 and no error
	streams, chunks, bytes, err := s.Stats()
	if err != nil {
		t.Fatalf("Stats with missing chunks dir: %v", err)
	}
	if streams != 2 {
		t.Errorf("streams = %d, want 2", streams)
	}
	if chunks != 0 {
		t.Errorf("chunks = %d, want 0 (no persisted chunks)", chunks)
	}
	if bytes != 0 {
		t.Errorf("bytes = %d, want 0 when chunks dir missing", bytes)
	}
}

func TestStoreStatsIgnoresOrphanedChunkTempFiles(t *testing.T) {
	s := newTestStore(t, t.TempDir(), 1<<30)
	defer s.Close()
	
	// Append and flush to create a real chunk file
	appendLine(t, s, map[string]string{"service": "api"}, 1_000, "one")
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	
	// Get baseline byte count
	_, chunks1, bytes1, err := s.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if chunks1 != 1 || bytes1 <= 0 {
		t.Fatalf("baseline stats wrong: chunks=%d bytes=%d", chunks1, bytes1)
	}
	
	// Create an orphaned .chunk.tmp file (simulating a crash between fsync and rename)
	s.mu.Lock()
	actualChunksDir := s.chunksDir
	s.mu.Unlock()
	
	tmpPath := filepath.Join(actualChunksDir, "orphaned.chunk.tmp")
	if err := os.WriteFile(tmpPath, []byte("fake chunk data with some bytes"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	
	// Stats should NOT count the .chunk.tmp file's bytes
	_, chunks2, bytes2, err := s.Stats()
	if err != nil {
		t.Fatalf("Stats with orphaned tmp: %v", err)
	}
	if chunks2 != 1 {
		t.Errorf("chunks = %d, want 1 (.tmp not counted)", chunks2)
	}
	if bytes2 != bytes1 {
		t.Errorf("bytes = %d, want %d (orphaned .tmp should not be counted)", bytes2, bytes1)
	}
}
