package logs

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
)

// gatedSink wraps a real ChunkSink and pauses exactly the flush that reaches
// it: it closes entered the first time IngestStreams is called (proof that
// the flush has taken Head.mu and the wrapped sink does not have the streams
// yet), then blocks until the test closes gate.
type gatedSink struct {
	sink    ChunkSink
	once    sync.Once
	entered chan struct{}
	gate    chan struct{}
}

func newGatedSink(sink ChunkSink) *gatedSink {
	return &gatedSink{sink: sink, entered: make(chan struct{}), gate: make(chan struct{})}
}

func (g *gatedSink) IngestStreams(ctx context.Context, streams []StreamData) error {
	g.once.Do(func() { close(g.entered) })
	<-g.gate
	return g.sink.IngestStreams(ctx, streams)
}

// openPausableSplit opens a fresh Head+ChunkStore pair, wired through a
// gatedSink, with one stream buffered in the head and nothing yet in the
// chunk store. It returns the composed Store, that stream's ID, and the sink
// a test uses to pause the stream's first flush.
func openPausableSplit(t *testing.T) (*Store, StreamID, *gatedSink) {
	t.Helper()
	dir := t.TempDir()
	cs, err := OpenChunkStore(filepath.Join(dir, "chunks"), filepath.Join(dir, "index"))
	if err != nil {
		t.Fatalf("OpenChunkStore: %v", err)
	}
	sink := newGatedSink(cs)
	h, err := OpenHead(filepath.Join(dir, "wal"), 1<<20, 1, 1<<30, sink, HeadOptions{})
	if err != nil {
		t.Fatalf("OpenHead: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	s := &Store{head: h, chunks: cs}

	l := mustLabels(t, map[string]string{"service": "api"})
	if err := s.Append(l, 1, "x"); err != nil {
		t.Fatalf("Append: %v", err)
	}
	return s, StreamIDOf(l), sink
}

// pauseFlush starts s.Flush() in the background and blocks until it is
// paused mid-sink: Head.mu is held by the flush, and the chunk store does not
// have the stream yet (proof: sink.entered, closed only from inside
// IngestStreams). It returns release, which lets the paused flush proceed,
// and done, which carries the flush's result once release is called.
func pauseFlush(s *Store, sink *gatedSink) (release func(), done <-chan error) {
	flushDone := make(chan error, 1)
	go func() { flushDone <- s.Flush() }()
	<-sink.entered
	return func() { close(sink.gate) }, flushDone
}

// TestReadOrder_StreamLabelSet_DuringPausedFlush is the regression for the
// finding in fix round 1: Store.StreamLabelSet checked the chunk store
// before the head. A stream that lives only in the head, whose first flush
// is paused mid-sink (chunk store still empty, head not yet reset), was
// reported absent: the chunk-store check missed it (not written yet), and by
// the time the fallback head check ran — after blocking on Head.mu until the
// paused flush finished — the head had already been reset. Reading the head
// first fixes this: the head-side lookup blocks on the same lock, but by the
// time it is released the chunk store is guaranteed to already hold the
// stream (IngestStreams completes, inside the flush, strictly before the
// head is reset), so the fallback chunk lookup always finds it.
//
// The reader goroutine signals readerStarted as its first statement, before
// calling Store.StreamLabelSet, so the gate is never released before the
// reader has begun running: a chunk-store-first check (the bug) is then
// guaranteed to run — and miss, since it needs no lock at all — before the
// gated flush is allowed to perform the real (disk-bound, much slower) chunk
// write. No sleep is used anywhere in this synchronization.
func TestReadOrder_StreamLabelSet_DuringPausedFlush(t *testing.T) {
	s, id, sink := openPausableSplit(t)
	release, flushDone := pauseFlush(s, sink)

	readerStarted := make(chan struct{})
	okCh := make(chan bool, 1)
	go func() {
		close(readerStarted)
		_, ok := s.StreamLabelSet(id)
		okCh <- ok
	}()
	<-readerStarted

	release()
	if err := <-flushDone; err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if ok := <-okCh; !ok {
		t.Fatal("StreamLabelSet missed a stream present throughout the flush (head, then chunk store)")
	}
}

// TestReadOrder_MatchingStreamIDs_DuringPausedFlush pins the same head-first
// guarantee for MatchingStreamIDs: it must still list a head-only stream
// whose first flush is paused mid-sink.
func TestReadOrder_MatchingStreamIDs_DuringPausedFlush(t *testing.T) {
	s, id, sink := openPausableSplit(t)
	release, flushDone := pauseFlush(s, sink)

	readerStarted := make(chan struct{})
	idsCh := make(chan []StreamID, 1)
	go func() {
		close(readerStarted)
		idsCh <- s.MatchingStreamIDs(nil)
	}()
	<-readerStarted

	release()
	if err := <-flushDone; err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if ids := <-idsCh; len(ids) != 1 || ids[0] != id {
		t.Fatalf("MatchingStreamIDs = %v during a paused flush, want [%d]", ids, id)
	}
}

// TestReadOrder_StreamEntries_DuringPausedFlush pins the same head-first
// guarantee for StreamEntries: it must still return a head-only stream's
// entries while its first flush is paused mid-sink.
func TestReadOrder_StreamEntries_DuringPausedFlush(t *testing.T) {
	s, id, sink := openPausableSplit(t)
	release, flushDone := pauseFlush(s, sink)

	type result struct {
		entries []LogEntry
		err     error
	}
	readerStarted := make(chan struct{})
	resultCh := make(chan result, 1)
	go func() {
		close(readerStarted)
		entries, err := s.StreamEntries(context.Background(), id, 0, 100)
		resultCh <- result{entries, err}
	}()
	<-readerStarted

	release()
	if err := <-flushDone; err != nil {
		t.Fatalf("Flush: %v", err)
	}
	got := <-resultCh
	if got.err != nil {
		t.Fatalf("StreamEntries: %v", got.err)
	}
	if len(got.entries) != 1 || got.entries[0].Line != "x" {
		t.Fatalf("StreamEntries = %+v during a paused flush, want one entry \"x\"", got.entries)
	}
}

// TestReadOrder_StatsStreamCount_DuringPausedFlush pins the same head-first
// guarantee for Stats: its stream count must not momentarily drop to zero
// while a head-only stream's first flush is paused mid-sink.
func TestReadOrder_StatsStreamCount_DuringPausedFlush(t *testing.T) {
	s, _, sink := openPausableSplit(t)
	release, flushDone := pauseFlush(s, sink)

	type result struct {
		streams int
		err     error
	}
	readerStarted := make(chan struct{})
	resultCh := make(chan result, 1)
	go func() {
		close(readerStarted)
		streams, _, _, err := s.Stats()
		resultCh <- result{streams, err}
	}()
	<-readerStarted

	release()
	if err := <-flushDone; err != nil {
		t.Fatalf("Flush: %v", err)
	}
	got := <-resultCh
	if got.err != nil {
		t.Fatalf("Stats: %v", got.err)
	}
	if got.streams != 1 {
		t.Fatalf("Stats streams = %d during a paused flush, want 1", got.streams)
	}
}
