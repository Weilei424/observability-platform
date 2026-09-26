package logs

import (
	"context"
	"errors"
	"path/filepath"
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
