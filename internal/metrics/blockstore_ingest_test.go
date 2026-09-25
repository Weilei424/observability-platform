package metrics_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/storage/block"
	"github.com/masonwheeler/observability-platform/internal/storage/chunk"
)

// sealedChunkAt returns a sealed chunk of n samples starting at base, one per
// second. (blockstore_test.go already declares a sealedChunk helper.)
func sealedChunkAt(t *testing.T, base int64, n int, gen int64) *chunk.Chunk {
	t.Helper()
	c := chunk.NewChunk()
	for i := range n {
		if err := c.Append(base+int64(i)*1000, float64(i), gen+int64(i)); err != nil {
			t.Fatalf("chunk Append: %v", err)
		}
	}
	return c
}

func TestBlockStore_IngestSeriesChunks_PersistsAndServes(t *testing.T) {
	dir := t.TempDir()
	bs, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })

	l, _ := metrics.NewLabels(map[string]string{"__name__": "ingested", "k": "v"})
	meta, err := bs.IngestSeriesChunks(context.Background(), []metrics.SeriesChunks{{
		ID: metrics.SeriesID(l.Hash()), Labels: l, Chunks: []*chunk.Chunk{sealedChunkAt(t, 0, 120, 1)},
	}})
	if err != nil {
		t.Fatalf("IngestSeriesChunks: %v", err)
	}
	if meta.NumSeries != 1 || meta.NumSamples != 120 {
		t.Fatalf("meta = %+v, want 1 series and 120 samples", meta)
	}
	// gens run 1..120 (gen=1+sample index, samples 0..119) and timestamps run
	// 0..119000 one per second, so MaxGen/MinTime/MaxTime pin the exact chunk
	// built by sealedChunkAt(t, 0, 120, 1), not just its counts.
	if meta.MaxGen != 120 {
		t.Fatalf("meta.MaxGen = %d, want 120", meta.MaxGen)
	}
	if meta.MinTime != 0 || meta.MaxTime != 119_000 {
		t.Fatalf("meta time bounds = [%d, %d], want [0, 119000]", meta.MinTime, meta.MaxTime)
	}

	// Queryable before the call returned — the flush-in contract.
	got, err := bs.QueryRange(metrics.SeriesID(l.Hash()), 0, 119_000)
	if err != nil || len(got) != 120 {
		t.Fatalf("QueryRange = %d samples, %v; want 120", len(got), err)
	}

	// And durable: a restart finds it.
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	bs2, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = bs2.Close() })
	got2, err := bs2.QueryRange(metrics.SeriesID(l.Hash()), 0, 119_000)
	if err != nil {
		t.Fatalf("QueryRange after restart: %v", err)
	}
	if len(got2) != 120 {
		t.Fatalf("after restart: %d samples, want 120", len(got2))
	}
}

func TestBlockStore_IngestSeriesChunks_RejectsEmptyAndMismatchedInput(t *testing.T) {
	dir := t.TempDir()
	bs, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })

	assertRejected := func(t *testing.T, desc string, series []metrics.SeriesChunks) {
		t.Helper()
		_, err := bs.IngestSeriesChunks(context.Background(), series)
		if !errors.Is(err, metrics.ErrInvalidSeriesChunks) {
			t.Fatalf("%s: err = %v, want a wrapped ErrInvalidSeriesChunks", desc, err)
		}
	}

	l, _ := metrics.NewLabels(map[string]string{"__name__": "mismatch"})
	lNoChunks, _ := metrics.NewLabels(map[string]string{"__name__": "no_chunks"})
	lNilChunk, _ := metrics.NewLabels(map[string]string{"__name__": "nil_chunk"})
	lEmptyChunk, _ := metrics.NewLabels(map[string]string{"__name__": "empty_chunk"})
	lDup, _ := metrics.NewLabels(map[string]string{"__name__": "dup"})

	assertRejected(t, "an empty ingest", nil)

	// An ID that is not the label set's fingerprint must be refused before any
	// write, leaving nothing on disk.
	assertRejected(t, "a fingerprint mismatch", []metrics.SeriesChunks{{
		ID: metrics.SeriesID(l.Hash() + 1), Labels: l, Chunks: []*chunk.Chunk{sealedChunkAt(t, 0, 120, 1)},
	}})

	// A series with no chunks at all.
	assertRejected(t, "a series with no chunks", []metrics.SeriesChunks{{
		ID: metrics.SeriesID(lNoChunks.Hash()), Labels: lNoChunks, Chunks: nil,
	}})

	// A nil chunk would otherwise panic in block.Writer.AddSeries.
	assertRejected(t, "a nil chunk", []metrics.SeriesChunks{{
		ID: metrics.SeriesID(lNilChunk.Hash()), Labels: lNilChunk, Chunks: []*chunk.Chunk{nil},
	}})

	// A zero-sample chunk is otherwise accepted by the writer and would publish
	// a fingerprint with no data.
	assertRejected(t, "a zero-sample chunk", []metrics.SeriesChunks{{
		ID: metrics.SeriesID(lEmptyChunk.Hash()), Labels: lEmptyChunk, Chunks: []*chunk.Chunk{chunk.NewChunk()},
	}})

	// A duplicate series ID within one ingest, caught up front rather than by
	// the writer's own (post-tmp-dir-creation) duplicate check.
	assertRejected(t, "a duplicate series ID", []metrics.SeriesChunks{
		{ID: metrics.SeriesID(lDup.Hash()), Labels: lDup, Chunks: []*chunk.Chunk{sealedChunkAt(t, 0, 120, 1)}},
		{ID: metrics.SeriesID(lDup.Hash()), Labels: lDup, Chunks: []*chunk.Chunk{sealedChunkAt(t, 0, 120, 1)}},
	})

	entries, err := os.ReadDir(filepath.Join(dir, "metrics", "blocks"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("a rejected ingest left %d block(s) on disk", len(entries))
	}
	tmpEntries, err := os.ReadDir(filepath.Join(dir, "metrics", "tmp"))
	if err != nil {
		t.Fatal(err)
	}
	if len(tmpEntries) != 0 {
		t.Fatalf("a rejected ingest left %d tmp entrie(s) on disk", len(tmpEntries))
	}
}

func TestBlockStore_IngestSeriesChunks_HonoursCancellation(t *testing.T) {
	dir := t.TempDir()
	bs, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	l, _ := metrics.NewLabels(map[string]string{"__name__": "cancelled"})
	_, err = bs.IngestSeriesChunks(ctx, []metrics.SeriesChunks{{
		ID: metrics.SeriesID(l.Hash()), Labels: l, Chunks: []*chunk.Chunk{sealedChunkAt(t, 0, 120, 1)},
	}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("IngestSeriesChunks with a cancelled context = %v, want context.Canceled", err)
	}
	entries, rdErr := os.ReadDir(filepath.Join(dir, "metrics", "blocks"))
	if rdErr != nil {
		t.Fatal(rdErr)
	}
	if len(entries) != 0 {
		t.Fatalf("a cancelled ingest left %d block(s) on disk", len(entries))
	}
}

// TestBlockStore_IngestSeriesChunks_CancelledWhileWaitingForLock is the
// regression for a cancellation that arrives while the call is queued behind
// flushMu: the first ctx.Err() check (before the lock) can pass and then the
// call blocks in flushMu.Lock() while a concurrent CompactOnce holds it. This
// drives that window without any test-only hook: CompactOnce's plan callback
// runs while flushMu is held (blockstore.go), so a plan that parks on a
// channel keeps flushMu held for as long as the test needs. A context wrapper
// reports the instant IngestSeriesChunks makes its first ctx.Err() call, so
// the test can guarantee cancellation happens only after that first check has
// already passed (and while flushMu is still held) — otherwise the goroutine
// scheduler could just as easily run the cancellation before IngestSeriesChunks
// ever checks ctx, which would pass this test for the wrong reason (the
// pre-lock check catching it) whether or not the post-lock re-check exists.
type firstErrCheckCtx struct {
	context.Context
	once    sync.Once
	checked chan struct{}
}

func newFirstErrCheckCtx(parent context.Context) *firstErrCheckCtx {
	return &firstErrCheckCtx{Context: parent, checked: make(chan struct{})}
}

// Err reports the wrapped context's error, closing checked the first time it
// is called so a test can observe exactly when the code under test performs
// its first cancellation check.
func (c *firstErrCheckCtx) Err() error {
	c.once.Do(func() { close(c.checked) })
	return c.Context.Err()
}

func TestBlockStore_IngestSeriesChunks_CancelledWhileWaitingForLock(t *testing.T) {
	dir := t.TempDir()
	bs, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })

	planStarted := make(chan struct{})
	release := make(chan struct{})
	compactDone := make(chan struct{})
	go func() {
		_, _ = bs.CompactOnce(func([]block.BlockInfo) [][]string {
			close(planStarted)
			<-release
			return nil
		})
		close(compactDone)
	}()
	<-planStarted // flushMu is now held by CompactOnce until release is closed.

	realCtx, cancel := context.WithCancel(context.Background())
	pctx := newFirstErrCheckCtx(realCtx)
	ingestErr := make(chan error, 1)
	go func() {
		l, _ := metrics.NewLabels(map[string]string{"__name__": "blocked_on_lock"})
		_, err := bs.IngestSeriesChunks(pctx, []metrics.SeriesChunks{{
			ID: metrics.SeriesID(l.Hash()), Labels: l, Chunks: []*chunk.Chunk{sealedChunkAt(t, 0, 120, 1)},
		}})
		ingestErr <- err
	}()

	// Wait for the pre-lock ctx.Err() check to actually run. It sees a live
	// context (not yet cancelled), so IngestSeriesChunks proceeds to
	// flushMu.Lock() and blocks there, since flushMu is still held (release is
	// not yet closed).
	<-pctx.checked
	cancel()
	close(release)
	<-compactDone

	if err := <-ingestErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("IngestSeriesChunks cancelled while waiting for flushMu = %v, want context.Canceled", err)
	}
	entries, err := os.ReadDir(filepath.Join(dir, "metrics", "blocks"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("a cancelled ingest left %d block(s) on disk", len(entries))
	}
}
