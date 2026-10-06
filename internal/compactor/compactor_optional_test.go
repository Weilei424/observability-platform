package compactor_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/masonwheeler/observability-platform/internal/compactor"
	"github.com/masonwheeler/observability-platform/internal/observability"
	"github.com/masonwheeler/observability-platform/internal/storage/block"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

type countingFlusher struct{ flushes int }

func (f *countingFlusher) FlushBlock() (bool, error) { f.flushes++; return true, nil }
func (f *countingFlusher) FlushBlockContext(context.Context) (bool, error) {
	return f.FlushBlock()
}
func (f *countingFlusher) SealedChunkCount() int { return 0 }

type countingBlocks struct{ compacts, retentions int }

func (b *countingBlocks) CompactOnce(func([]block.BlockInfo) [][]string) (int, error) {
	b.compacts++
	return 0, nil
}

func (b *countingBlocks) ApplyRetention(time.Time, time.Duration) (int, error) {
	b.retentions++
	return 0, nil
}

func quietMetrics() *observability.Metrics {
	_, inst := observability.NewRegistry(observability.RegistryOptions{Cardinality: fakeCard{}})
	return inst.Maintenance
}

// The ingester runs the loop with a flusher and nothing else.
func TestCompactor_NilBlockManagerOnlyFlushes(t *testing.T) {
	f := &countingFlusher{}
	c := compactor.New(f, nil, nil, time.Now, testConfig(), quietMetrics(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	c.RunOnce(context.Background())
	if f.flushes != 1 {
		t.Fatalf("flushes = %d, want 1", f.flushes)
	}
}

// The compactor target runs it with a block manager and nothing else — and
// must not attempt a final flush on shutdown.
func TestCompactor_NilFlusherOnlyCompactsAndRetains(t *testing.T) {
	b := &countingBlocks{}
	c := compactor.New(nil, b, nil, time.Now, testConfig(), quietMetrics(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	c.RunOnce(context.Background())
	if b.compacts != 1 || b.retentions != 1 {
		t.Fatalf("compacts=%d retentions=%d, want 1 and 1", b.compacts, b.retentions)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c.Run(ctx) // returns at once; a nil flusher must not be called
}

type fakeCard struct{}

func (fakeCard) Cardinality() (int, int, int) { return 0, 0, 0 }

// The ingester drains its whole head on the way out instead, so its loop skips
// the final flush: one bounded drain, not a flush plus a drain stacking up
// against the pod's grace period.
func TestCompactor_SkipFinalFlush(t *testing.T) {
	for _, skip := range []bool{false, true} {
		f := &countingFlusher{}
		cfg := testConfig()
		cfg.SkipFinalFlush = skip
		c := compactor.New(f, nil, nil, time.Now, cfg, quietMetrics(), slog.New(slog.NewTextHandler(io.Discard, nil)))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		c.Run(ctx)
		if want := map[bool]int{false: 1, true: 0}[skip]; f.flushes != want {
			t.Errorf("SkipFinalFlush=%v: final flushes = %d, want %d", skip, f.flushes, want)
		}
	}
}

// stallingFlusher's flush runs until its context is done, as a flush to a
// store that has stopped answering does.
type stallingFlusher struct{ started chan struct{} }

func (f *stallingFlusher) FlushBlock() (bool, error) { return false, nil }
func (f *stallingFlusher) FlushBlockContext(ctx context.Context) (bool, error) {
	close(f.started)
	<-ctx.Done()
	return false, ctx.Err()
}
func (f *stallingFlusher) SealedChunkCount() int { return 0 }

// Shutdown cancels a maintenance flush in progress: the loop must not hold
// the ingester's bounded drain back behind a flush to a stalled store, and a
// cancelled flush is not counted or logged as a failure.
func TestCompactor_RunCancelsAFlushInProgress(t *testing.T) {
	f := &stallingFlusher{started: make(chan struct{})}
	cfg := testConfig()
	cfg.MaintenanceInterval = 5 * time.Millisecond
	cfg.SkipFinalFlush = true
	mx := quietMetrics()
	c := compactor.New(f, nil, nil, time.Now, cfg, mx, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.Run(ctx); close(done) }()
	<-f.started
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after shutdown cancelled its flush")
	}
	if v := testutil.ToFloat64(mx.FlushFailuresTotal); v != 0 {
		t.Errorf("flush failures = %v after a cancelled flush, want 0", v)
	}
}
