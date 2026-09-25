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
)

type countingFlusher struct{ flushes int }

func (f *countingFlusher) FlushBlock() (bool, error) { f.flushes++; return true, nil }
func (f *countingFlusher) SealedChunkCount() int     { return 0 }

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
