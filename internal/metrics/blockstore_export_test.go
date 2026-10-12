package metrics_test

import (
	"context"
	"errors"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/metrics"
)

func exportStore(t *testing.T) *metrics.BlockStore {
	t.Helper()
	bs, err := metrics.NewBlockStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	a := map[string]string{"__name__": "a"}
	b := map[string]string{"__name__": "b"}
	c := map[string]string{"__name__": "c"}
	flushLabeledBlock(t, bs, 0, a, b)
	flushLabeledBlock(t, bs, 1_000_000, a, c)
	return bs
}

type sampleKey struct {
	id  metrics.SeriesID
	ts  int64
	gen int64
}

func TestExportSeriesCoversEverySeriesAndSample(t *testing.T) {
	for _, limit := range []int{1 << 20, 1} {
		bs := exportStore(t)
		got := map[sampleKey]float64{}
		seriesInBatch := 0
		batches := 0
		perSeriesBatches := map[metrics.SeriesID]int{}
		n, err := bs.ExportSeries(context.Background(), limit, func(batch []metrics.SeriesChunks) error {
			batches++
			seen := map[metrics.SeriesID]bool{}
			for _, sc := range batch {
				if seen[sc.ID] {
					t.Errorf("series %d twice in one batch", sc.ID)
				}
				seen[sc.ID] = true
				seriesInBatch++
				for _, c := range sc.Chunks {
					it := c.Iterator()
					for it.Next() {
						ts, v := it.At()
						got[sampleKey{sc.ID, ts, it.Gen()}] = v
					}
				}
			}
			for id := range seen {
				perSeriesBatches[id]++
			}
			return nil
		})
		if err != nil {
			t.Fatalf("limit %d: ExportSeries: %v", limit, err)
		}
		if n != 3 {
			t.Errorf("limit %d: distinct series = %d, want 3", limit, n)
		}
		if len(got) != 4*120 {
			t.Errorf("limit %d: distinct samples = %d, want %d", limit, len(got), 4*120)
		}
		if limit == 1<<20 && batches != 2 {
			t.Errorf("batches = %d, want 2 (one per block)", batches)
		}
		if limit == 1 && batches < 4 {
			t.Errorf("tiny limit: batches = %d, want one per chunk (>=4)", batches)
		}
	}
}

func TestExportSeriesStopsOnFnErrorAndCancel(t *testing.T) {
	bs := exportStore(t)
	boom := errors.New("boom")
	if _, err := bs.ExportSeries(context.Background(), 1<<20, func([]metrics.SeriesChunks) error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	_, err := bs.ExportSeries(ctx, 1<<20, func([]metrics.SeriesChunks) error { calls++; cancel(); return nil })
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("err = %v calls = %d, want Canceled after 1 call", err, calls)
	}
}
