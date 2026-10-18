package metrics_test

import (
	"context"
	"errors"
	"github.com/masonwheeler/observability-platform/internal/tenant"
	"path/filepath"
	"testing"
	"time"

	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/storage/block"
)

func exportStore(t *testing.T) (*metrics.BlockStore, string) {
	t.Helper()
	dir := t.TempDir()
	bs, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	a := map[string]string{"__name__": "a"}
	b := map[string]string{"__name__": "b"}
	c := map[string]string{"__name__": "c"}
	flushLabeledBlock(t, bs, 0, a, b)
	flushLabeledBlock(t, bs, 1_000_000, a, c)
	return bs, dir
}

type sampleKey struct {
	id  metrics.SeriesID
	ts  int64
	gen int64
}

func TestExportSeriesCoversEverySeriesAndSample(t *testing.T) {
	for _, limit := range []int{1 << 20, 1} {
		bs, dir := exportStore(t)
		stored := storedSamples(t, dir, bs)
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
						k := sampleKey{sc.ID, ts, it.Gen()}
						got[k] = v
						if want, ok := stored[k]; !ok || want != v {
							t.Errorf("limit %d: exported %+v=%v not in store (stored %v)", limit, k, v, want)
						}
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
		if len(got) != len(stored) {
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
	bs, _ := exportStore(t)
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

// storedSamples decodes every block's chunks straight from disk, independent of
// ExportSeries, returning every stored (series, ts, gen) -> value.
func storedSamples(t *testing.T, dir string, bs *metrics.BlockStore) map[sampleKey]float64 {
	t.Helper()
	out := map[sampleKey]float64{}
	for _, info := range bs.BlockInfos() {
		r, err := block.OpenReader(filepath.Join(dir, "metrics", "blocks", info.ID))
		if err != nil {
			t.Fatalf("open block: %v", err)
		}
		for _, se := range r.Series() {
			for _, ref := range se.Chunks {
				c, err := r.ReadChunk(ref)
				if err != nil {
					t.Fatal(err)
				}
				it := c.Iterator()
				for it.Next() {
					ts, v := it.At()
					out[sampleKey{metrics.SeriesID(se.ID), ts, it.Gen()}] = v
				}
			}
		}
		_ = r.Close()
	}
	return out
}

func exportDuringChange(t *testing.T, mutate func(t *testing.T, bs *metrics.BlockStore, infos []block.BlockInfo)) {
	t.Helper()
	dir := t.TempDir()
	bs, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	flushLabeledBlock(t, bs, 1_000_000_000_000, map[string]string{"__name__": "x"})
	flushLabeledBlock(t, bs, 1_100_000_000_000, map[string]string{"__name__": "y"})
	flushLabeledBlock(t, bs, 1_200_000_000_000, map[string]string{"__name__": "z"})
	infos := bs.BlockInfos()
	if len(infos) != 3 {
		t.Fatalf("setup blocks = %d", len(infos))
	}
	// The first block exported is infos[0]; mutate once, after it was sent.
	var before map[sampleKey]float64
	done := false
	got := map[sampleKey]float64{}
	_, err = bs.ExportSeries(context.Background(), 1<<20, func(batch []metrics.SeriesChunks) error {
		if !done {
			done = true
			before = storedSamples(t, dir, bs)
			mutate(t, bs, infos)
		}
		for _, sc := range batch {
			for _, c := range sc.Chunks {
				it := c.Iterator()
				for it.Next() {
					ts, v := it.At()
					got[sampleKey{sc.ID, ts, it.Gen()}] = v
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ExportSeries: %v", err)
	}
	after := storedSamples(t, dir, bs)
	// Everything stored at the end, and everything stored at the start that
	// survives (retention may delete the first block, already exported), must
	// have been exported.
	for k, v := range after {
		if got[k] != v {
			t.Errorf("stored sample %+v=%v missing from export", k, v)
		}
	}
	for k, v := range before {
		if got[k] != v {
			t.Errorf("sample %+v=%v present at export start missing from export", k, v)
		}
	}
}

func TestExportSeriesSurvivesCompactionDuringExport(t *testing.T) {
	exportDuringChange(t, func(t *testing.T, bs *metrics.BlockStore, infos []block.BlockInfo) {
		// Merge the exported first block with the second: the merged block lands
		// at the end and the untouched third shifts to position 0.
		ids := []string{infos[0].ID, infos[1].ID}
		if n, err := bs.CompactOnce(func([]block.BlockInfo) [][]string { return [][]string{ids} }); err != nil || n != 1 {
			t.Fatalf("CompactOnce = %d, %v", n, err)
		}
	})
}

func TestExportSeriesSurvivesRetentionDuringExport(t *testing.T) {
	exportDuringChange(t, func(t *testing.T, bs *metrics.BlockStore, infos []block.BlockInfo) {
		// Expire only the (already exported) first block.
		now := time.UnixMilli(1_150_000_000_000)
		if n, err := bs.ApplyRetention(now, tenant.RetentionPolicy{Default: 100_000_000 * time.Second}); err != nil || n != 1 {
			t.Fatalf("ApplyRetention = %d, %v", n, err)
		}
	})
}

func TestExportSeriesSeesBlockFlushedDuringExport(t *testing.T) {
	exportDuringChange(t, func(t *testing.T, bs *metrics.BlockStore, infos []block.BlockInfo) {
		flushLabeledBlock(t, bs, 1_300_000_000_000, map[string]string{"__name__": "w"})
	})
}
