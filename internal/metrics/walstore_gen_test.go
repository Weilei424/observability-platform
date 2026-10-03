package metrics_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/storage/wal"
)

// openWALStore opens a BlockStore-backed WALStore in dir with a fixed clock.
func openWALStore(t *testing.T, dir string, now func() int64) (*metrics.WALStore, *metrics.BlockStore, *wal.WAL) {
	t.Helper()
	bs, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	bs.MemStore().SetGenerationClock(now)
	w, err := wal.Open(filepath.Join(dir, "metrics", "wal"), 1<<20, 1)
	if err != nil {
		t.Fatal(err)
	}
	return metrics.NewWALStore(w, bs, dir), bs, w
}

func TestWALStoreRecordsTheGenerationItAssigns(t *testing.T) {
	dir := t.TempDir()
	s, bs, w := openWALStore(t, dir, func() int64 { return 4_000_000 })
	l, _ := metrics.NewLabels(map[string]string{"__name__": "m"})
	if err := s.Append(l, 10, 1); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	_ = bs.Close()

	var gens []int64
	if err := wal.ReplayFromGen(filepath.Join(dir, "metrics", "wal"), 0,
		func(_ []wal.LabelPair, _ int64, _ float64, gen int64) { gens = append(gens, gen) }); err != nil {
		t.Fatal(err)
	}
	if len(gens) != 1 || gens[0] != 4_000_000 {
		t.Fatalf("recorded gens = %v, want [4000000]: the WAL must carry the generation memory got", gens)
	}
}

// Replay restores the exact generation even when the clock now reads far
// later: a replayed write must not outrank a newer write another ingester took.
func TestReplayRestoresExactGenerations(t *testing.T) {
	dir := t.TempDir()
	s, bs, w := openWALStore(t, dir, func() int64 { return 4_000_000 })
	l, _ := metrics.NewLabels(map[string]string{"__name__": "m"})
	_ = s.Append(l, 10, 1)
	_ = w.Close()
	_ = bs.Close()

	fresh, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	fresh.MemStore().SetGenerationClock(func() int64 { return 9_000_000_000 })
	if err := wal.ReplayFromGen(filepath.Join(dir, "metrics", "wal"), 0, func(pairs []wal.LabelPair, ts int64, v float64, gen int64) {
		lm := map[string]string{}
		for _, p := range pairs {
			lm[p.Name] = p.Value
		}
		ll, _ := metrics.NewLabels(lm)
		if err := fresh.AppendGen(ll, ts, v, gen); err != nil {
			t.Fatal(err)
		}
	}); err != nil {
		t.Fatal(err)
	}
	sds, _ := fresh.Select(context.Background(), metrics.SelectParams{Selector: metrics.Selector{MetricName: "m"}, MinT: 10, MaxT: 10})
	if len(sds) != 1 || sds[0].Samples[0].Gen != 4_000_000 {
		t.Fatalf("replayed sample = %+v, want generation 4000000", sds)
	}
}

func TestReplayRaisesFloorPastRestoredGenerations(t *testing.T) {
	bs, err := metrics.NewBlockStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	bs.MemStore().SetGenerationClock(func() int64 { return 100 }) // the clock is now behind
	l, _ := metrics.NewLabels(map[string]string{"__name__": "m"})
	_ = bs.AppendGen(l, 1, 1, 5_000_000) // a replayed generation from before the step back
	_ = bs.Append(l, 2, 2)
	sds, _ := bs.Select(context.Background(), metrics.SelectParams{Selector: metrics.Selector{MetricName: "m"}, MinT: 2, MaxT: 2})
	if g := sds[0].Samples[0].Gen; g <= 5_000_000 {
		t.Fatalf("gen after replay = %d, want above the restored 5000000", g)
	}
}
