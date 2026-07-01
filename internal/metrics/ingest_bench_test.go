package metrics_test

import (
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/masonwheeler/observability-platform/internal/compactor"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/storage/block"
	"github.com/masonwheeler/observability-platform/internal/storage/wal"
)

// benchLabels builds n deterministic series across one metric. instance has 50
// distinct values (so a {instance="inst-7"} selector matches n/50 series) and
// series is unique per i. Shared by the ingestion and query benchmarks.
func benchLabels(tb testing.TB, n int) []metrics.Labels {
	tb.Helper()
	out := make([]metrics.Labels, n)
	for i := 0; i < n; i++ {
		l, err := metrics.NewLabels(map[string]string{
			"__name__": "bench_http_requests_total",
			"job":      "bench",
			"instance": fmt.Sprintf("inst-%d", i%50),
			"series":   fmt.Sprintf("s-%d", i),
		})
		if err != nil {
			tb.Fatalf("NewLabels: %v", err)
		}
		out[i] = l
	}
	return out
}

// BenchmarkIngest_MemoryStore measures the pure chunk-encode append path with no
// WAL, round-robining across 1000 series. Reports samples/sec.
func BenchmarkIngest_MemoryStore(b *testing.B) {
	ms := metrics.NewMemoryStore()
	ls := benchLabels(b, 1000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l := ls[i%len(ls)]
		if err := ms.Append(l, int64(i)*1000, float64(i)); err != nil {
			b.Fatalf("Append: %v", err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "samples/sec")
}

// newWALStore builds a WALStore over a fresh temp dir with the given fsync policy.
func newWALStore(b *testing.B, syncEveryN int) (*metrics.WALStore, func()) {
	b.Helper()
	dir := b.TempDir()
	bs, err := metrics.NewBlockStore(dir)
	if err != nil {
		b.Fatalf("NewBlockStore: %v", err)
	}
	w, err := wal.Open(filepath.Join(dir, "metrics", "wal"), 128<<20, syncEveryN)
	if err != nil {
		b.Fatalf("wal.Open: %v", err)
	}
	store := metrics.NewWALStore(w, bs, dir)
	return store, func() { _ = w.Close(); _ = bs.Close() }
}

// BenchmarkIngest_WALStore_Sync1 measures the durable path with fsync on every
// record (wal_sync_every_n=1) — the cost of durability versus memory-only.
func BenchmarkIngest_WALStore_Sync1(b *testing.B) {
	store, cleanup := newWALStore(b, 1)
	defer cleanup()
	ls := benchLabels(b, 1000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l := ls[i%len(ls)]
		if err := store.Append(l, int64(i)*1000, float64(i)); err != nil {
			b.Fatalf("Append: %v", err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "samples/sec")
}

// BenchmarkIngest_WALStore_SyncSweep shows how throughput scales as fsync is
// batched (sync every N records).
func BenchmarkIngest_WALStore_SyncSweep(b *testing.B) {
	for _, n := range []int{1, 16, 128} {
		b.Run(fmt.Sprintf("syncEveryN=%d", n), func(b *testing.B) {
			store, cleanup := newWALStore(b, n)
			defer cleanup()
			ls := benchLabels(b, 1000)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				l := ls[i%len(ls)]
				if err := store.Append(l, int64(i)*1000, float64(i)); err != nil {
					b.Fatalf("Append: %v", err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "samples/sec")
		})
	}
}

// BenchmarkIngest_CompactionOnOff compares ingest throughput with a background
// flush+compact loop running versus a quiet baseline. APPROXIMATE: the
// background goroutine introduces scheduling noise; the number is indicative,
// not exact.
//
// Samples advance the timestamp by 1ms/append (int64(i)) so a flushed block spans
// only minutes — well under the 2h base compaction range — and successive blocks
// share the same range window, keeping them eligible to merge. The "compaction=on"
// case reports a "compactions" metric so the result proves compaction actually ran
// rather than measuring bare periodic flushing.
func BenchmarkIngest_CompactionOnOff(b *testing.B) {
	ranges := compactor.Ranges((2 * time.Hour).Milliseconds(), 4, 3)
	plan := func(infos []block.BlockInfo) [][]string {
		return compactor.Plan(infos, ranges)
	}
	for _, withCompaction := range []bool{false, true} {
		name := "compaction=off"
		if withCompaction {
			name = "compaction=on"
		}
		b.Run(name, func(b *testing.B) {
			dir := b.TempDir()
			bs, err := metrics.NewBlockStore(dir)
			if err != nil {
				b.Fatalf("NewBlockStore: %v", err)
			}
			defer bs.Close()
			ls := benchLabels(b, 1000)

			var stop chan struct{}
			var wg sync.WaitGroup
			var compactions int64
			if withCompaction {
				stop = make(chan struct{})
				wg.Add(1)
				go func() {
					defer wg.Done()
					t := time.NewTicker(50 * time.Millisecond)
					defer t.Stop()
					for {
						select {
						case <-stop:
							return
						case <-t.C:
							if _, err := bs.FlushBlock(); err != nil {
								b.Errorf("FlushBlock: %v", err)
								return
							}
							n, err := bs.CompactOnce(plan)
							if err != nil {
								b.Errorf("CompactOnce: %v", err)
								return
							}
							atomic.AddInt64(&compactions, int64(n))
						}
					}
				}()
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				l := ls[i%len(ls)]
				if err := bs.Append(l, int64(i), float64(i)); err != nil {
					b.Fatalf("Append: %v", err)
				}
			}
			b.StopTimer()
			if withCompaction {
				close(stop)
				wg.Wait()
			}
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "samples/sec")
			if withCompaction {
				b.ReportMetric(float64(atomic.LoadInt64(&compactions)), "compactions")
			}
		})
	}
}
