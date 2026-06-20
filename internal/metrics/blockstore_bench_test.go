package metrics_test

import (
	"fmt"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/metrics"
)

// buildFlushedBlockStore creates nSeries (each with one sealed chunk), flushes
// them into an on-disk block, then reopens the store from the same data dir.
// Reopening matters: DiscardSealedChunks leaves the flushed series in the head
// index, so without a fresh open SelectSeries would still resolve them from
// memory and the benchmark would not measure the persisted path. After reopen
// the memory store is empty and every series lives only in the block, isolating
// the postings + ID-index resolution path.
func buildFlushedBlockStore(b *testing.B, nSeries int) *metrics.BlockStore {
	dataDir := b.TempDir()
	bs, err := metrics.NewBlockStore(dataDir)
	if err != nil {
		b.Fatalf("NewBlockStore: %v", err)
	}
	for i := 0; i < nSeries; i++ {
		l, err := metrics.NewLabels(map[string]string{
			"__name__": "http_requests_total",
			"job":      fmt.Sprintf("job-%d", i%20),
			"instance": fmt.Sprintf("inst-%d", i),
		})
		if err != nil {
			b.Fatalf("NewLabels: %v", err)
		}
		// 120 samples fills and seals exactly one chunk so FlushBlock drains it.
		for ts := 0; ts < 120; ts++ {
			if err := bs.Append(l, int64(ts*1000), float64(ts)); err != nil {
				b.Fatalf("Append: %v", err)
			}
		}
	}
	if ok, err := bs.FlushBlock(); err != nil || !ok {
		b.Fatalf("FlushBlock: ok=%v err=%v", ok, err)
	}
	if err := bs.Close(); err != nil {
		b.Fatalf("Close: %v", err)
	}
	reopened, err := metrics.NewBlockStore(dataDir)
	if err != nil {
		b.Fatalf("reopen NewBlockStore: %v", err)
	}
	return reopened
}

// BenchmarkBlockStoreSelectSeries_Persisted measures the persisted-query path
// (postings lookup + per-ID resolution via the block's ID index). With the ID
// index in place, resolving a small match set no longer scans every series in
// the block, so the cost tracks the match-set size rather than total cardinality.
func BenchmarkBlockStoreSelectSeries_Persisted(b *testing.B) {
	bs := buildFlushedBlockStore(b, 10000)
	defer bs.Close()
	sel := metrics.Selector{
		MetricName: "http_requests_total",
		Matchers:   []metrics.Matcher{{Name: "job", Value: "job-7"}},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = bs.SelectSeries(sel)
	}
}
