package metrics_test

import (
	"fmt"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/metrics"
)

const benchSelector = `bench_http_requests_total{instance="inst-7"}`

// buildMemEngine builds an in-memory engine: nSeries each with samplesPerSeries
// samples at ts = t*1000.
func buildMemEngine(b *testing.B, nSeries, samplesPerSeries int) *metrics.QueryEngine {
	b.Helper()
	ms := metrics.NewMemoryStore()
	for _, l := range benchLabels(b, nSeries) {
		for t := 0; t < samplesPerSeries; t++ {
			if err := ms.Append(l, int64(t)*1000, float64(t)); err != nil {
				b.Fatalf("Append: %v", err)
			}
		}
	}
	return metrics.NewQueryEngine(ms)
}

// buildPersistedKBlocks ingests nSeries, then flushes k disjoint time windows so
// the reopened store has k persisted blocks and an empty memory head. Reopening
// matters: it drains the head index so queries resolve only from blocks.
func buildPersistedKBlocks(b *testing.B, nSeries, k int) (*metrics.QueryEngine, func()) {
	b.Helper()
	dir := b.TempDir()
	bs, err := metrics.NewBlockStore(dir)
	if err != nil {
		b.Fatalf("NewBlockStore: %v", err)
	}
	ls := benchLabels(b, nSeries)
	base := int64(0)
	for blk := 0; blk < k; blk++ {
		for _, l := range ls {
			for t := 0; t < 120; t++ { // 120 samples seals exactly one chunk
				if err := bs.Append(l, base+int64(t)*1000, float64(t)); err != nil {
					b.Fatalf("Append: %v", err)
				}
			}
		}
		if ok, err := bs.FlushBlock(); err != nil || !ok {
			b.Fatalf("FlushBlock: ok=%v err=%v", ok, err)
		}
		base += 120 * 1000
	}
	if err := bs.Close(); err != nil {
		b.Fatalf("Close: %v", err)
	}
	reopened, err := metrics.NewBlockStore(dir)
	if err != nil {
		b.Fatalf("reopen NewBlockStore: %v", err)
	}
	return metrics.NewQueryEngine(reopened), func() { _ = reopened.Close() }
}

func mustParse(b *testing.B, q string) metrics.Expr {
	b.Helper()
	expr, err := metrics.ParseExpr(q)
	if err != nil {
		b.Fatalf("ParseExpr(%q): %v", q, err)
	}
	return expr
}

// BenchmarkInstant_InMemory measures EvalInstant over head chunks.
func BenchmarkInstant_InMemory(b *testing.B) {
	eng := buildMemEngine(b, 10000, 1)
	expr := mustParse(b, benchSelector)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := eng.EvalInstant(expr, 1_000_000)
		if err != nil {
			b.Fatalf("EvalInstant: %v", err)
		}
		if len(res) == 0 {
			b.Fatal("empty result: selector matched no series")
		}
	}
}

// BenchmarkInstant_Persisted measures EvalInstant reading only from a persisted
// block (memory drained by reopen). The empty-result guard proves it is actually
// reading the block rather than silently matching nothing.
func BenchmarkInstant_Persisted(b *testing.B) {
	eng, cleanup := buildPersistedKBlocks(b, 10000, 1)
	defer cleanup()
	expr := mustParse(b, benchSelector)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := eng.EvalInstant(expr, 1_000_000)
		if err != nil {
			b.Fatalf("EvalInstant: %v", err)
		}
		if len(res) == 0 {
			b.Fatal("empty result: persisted block not read")
		}
	}
}

// BenchmarkRange_StepWidths measures EvalRange over an increasing number of ticks.
func BenchmarkRange_StepWidths(b *testing.B) {
	eng := buildMemEngine(b, 2000, 240)
	expr := mustParse(b, benchSelector)
	const start, end = int64(0), int64(239000)
	for _, ticks := range []int{60, 360, 1440} {
		b.Run(fmt.Sprintf("ticks=%d", ticks), func(b *testing.B) {
			step := (end - start) / int64(ticks-1)
			if step <= 0 {
				step = 1
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := eng.EvalRange(expr, start, end, step); err != nil {
					b.Fatalf("EvalRange: %v", err)
				}
			}
		})
	}
}

// BenchmarkInstant_BlockCount measures how instant-query cost grows with the
// number of persisted blocks the store must consult.
func BenchmarkInstant_BlockCount(b *testing.B) {
	expr := mustParse(b, benchSelector)
	for _, k := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("blocks=%d", k), func(b *testing.B) {
			eng, cleanup := buildPersistedKBlocks(b, 2000, k)
			defer cleanup()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				res, err := eng.EvalInstant(expr, int64(k)*120*1000)
				if err != nil {
					b.Fatalf("EvalInstant: %v", err)
				}
				if len(res) == 0 {
					b.Fatal("empty result")
				}
			}
		})
	}
}
