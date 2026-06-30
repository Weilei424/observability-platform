package chunk_test

import (
	"math/rand"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/storage/chunk"
)

// fillChunk writes 120 samples (one full chunk) following a value pattern.
// Generations are 1..120 (all <= chunk.MaxGeneration).
func fillChunk(c *chunk.Chunk, pattern string) {
	v := 0.0
	for i := int64(0); i < 120; i++ {
		switch pattern {
		case "counter":
			v += 1
		case "gauge":
			v += rand.NormFloat64()
		case "constant":
			v = 42.0
		}
		_ = c.Append(1000+i*1000, v, i+1)
	}
}

func BenchmarkChunk_Encode(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		c := chunk.NewChunk()
		fillChunk(c, "counter")
		_ = c.Bytes()
	}
}

func BenchmarkChunk_Decode(b *testing.B) {
	c := chunk.NewChunk()
	fillChunk(c, "counter")
	data := c.Bytes()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := chunk.FromBytes(data); err != nil {
			b.Fatalf("FromBytes: %v", err)
		}
	}
}

// BenchmarkChunk_CompressionRatio reports bytes/sample for each data pattern.
func BenchmarkChunk_CompressionRatio(b *testing.B) {
	for _, pat := range []string{"counter", "gauge", "constant"} {
		b.Run(pat, func(b *testing.B) {
			var sz, n int
			for i := 0; i < b.N; i++ {
				c := chunk.NewChunk()
				fillChunk(c, pat)
				sz = len(c.Bytes())
				n = c.NumSamples()
			}
			b.ReportMetric(float64(sz)/float64(n), "bytes/sample")
			b.ReportMetric(float64(sz), "chunk-bytes")
		})
	}
}
