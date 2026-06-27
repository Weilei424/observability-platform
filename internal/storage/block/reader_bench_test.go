package block

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/storage/chunk"
	"github.com/masonwheeler/observability-platform/internal/storage/index"
)

// benchSealedChunk returns a chunk filled to the seal threshold.
func benchSealedChunk() *chunk.Chunk {
	c := chunk.NewChunk()
	for i := int64(0); i < 120; i++ {
		_ = c.Append(1000+i, float64(i), i)
	}
	return c
}

// buildBenchBlock writes a block of nSeries (job label has 20 distinct values)
// and returns an opened Reader.
func buildBenchBlock(b *testing.B, nSeries int) *Reader {
	b.Helper()
	root := b.TempDir()
	blocksDir := filepath.Join(root, "blocks")
	tmpDir := filepath.Join(root, "tmp")
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		b.Fatal(err)
	}
	w, err := NewWriter(blocksDir, tmpDir)
	if err != nil {
		b.Fatalf("NewWriter: %v", err)
	}
	for i := 0; i < nSeries; i++ {
		labels := []LabelPair{
			{"__name__", "http_requests_total"},
			{"job", fmt.Sprintf("job-%d", i%20)},
			{"instance", fmt.Sprintf("inst-%d", i)},
		}
		if err := w.AddSeries(uint64(i+1), labels, []*chunk.Chunk{benchSealedChunk()}); err != nil {
			b.Fatalf("AddSeries: %v", err)
		}
	}
	meta, err := w.Commit()
	if err != nil {
		b.Fatalf("Commit: %v", err)
	}
	r, err := OpenReader(filepath.Join(blocksDir, meta.BlockID))
	if err != nil {
		b.Fatalf("OpenReader: %v", err)
	}
	return r
}

// BenchmarkReaderResolve_ByID measures resolving a postings match set via the
// O(1) ID index — the path BlockStore now uses.
func BenchmarkReaderResolve_ByID(b *testing.B) {
	r := buildBenchBlock(b, 10000)
	defer r.Close()
	matchers := []index.Pair{{Name: "job", Value: "job-7"}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ids, err := r.Postings(matchers)
		if err != nil {
			b.Fatalf("Postings: %v", err)
		}
		var n int
		for _, id := range ids {
			if _, ok := r.SeriesByID(id); ok {
				n++
			}
		}
		_ = n
	}
}

// BenchmarkReaderResolve_FullScan is the pre-fix baseline: resolve the same
// postings match set by scanning every series in the block. Comparing the two
// shows the persisted-query improvement on the persisted path itself, which the
// memory-only index benchmark could not demonstrate.
func BenchmarkReaderResolve_FullScan(b *testing.B) {
	r := buildBenchBlock(b, 10000)
	defer r.Close()
	matchers := []index.Pair{{Name: "job", Value: "job-7"}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ids, err := r.Postings(matchers)
		if err != nil {
			b.Fatalf("Postings: %v", err)
		}
		want := make(map[uint64]struct{}, len(ids))
		for _, id := range ids {
			want[id] = struct{}{}
		}
		var n int
		for _, se := range r.Series() {
			if _, ok := want[se.ID]; ok {
				n++
			}
		}
		_ = n
	}
}
