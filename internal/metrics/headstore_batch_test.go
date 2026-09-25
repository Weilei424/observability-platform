package metrics

import (
	"testing"

	"github.com/masonwheeler/observability-platform/internal/storage/chunk"
)

// TestBatchSeriesChunks_SplitsALongSeriesByPerSeriesCap pins the per-series
// chunk cap batchSeriesChunks enforces on top of its byte-based split:
// block.OpenReader refuses a block whose index declares more than
// block.MaxChunksPerSeries chunks for any one series, and it only discovers
// that after the store has written and fsynced the block. With a small cap and
// a byte limit generous enough to hold every chunk in one request, a long
// series must still split across batches purely on chunk count: no batch may
// hold more than the cap for any series, no batch may hold a series ID twice,
// and every chunk must appear exactly once, in order.
func TestBatchSeriesChunks_SplitsALongSeriesByPerSeriesCap(t *testing.T) {
	const perSeriesCap = 3
	const numChunks = 10 // not a multiple of perSeriesCap, to exercise a partial final batch

	l, err := NewLabels(map[string]string{"__name__": "long"})
	if err != nil {
		t.Fatal(err)
	}
	id := SeriesID(l.Hash())

	chunks := make([]*chunk.Chunk, numChunks)
	for i := range chunks {
		c := chunk.NewChunk()
		if err := c.Append(int64(i)*1000, float64(i), int64(i+1)); err != nil {
			t.Fatalf("chunk %d Append: %v", i, err)
		}
		chunks[i] = c
	}
	series := []SeriesChunks{{ID: id, Labels: l, Chunks: chunks}}

	// A byte limit far larger than the encoded size of all ten chunks together,
	// so only the per-series cap can be forcing the split.
	batches := batchSeriesChunks(series, 1<<20, perSeriesCap)

	if len(batches) < 2 {
		t.Fatalf("got %d batch(es), want at least 2 to hold %d chunks under a cap of %d", len(batches), numChunks, perSeriesCap)
	}

	var seenChunks []*chunk.Chunk
	for bi, batch := range batches {
		seenIDs := make(map[SeriesID]bool)
		for _, sc := range batch {
			if seenIDs[sc.ID] {
				t.Fatalf("batch %d holds series %d twice", bi, sc.ID)
			}
			seenIDs[sc.ID] = true
			if len(sc.Chunks) > perSeriesCap {
				t.Fatalf("batch %d holds %d chunks for series %d, want <= %d", bi, len(sc.Chunks), sc.ID, perSeriesCap)
			}
			if len(sc.Chunks) == 0 {
				t.Fatalf("batch %d holds an empty entry for series %d", bi, sc.ID)
			}
			seenChunks = append(seenChunks, sc.Chunks...)
		}
	}

	if len(seenChunks) != numChunks {
		t.Fatalf("total chunks across batches = %d, want %d", len(seenChunks), numChunks)
	}
	for i, c := range seenChunks {
		if c != chunks[i] {
			t.Fatalf("chunk at position %d is not the original chunk %d in order (each chunk must appear exactly once, in order)", i, i)
		}
	}
}
