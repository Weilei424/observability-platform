package metrics

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
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

// TestBatchSeriesChunks_BatchesStayWithinLimitAsEncoded pins that batches are
// sized by wire cost, not raw chunk bytes. Many sparse series each seal one
// tiny two-sample chunk; base64 (x4/3), long label values, and framing make
// the real request several times the raw byte count. Every batch's actual
// encoded body (the rpc wire shape, HTML escaping off) must stay within limit,
// except a lone series-open that is itself oversized.
func TestBatchSeriesChunks_BatchesStayWithinLimitAsEncoded(t *testing.T) {
	type wireSeries struct {
		Labels map[string]string `json:"labels"`
		Chunks [][]byte          `json:"chunks"`
	}
	type wireReq struct {
		Series []wireSeries `json:"series"`
	}
	const limit = 8 << 10
	long := strings.Repeat("v", 300) + "\"\n<é"

	var series []SeriesChunks
	for i := 0; i < 200; i++ {
		l, err := NewLabels(map[string]string{
			"__name__": "sparse_metric",
			"instance": fmt.Sprintf("%s-%d", long, i),
			"job":      long,
		})
		if err != nil {
			t.Fatal(err)
		}
		c := chunk.NewChunk()
		for j := 0; j < 2; j++ {
			if err := c.Append(int64(j)*3600_000, float64(j), int64(j+1)); err != nil {
				t.Fatal(err)
			}
		}
		series = append(series, SeriesChunks{ID: SeriesID(l.Hash()), Labels: l, Chunks: []*chunk.Chunk{c}})
	}

	batches := batchSeriesChunks(series, limit, 1000)
	if len(batches) < 2 {
		t.Fatalf("got %d batch(es), want the limit to force several", len(batches))
	}
	total := 0
	for bi, batch := range batches {
		req := wireReq{}
		for _, sc := range batch {
			ws := wireSeries{Labels: sc.Labels.Map()}
			for _, c := range sc.Chunks {
				ws.Chunks = append(ws.Chunks, c.Bytes())
			}
			req.Series = append(req.Series, ws)
			total += len(sc.Chunks)
		}
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(req); err != nil {
			t.Fatal(err)
		}
		if buf.Len() > limit && len(batch) > 1 {
			t.Fatalf("batch %d encodes to %d bytes with %d series, want <= %d", bi, buf.Len(), len(batch), limit)
		}
	}
	if total != len(series) {
		t.Fatalf("total chunks = %d, want %d", total, len(series))
	}
}

// TestJSONStringBytesMatchesEncodingJSON pins the copy of internal/logs's
// jsonStringBytes to the bytes encoding/json actually emits (HTML escaping off).
func TestJSONStringBytesMatchesEncodingJSON(t *testing.T) {
	all := make([]byte, 0x20)
	for i := range all {
		all[i] = byte(i)
	}
	for _, s := range []string{
		"", "hello 123", string(all), `a "q" \ b`, "<script>a&b</script>",
		"café", "日本語", "x \U0001F600 y", "\u2028", "\u2029", "a\u2028b\u2029c",
		strings.Repeat("\x00\x01\"\\", 5000),
	} {
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(s); err != nil {
			t.Fatal(err)
		}
		want := len(bytes.TrimSuffix(buf.Bytes(), []byte("\n")))
		if got := jsonStringBytes(s); got != want {
			t.Fatalf("jsonStringBytes(%q) = %d, want %d", s, got, want)
		}
	}
}
