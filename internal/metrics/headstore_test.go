package metrics_test

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/storage/block"
)

// recordingSink forwards to a real BlockStore unless told to fail, and records
// what it saw.
type recordingSink struct {
	mu      sync.Mutex
	target  *metrics.BlockStore
	calls   int
	failOn  map[int]bool // 1-based call numbers that fail
	onCall  func(series []metrics.SeriesChunks)
	batches [][]metrics.SeriesChunks
}

func (s *recordingSink) IngestSeriesChunks(ctx context.Context, series []metrics.SeriesChunks) (block.Meta, error) {
	s.mu.Lock()
	s.calls++
	n := s.calls
	s.batches = append(s.batches, series)
	s.mu.Unlock()
	if s.onCall != nil {
		s.onCall(series)
	}
	if s.failOn[n] {
		return block.Meta{}, errors.New("simulated store outage")
	}
	return s.target.IngestSeriesChunks(ctx, series)
}

func newSinkTarget(t *testing.T) *metrics.BlockStore {
	t.Helper()
	bs, err := metrics.NewBlockStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	return bs
}

func appendSeries(t *testing.T, h *metrics.HeadStore, name string, n int) metrics.Labels {
	t.Helper()
	l, _ := metrics.NewLabels(map[string]string{"__name__": name})
	for i := range n {
		if err := h.Append(l, int64(i)*1000, float64(i)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	return l
}

func TestHeadStore_FlushSendsSealedChunksAndDropsThem(t *testing.T) {
	dataDir := t.TempDir()
	target := newSinkTarget(t)
	h, err := metrics.OpenHeadStore(dataDir, &recordingSink{target: target}, metrics.HeadStoreOptions{})
	if err != nil {
		t.Fatalf("OpenHeadStore: %v", err)
	}
	l := appendSeries(t, h, "flushed", 121) // one sealed chunk plus an open one

	wrote, err := h.FlushBlock()
	if err != nil || !wrote {
		t.Fatalf("FlushBlock = %v, %v", wrote, err)
	}
	if n := h.SealedChunkCount(); n != 0 {
		t.Fatalf("SealedChunkCount after flush = %d, want 0", n)
	}
	if got, _ := target.QueryRange(metrics.SeriesID(l.Hash()), 0, 119_000); len(got) != 120 {
		t.Fatalf("store holds %d samples, want the 120 sealed ones", len(got))
	}
	if got, _ := h.QueryRange(metrics.SeriesID(l.Hash()), 0, 200_000); len(got) != 1 {
		t.Fatalf("head holds %d samples, want only the open chunk's one", len(got))
	}
}

func TestHeadStore_PersistsTheFloorBeforeSending(t *testing.T) {
	dataDir := t.TempDir()
	var floorSeen int64
	var maxGenSent int64
	sink := &recordingSink{target: newSinkTarget(t), onCall: func(series []metrics.SeriesChunks) {
		raw, err := os.ReadFile(metrics.GenFloorPath(dataDir))
		if err != nil {
			t.Errorf("no generation floor on disk when the sink was called: %v", err)
			return
		}
		floorSeen, _ = strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
		for _, sc := range series {
			for _, c := range sc.Chunks {
				it := c.Iterator()
				for it.Next() {
					if g := it.Gen(); g > maxGenSent {
						maxGenSent = g
					}
				}
			}
		}
	}}
	h, err := metrics.OpenHeadStore(dataDir, sink, metrics.HeadStoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	appendSeries(t, h, "floor", 121)
	if _, err := h.FlushBlock(); err != nil {
		t.Fatal(err)
	}
	if floorSeen <= maxGenSent {
		t.Fatalf("floor on disk %d is not above the highest generation sent %d", floorSeen, maxGenSent)
	}
}

func TestHeadStore_FloorSurvivesRestart(t *testing.T) {
	dataDir := t.TempDir()
	target := newSinkTarget(t)
	h, err := metrics.OpenHeadStore(dataDir, &recordingSink{target: target}, metrics.HeadStoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	appendSeries(t, h, "restart", 121)
	if _, err := h.FlushBlock(); err != nil {
		t.Fatal(err)
	}

	// A new head on the same directory must hand out generations above every
	// generation the store now holds, or a post-restart overwrite at an old
	// timestamp would lose to the stale value.
	h2, err := metrics.OpenHeadStore(dataDir, &recordingSink{target: target}, metrics.HeadStoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	l, _ := metrics.NewLabels(map[string]string{"__name__": "restart"})
	if err := h2.Append(l, 0, 999); err != nil { // overwrite the flushed sample at ts 0
		t.Fatal(err)
	}
	id := metrics.SeriesID(l.Hash())
	headSample, ok, err := h2.QueryInstant(id, 0)
	if err != nil || !ok {
		t.Fatalf("QueryInstant: %v, %v", ok, err)
	}
	stored, err := target.QueryRange(id, 0, 0)
	if err != nil || len(stored) != 1 {
		t.Fatalf("store sample at 0: %v, %v", stored, err)
	}
	if headSample.Gen <= stored[0].Gen {
		t.Fatalf("post-restart generation %d does not outrank the stored %d", headSample.Gen, stored[0].Gen)
	}
}

func TestHeadStore_RejectsACorruptFloor(t *testing.T) {
	dataDir := t.TempDir()
	if err := os.MkdirAll(dataDir+"/metrics", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(metrics.GenFloorPath(dataDir), []byte("not-a-number\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := metrics.OpenHeadStore(dataDir, &recordingSink{target: newSinkTarget(t)}, metrics.HeadStoreOptions{})
	if err == nil || !strings.Contains(err.Error(), "genfloor") {
		t.Fatalf("OpenHeadStore with a corrupt floor = %v, want an error naming the file", err)
	}
}

func TestHeadStore_FailedFlushKeepsTheHead(t *testing.T) {
	h, err := metrics.OpenHeadStore(t.TempDir(), &recordingSink{target: newSinkTarget(t), failOn: map[int]bool{1: true}}, metrics.HeadStoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	appendSeries(t, h, "outage", 121)
	if _, err := h.FlushBlock(); err == nil {
		t.Fatal("FlushBlock with a failing sink returned nil")
	}
	if n := h.SealedChunkCount(); n != 1 {
		t.Fatalf("SealedChunkCount after a failed flush = %d, want 1 (nothing may be dropped)", n)
	}
}

func TestHeadStore_BatchesAndKeepsWhatAFailedBatchHeld(t *testing.T) {
	sink := &recordingSink{target: newSinkTarget(t), failOn: map[int]bool{2: true}}
	// A batch limit of one byte puts every chunk in its own request.
	h, err := metrics.OpenHeadStore(t.TempDir(), sink, metrics.HeadStoreOptions{BatchBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"b1", "b2", "b3"} {
		appendSeries(t, h, name, 120) // one sealed chunk each
	}
	if _, err := h.FlushBlock(); err == nil {
		t.Fatal("FlushBlock returned nil although the second request failed")
	}
	if sink.calls != 2 {
		t.Fatalf("sink calls = %d, want 2: the flush stops at the first failure", sink.calls)
	}
	if n := h.SealedChunkCount(); n != 2 {
		t.Fatalf("SealedChunkCount = %d, want 2: the acknowledged batch is dropped, the rest kept", n)
	}
	if _, err := h.FlushBlock(); err != nil {
		t.Fatalf("retry FlushBlock: %v", err)
	}
	if n := h.SealedChunkCount(); n != 0 {
		t.Fatalf("SealedChunkCount after retry = %d, want 0", n)
	}
}

func TestHeadStore_DropsSeriesWhoseHeadEmptied(t *testing.T) {
	h, err := metrics.OpenHeadStore(t.TempDir(), &recordingSink{target: newSinkTarget(t)}, metrics.HeadStoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	appendSeries(t, h, "drained", 120) // exactly one chunk, sealed: nothing stays open
	if _, err := h.FlushBlock(); err != nil {
		t.Fatal(err)
	}
	if vals := h.LabelValues("__name__"); len(vals) != 0 {
		t.Fatalf("head still indexes %v after its only chunk was flushed", vals)
	}
}
