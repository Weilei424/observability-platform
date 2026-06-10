package integration_test

import (
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/storage/wal"
)

// TestFlushQuery_NoVisibilityGap is a regression test for the two-part
// ordering invariant that prevents queries from returning incomplete results
// during a concurrent flush:
//
//  1. BlockStore.FlushBlock registers the block reader in bs.blocks before
//     discarding sealed chunks from memory.
//  2. BlockStore.QueryRange reads from memory before snapshotting bs.blocks.
//
// Together these guarantee: if sealed chunks are absent from memory the block
// reader is already in the list, so the subsequent block snapshot captures it.
// Without invariant 1 or 2 a query can snapshot an outdated block list and
// then read empty memory — returning zero results for data that exists.
// Running under -race additionally verifies there are no data races on shared
// state.
func TestFlushQuery_NoVisibilityGap(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "metrics", "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	bs, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}
	w, err := wal.Open(walDir, 128<<20, 1)
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	defer w.Close()

	walStore := metrics.NewWALStore(w, bs, dir)
	labels, _ := metrics.NewLabels(map[string]string{"__name__": "gap_probe"})
	id := labels.Fingerprint()

	// Ingest one sealed chunk before starting concurrent goroutines. These
	// samples (ts 0..119) must be findable at all times: either in memory as
	// sealed chunks or, after the first flush, in a block that persists for the
	// rest of the test.
	const chunkSize = 120
	for i := 0; i < chunkSize; i++ {
		if err := walStore.Append(labels, int64(i*1000), float64(i)); err != nil {
			t.Fatalf("seed Append: %v", err)
		}
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Flush goroutine: repeatedly ingest a new sealed chunk then flush. Each
	// flush cycle exercises the read-memory-before-snapshot ordering.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for cycle := 0; ; cycle++ {
			select {
			case <-stop:
				return
			default:
			}
			base := int64((cycle + 1) * chunkSize * 1000)
			for i := 0; i < chunkSize; i++ {
				walStore.Append(labels, base+int64(i*1000), float64(i))
			}
			walStore.FlushBlock()
		}
	}()

	// Query goroutines: the seeded samples must be visible at all times.
	// A zero-length result indicates a visibility gap.
	const queryWorkers = 6
	for i := 0; i < queryWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				got, err := bs.QueryRange(id, 0, math.MaxInt64)
				if err != nil {
					t.Errorf("QueryRange error: %v", err)
					return
				}
				if len(got) == 0 {
					t.Errorf("QueryRange returned 0 samples during concurrent flush — visibility gap detected")
					return
				}
			}
		}()
	}

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// TestAppendFlush_AcknowledgedSamplesSurvive is a regression test for the
// WALStore appendMu invariant: every sample for which Append returned nil must
// survive a simulated crash-restart regardless of concurrent FlushBlock calls.
//
// The invariant is enforced by holding appendMu across both WriteRecord and
// AppendTracked in Append, and holding it again during OldestHeadSegment +
// SegmentIndex sampling in FlushBlock. Without this lock FlushBlock can observe
// a state where a WAL record exists on disk but headSeg has not yet been updated
// in memory, causing it to compute a safeDelete value that covers a segment
// containing that head-chunk sample and delete it.
//
// 1 KiB WAL segments force frequent rotation, maximising the probability of
// the goroutine scheduler splitting WriteRecord from AppendTracked.
func TestAppendFlush_AcknowledgedSamplesSurvive(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "metrics", "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	bs1, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}
	w1, err := wal.Open(walDir, 1<<10, 1) // 1 KiB segments
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	store := metrics.NewWALStore(w1, bs1, dir)

	labels, _ := metrics.NewLabels(map[string]string{"__name__": "ack_probe"})

	var mu sync.Mutex
	acknowledged := make(map[int64]float64)

	var wg sync.WaitGroup
	const appendWorkers = 8
	const samplesPerWorker = 150

	for i := 0; i < appendWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < samplesPerWorker; j++ {
				ts := int64(workerID*samplesPerWorker + j)
				val := float64(ts)
				if err := store.Append(labels, ts, val); err == nil {
					mu.Lock()
					acknowledged[ts] = val
					mu.Unlock()
				}
			}
		}(i)
	}

	// Flush goroutine runs concurrently with all appends.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 40; i++ {
			store.FlushBlock()
		}
	}()

	wg.Wait()
	if err := w1.Close(); err != nil {
		t.Fatalf("Close WAL: %v", err)
	}

	// Simulate restart: load persisted blocks then replay remaining WAL segments.
	bs2, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("NewBlockStore restart: %v", err)
	}
	checkpoint := metrics.ReadCheckpoint(dir)
	replayInto(t, walDir, checkpoint, bs2)

	id := labels.Fingerprint()
	got, err := bs2.QueryRange(id, 0, math.MaxInt64)
	if err != nil {
		t.Fatalf("QueryRange after restart: %v", err)
	}

	gotMap := make(map[int64]float64, len(got))
	for _, s := range got {
		gotMap[s.TimestampMs] = s.Value
	}

	mu.Lock()
	defer mu.Unlock()
	missing := 0
	for ts, want := range acknowledged {
		if v, ok := gotMap[ts]; !ok {
			missing++
			if missing <= 5 {
				t.Errorf("acknowledged sample ts=%d missing after restart", ts)
			}
		} else if v != want {
			t.Errorf("sample ts=%d: got %g, want %g", ts, v, want)
		}
	}
	if missing > 5 {
		t.Errorf("... and %d more missing samples (total missing: %d)", missing-5, missing)
	}
}
