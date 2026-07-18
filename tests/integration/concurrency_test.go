package integration_test

import (
	"fmt"
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
	id := metrics.SeriesID(labels.Hash())

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
				// Query only the seeded range. Later re-ingested samples start at
				// chunkSize*1000 and must not mask a gap in the original 0..119 set.
				got, err := bs.QueryRange(id, 0, int64((chunkSize-1)*1000))
				if err != nil {
					t.Errorf("QueryRange error: %v", err)
					return
				}
				if len(got) != chunkSize {
					t.Errorf("QueryRange returned %d samples for seeded range, want %d — visibility gap detected", len(got), chunkSize)
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

	const appendWorkers = 8
	const samplesPerWorker = 150

	// Pre-seed one sealed chunk (120 samples) before starting any goroutine.
	// This guarantees the first FlushBlock call has sealed chunks to commit,
	// so the checkpoint-deletion path is definitely exercised. Without this,
	// all 40 flush iterations can be no-ops if chunks haven't sealed yet, and
	// the test passes trivially via WAL replay alone.
	// Timestamps are placed beyond the concurrent range to avoid collisions.
	const seedBase = int64(appendWorkers * samplesPerWorker)
	for i := 0; i < 120; i++ {
		ts := seedBase + int64(i)
		val := float64(ts)
		if err := store.Append(labels, ts, val); err != nil {
			t.Fatalf("seed Append %d: %v", i, err)
		}
		mu.Lock()
		acknowledged[ts] = val
		mu.Unlock()
	}

	var appendWg sync.WaitGroup
	for i := 0; i < appendWorkers; i++ {
		appendWg.Add(1)
		go func(workerID int) {
			defer appendWg.Done()
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

	// Flush goroutine: loops continuously until all append workers are done.
	// This guarantees overlap between FlushBlock's checkpoint calculation and
	// concurrent appends throughout the whole append phase, not just in a fixed
	// window that might complete before appends generate sealed chunks.
	appendsDone := make(chan struct{})
	var flushWg sync.WaitGroup
	flushWg.Add(1)
	go func() {
		defer flushWg.Done()
		for {
			if _, err := store.FlushBlock(); err != nil {
				t.Errorf("FlushBlock: %v", err)
			}
			select {
			case <-appendsDone:
				return
			default:
			}
		}
	}()

	appendWg.Wait()
	close(appendsDone)
	flushWg.Wait()
	if err := w1.Close(); err != nil {
		t.Fatalf("Close WAL: %v", err)
	}

	// Assert that at least one flush actually committed a block. If the block
	// directory is empty the checkpoint-deletion path was never exercised and
	// the correctness assertion below proves nothing.
	blockDir := filepath.Join(dir, "metrics", "blocks")
	blockEntries, err := os.ReadDir(blockDir)
	if err != nil {
		t.Fatalf("ReadDir blocks: %v", err)
	}
	var blockCount int
	for _, e := range blockEntries {
		if e.IsDir() {
			blockCount++
		}
	}
	if blockCount == 0 {
		t.Fatalf("no blocks written — FlushBlock never committed a chunk; checkpoint-deletion race was not exercised")
	}
	// Read the checkpoint value and verify that each covered WAL segment was
	// actually deleted. os.Stat alone only proves the file exists; we need
	// concrete evidence that WAL segments were removed so that acknowledged
	// sample survival depends on the block, not WAL replay.
	checkpointPath := filepath.Join(dir, "metrics", "checkpoint")
	if _, statErr := os.Stat(checkpointPath); os.IsNotExist(statErr) {
		t.Fatal("checkpoint file never written — no WAL segments were deleted; appendMu invariant was not on the critical path")
	}
	checkpointVal := metrics.ReadCheckpoint(dir)
	if checkpointVal < 0 {
		t.Fatalf("checkpoint value %d means no WAL segments were deleted; appendMu invariant was not on the critical path", checkpointVal)
	}
	for seg := 0; seg <= checkpointVal; seg++ {
		segPath := filepath.Join(walDir, fmt.Sprintf("%06d.wal", seg))
		if _, statErr := os.Stat(segPath); !os.IsNotExist(statErr) {
			t.Errorf("WAL segment %06d.wal should have been deleted (checkpoint=%d) but still exists", seg, checkpointVal)
			break
		}
	}

	// Simulate restart: load persisted blocks then replay remaining WAL segments.
	bs2, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("NewBlockStore restart: %v", err)
	}
	checkpoint := metrics.ReadCheckpoint(dir)
	replayInto(t, walDir, checkpoint, bs2)

	id := metrics.SeriesID(labels.Hash())
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

// pausingWriter wraps a WAL RecordWriter and pauses inside WriteRecord after the
// bytes have been committed but before returning. This creates the exact window
// the appendMu invariant protects: WAL record on disk, headSeg not yet updated.
type pausingWriter struct {
	inner     wal.RecordWriter
	afterDone chan struct{} // closed when WriteRecord finishes (before pause)
	resume    chan struct{} // closed by the test to release the pause
}

func (p *pausingWriter) WriteRecord(pairs []wal.LabelPair, tsMs int64, val float64) error {
	err := p.inner.WriteRecord(pairs, tsMs, val)
	close(p.afterDone) // WAL bytes on disk; AppendTracked has not been called yet
	<-p.resume         // hold the appendMu window open while the test probes FlushBlock
	return err
}

func (p *pausingWriter) SegmentIndex() int { return p.inner.SegmentIndex() }

// TestAppendMu_FlushBlockWaitsForInFlightAppend is a deterministic regression
// test for the appendMu invariant. It forces the exact interleaving that was
// previously a race condition:
//
//  1. Append goroutine writes to WAL (holding appendMu) then pauses before
//     calling AppendTracked — WAL record is on disk, headSeg is not updated.
//  2. FlushBlock goroutine completes its block write then tries to acquire
//     appendMu for OldestHeadSegment+SegmentIndex sampling. It must block.
//  3. Test asserts FlushBlock has not finished after 50ms (it is blocked).
//  4. Append is released: AppendTracked sets headSeg, appendMu released.
//  5. FlushBlock samples the correct fence and writes a checkpoint that
//     preserves the segment containing the in-flight sample.
//  6. Restart confirms the sample survives.
//
// Without appendMu, step 2 would observe OldestHeadSegment=-1 and compute
// safeDelete = SegmentIndex()-1, potentially deleting the segment from step 1.
func TestAppendMu_FlushBlockWaitsForInFlightAppend(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "metrics", "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	// 1 KiB segments ensure rotation during seed so safeDelete ≥ 0 and at
	// least one segment is deleted, making sample survival depend on the block.
	w, err := wal.Open(walDir, 1<<10, 1)
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}

	bs, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}

	// Seed 120 samples via a plain WALStore so FlushBlock has a sealed chunk to commit.
	seedStore := metrics.NewWALStore(w, bs, dir)
	labels, _ := metrics.NewLabels(map[string]string{"__name__": "mutex_probe"})
	for i := 0; i < 120; i++ {
		if err := seedStore.Append(labels, int64(i), float64(i)); err != nil {
			t.Fatalf("seed Append %d: %v", i, err)
		}
	}

	// Wire up the pausingWriter for the single controlled Append (ts=120).
	afterWriteDone := make(chan struct{})
	resume := make(chan struct{})
	pw := &pausingWriter{inner: w, afterDone: afterWriteDone, resume: resume}
	store := metrics.NewWALStore(pw, bs, dir)

	// Install the hook so we know exactly when FlushBlock has finished block I/O
	// and is about to call appendMu.Lock(). The Append goroutine is still paused
	// (holding appendMu) when the hook fires, so in a broken implementation
	// without appendMu FlushBlock would call OldestHeadSegment immediately and
	// observe headSeg=-1; with appendMu it must block here until we release Append.
	readyToLock := make(chan struct{})
	store.SetTestBeforeCheckpoint(func() {
		close(readyToLock) // FlushBlock has reached the appendMu acquisition point
	})

	// Goroutine A: Append ts=120. Pauses inside WriteRecord holding appendMu.
	appendDone := make(chan error, 1)
	go func() {
		appendDone <- store.Append(labels, 120, 120.0)
	}()

	// Wait for WAL write to complete (AppendTracked has NOT been called yet).
	select {
	case <-afterWriteDone:
	case <-time.After(5 * time.Second):
		close(resume)
		t.Fatal("append goroutine never reached WriteRecord pause point")
	}

	// Goroutine B: FlushBlock. Starts block I/O; will fire the hook when it
	// reaches appendMu acquisition.
	flushDone := make(chan error, 1)
	go func() {
		_, ferr := store.FlushBlock()
		flushDone <- ferr
	}()

	// Wait for FlushBlock to reach appendMu.Lock() — this is deterministic.
	// If FlushBlock never fires the hook (e.g. no sealed chunks), the test
	// setup is wrong and we fail hard.
	select {
	case <-readyToLock:
	case <-time.After(10 * time.Second):
		close(resume)
		t.Fatal("FlushBlock never reached checkpoint boundary (no sealed chunk?)")
	}

	// At this exact point: Append goroutine holds appendMu (still paused at
	// <-p.resume), FlushBlock is at appendMu.Lock(). FlushBlock must not have
	// finished yet.
	select {
	case err := <-flushDone:
		t.Errorf("FlushBlock completed before acquiring appendMu (invariant broken): %v", err)
		close(resume)
		return
	default:
	}

	// Release Goroutine A: AppendTracked sets headSeg, then appendMu is released.
	// FlushBlock unblocks, samples the correct fence, and writes checkpoint.
	close(resume)

	if err := <-appendDone; err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := <-flushDone; err != nil {
		t.Fatalf("FlushBlock: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close WAL: %v", err)
	}

	// Simulate restart: the checkpoint must have preserved the segment holding
	// ts=120 (safeDelete = headFence-1 < segment of ts=120).
	bs2, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("NewBlockStore restart: %v", err)
	}
	checkpoint := metrics.ReadCheckpoint(dir)
	replayInto(t, walDir, checkpoint, bs2)

	got, err := bs2.QueryRange(metrics.SeriesID(labels.Hash()), 120, 120)
	if err != nil {
		t.Fatalf("QueryRange: %v", err)
	}
	if len(got) != 1 {
		t.Errorf("in-flight sample ts=120 missing after restart: got %d samples, want 1 — appendMu did not preserve its WAL segment", len(got))
	}
}
