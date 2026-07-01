package metrics_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/storage/block"
	"github.com/masonwheeler/observability-platform/internal/storage/chunk"
)

// startQueryLoop runs do (a query plus its assertions) in a goroutine until stop is
// called, returning a started channel closed once the first query has COMPLETED.
// Waiting on started guarantees the query path is exercised and the goroutine is
// actively looping before the operation under test begins, so the operation runs
// concurrently with a live query loop — without a timing-based completion count that
// could false-fail when the operation legitimately blocks queries under its write
// lock. A do error is reported via t.Errorf.
func startQueryLoop(t *testing.T, do func() error) (started <-chan struct{}, stop, wait func()) {
	t.Helper()
	startedCh := make(chan struct{})
	stopCh := make(chan struct{})
	var once sync.Once
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer once.Do(func() { close(startedCh) }) // never leave a waiter blocked
		for {
			select {
			case <-stopCh:
				return
			default:
				if err := do(); err != nil {
					t.Errorf("query during operation: %v", err)
					return
				}
				once.Do(func() { close(startedCh) })
			}
		}
	}()
	return startedCh, func() { close(stopCh) }, wg.Wait
}

// fdsUnder counts this process's open file descriptors whose target path is
// under dir, via /proc/self/fd. The test is skipped on platforms without it.
func fdsUnder(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skip("/proc/self/fd unavailable; cannot observe fd leaks")
	}
	n := 0
	for _, e := range entries {
		target, err := os.Readlink(filepath.Join("/proc/self/fd", e.Name()))
		if err != nil {
			continue
		}
		if strings.HasPrefix(target, dir) {
			n++
		}
	}
	return n
}

func makeLabels(t *testing.T, m map[string]string) metrics.Labels {
	t.Helper()
	l, err := metrics.NewLabels(m)
	if err != nil {
		t.Fatalf("NewLabels: %v", err)
	}
	return l
}

// labelsToBlockPairs converts a label set to sorted block.LabelPair form.
func labelsToBlockPairs(l metrics.Labels) []block.LabelPair {
	m := l.Map()
	pairs := make([]block.LabelPair, 0, len(m))
	for name, val := range m {
		pairs = append(pairs, block.LabelPair{Name: name, Value: val})
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].Name < pairs[j].Name })
	return pairs
}

// sealedChunk returns a chunk filled past the seal threshold.
func sealedChunk(t *testing.T) *chunk.Chunk {
	t.Helper()
	c := chunk.NewChunk()
	for i := int64(0); i < 120; i++ {
		if err := c.Append(1000+i, float64(i), i); err != nil {
			t.Fatalf("chunk append: %v", err)
		}
	}
	return c
}

// TestNewBlockStore_RejectsFingerprintMismatch verifies that a block whose
// stored series ID does not match the fingerprint of its label set is rejected
// at load, rather than silently producing wrong query results later.
func TestNewBlockStore_RejectsFingerprintMismatch(t *testing.T) {
	dataDir := t.TempDir()
	blocksDir := filepath.Join(dataDir, "metrics", "blocks")
	tmpDir := filepath.Join(dataDir, "metrics", "tmp")
	w, err := block.NewWriter(blocksDir, tmpDir)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	labels := makeLabels(t, map[string]string{"__name__": "cpu", "host": "a"})
	// Deliberately store a wrong ID (correct fingerprint + 1).
	wrongID := uint64(labels.Fingerprint()) + 1
	if err := w.AddSeries(wrongID, labelsToBlockPairs(labels), []*chunk.Chunk{sealedChunk(t)}); err != nil {
		t.Fatalf("AddSeries: %v", err)
	}
	if _, err := w.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// This block opens successfully (holding its postings fd) and only then fails
	// fingerprint validation, so the failure path must close the reader it opened.
	before := fdsUnder(t, blocksDir)
	if _, err := metrics.NewBlockStore(dataDir); err == nil {
		t.Fatal("NewBlockStore with fingerprint/ID mismatch: want error, got nil")
	}
	if after := fdsUnder(t, blocksDir); after > before {
		t.Errorf("NewBlockStore leaked %d open fd(s) under %s when validation failed", after-before, blocksDir)
	}
}

// TestNewBlockStore_FailureClosesLoadedReaders verifies that when loading aborts
// partway through, the readers already opened are closed rather than leaked. A
// valid block (random hex directory name) is loaded first, then an invalid
// directory named to sort after any hex name forces OpenReader to fail; the
// test asserts no file descriptor under the blocks directory is left open.
func TestNewBlockStore_FailureClosesLoadedReaders(t *testing.T) {
	dataDir := t.TempDir()

	// A valid block written through the normal flush path.
	bs, err := metrics.NewBlockStore(dataDir)
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}
	good := makeLabels(t, map[string]string{"__name__": "ok", "host": "a"})
	for i := 0; i < 120; i++ {
		if err := bs.Append(good, int64(i*1000), float64(i)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if _, err := bs.FlushBlock(); err != nil {
		t.Fatalf("FlushBlock: %v", err)
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// An invalid block directory that sorts after any hex block id (block IDs are
	// lowercase hex, all < 'z'), so the valid block is loaded — and must be
	// closed — before OpenReader fails on this one (no meta.json).
	blocksDir := filepath.Join(dataDir, "metrics", "blocks")
	if err := os.Mkdir(filepath.Join(blocksDir, "zzzz-invalid"), 0o755); err != nil {
		t.Fatalf("mkdir invalid block: %v", err)
	}

	before := fdsUnder(t, blocksDir)
	if _, err := metrics.NewBlockStore(dataDir); err == nil {
		t.Fatal("NewBlockStore with an invalid block directory: want error, got nil")
	}
	if after := fdsUnder(t, blocksDir); after > before {
		t.Errorf("NewBlockStore leaked %d open fd(s) under %s on failure", after-before, blocksDir)
	}
}

func TestBlockStore_FlushBlock_DrainsSealedChunks(t *testing.T) {
	dataDir := t.TempDir()
	bs, err := metrics.NewBlockStore(dataDir)
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}

	labels := makeLabels(t, map[string]string{"__name__": "cpu", "host": "a"})

	// Append 121 samples to force one sealed chunk + one open head chunk.
	for i := 0; i < 121; i++ {
		if err := bs.Append(labels, int64(i*1000), float64(i)); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	if _, err := bs.FlushBlock(); err != nil {
		t.Fatalf("FlushBlock: %v", err)
	}

	// After flush, the memory store should retain only the unsealed head chunk.
	id := labels.Fingerprint()
	mem := bs.MemStore()
	if mem.ChunkCount(id) != 1 {
		t.Errorf("ChunkCount after flush = %d, want 1 (head only)", mem.ChunkCount(id))
	}
}

func TestBlockStore_QueryRange_SpansMemoryAndBlock(t *testing.T) {
	dataDir := t.TempDir()
	bs, err := metrics.NewBlockStore(dataDir)
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}

	labels := makeLabels(t, map[string]string{"__name__": "req", "env": "test"})
	id := labels.Fingerprint()

	// Append 120 samples — exactly fills one chunk (seals it).
	for i := 0; i < 120; i++ {
		if err := bs.Append(labels, int64(i*1000), float64(i)); err != nil {
			t.Fatalf("Append block sample %d: %v", i, err)
		}
	}

	if _, err := bs.FlushBlock(); err != nil {
		t.Fatalf("FlushBlock: %v", err)
	}

	// Append 3 more samples into the new head chunk.
	for i := 120; i < 123; i++ {
		if err := bs.Append(labels, int64(i*1000), float64(i)); err != nil {
			t.Fatalf("Append head sample %d: %v", i, err)
		}
	}

	got, err := bs.QueryRange(id, 0, int64(122*1000))
	if err != nil {
		t.Fatalf("QueryRange: %v", err)
	}
	if len(got) != 123 {
		t.Errorf("QueryRange returned %d samples, want 123", len(got))
	}
}

func TestBlockStore_QueryRange_Deduplication(t *testing.T) {
	dataDir := t.TempDir()
	bs, err := metrics.NewBlockStore(dataDir)
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}

	labels := makeLabels(t, map[string]string{"__name__": "dup", "host": "x"})
	id := labels.Fingerprint()

	// Append 120 samples and flush to block.
	for i := 0; i < 120; i++ {
		if err := bs.Append(labels, int64(i*1000), float64(i)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if _, err := bs.FlushBlock(); err != nil {
		t.Fatalf("FlushBlock: %v", err)
	}

	// Simulate crash-recovery: re-append the same 120 samples into memory.
	for i := 0; i < 120; i++ {
		if err := bs.Append(labels, int64(i*1000), float64(i)); err != nil {
			t.Fatalf("Re-append: %v", err)
		}
	}

	got, err := bs.QueryRange(id, 0, int64(119*1000))
	if err != nil {
		t.Fatalf("QueryRange: %v", err)
	}
	if len(got) != 120 {
		t.Errorf("QueryRange returned %d samples after re-append, want 120 (no duplicates)", len(got))
	}
}

func TestBlockStore_SelectSeries_IncludesBlockSeries(t *testing.T) {
	dataDir := t.TempDir()
	bs, err := metrics.NewBlockStore(dataDir)
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}

	labels := makeLabels(t, map[string]string{"__name__": "disk", "dev": "sda"})

	// Fill and flush a chunk so the series lands in a block.
	for i := 0; i < 120; i++ {
		if err := bs.Append(labels, int64(i*1000), float64(i)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if _, err := bs.FlushBlock(); err != nil {
		t.Fatalf("FlushBlock: %v", err)
	}

	// Open a fresh BlockStore from the same dataDir to simulate restart:
	// memory is empty, block is loaded from disk.
	bs2, err := metrics.NewBlockStore(dataDir)
	if err != nil {
		t.Fatalf("NewBlockStore restart: %v", err)
	}

	sel := metrics.Selector{MetricName: "disk"}
	matched, err := bs2.SelectSeries(sel)
	if err != nil {
		t.Fatalf("SelectSeries: %v", err)
	}
	if len(matched) == 0 {
		t.Fatal("SelectSeries on fresh BlockStore returned no series, want series from block")
	}
}

func TestBlockStore_SelectSeries_IndexedAcrossBlockAndMemory(t *testing.T) {
	bs, err := metrics.NewBlockStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}
	defer bs.Close()

	// Seal a chunk (>120 samples) for an "http"/"api" series, then flush to a block.
	apiLabels := mustLabels(t, map[string]string{"__name__": "http", "job": "api"})
	for i := int64(0); i < 130; i++ {
		if err := bs.Append(apiLabels, 1000+i, float64(i)); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if _, err := bs.FlushBlock(); err != nil {
		t.Fatalf("FlushBlock: %v", err)
	}
	// A memory-only series.
	webLabels := mustLabels(t, map[string]string{"__name__": "http", "job": "web"})
	if err := bs.Append(webLabels, 2000, 1); err != nil {
		t.Fatalf("append web: %v", err)
	}

	got, err := bs.SelectSeries(metrics.Selector{MetricName: "http"})
	if err != nil {
		t.Fatalf("SelectSeries(http): %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("SelectSeries(http) matched %d, want 2 (block+memory)", len(got))
	}
	gotAPI, err := bs.SelectSeries(metrics.Selector{MetricName: "http", Matchers: []metrics.Matcher{{Name: "job", Value: "api"}}})
	if err != nil {
		t.Fatalf("SelectSeries(http,job=api): %v", err)
	}
	if len(gotAPI) != 1 {
		t.Fatalf("SelectSeries(http,job=api) matched %d, want 1", len(gotAPI))
	}
}

func TestBlockStore_LabelNamesValues_AcrossBlockAndMemory(t *testing.T) {
	bs, err := metrics.NewBlockStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}
	defer bs.Close()

	l := mustLabels(t, map[string]string{"__name__": "http", "job": "api"})
	for i := int64(0); i < 130; i++ {
		_ = bs.Append(l, 1000+i, float64(i))
	}
	_, _ = bs.FlushBlock()
	_ = bs.Append(mustLabels(t, map[string]string{"__name__": "cpu", "host": "h1"}), 2000, 1)

	names := bs.LabelNames()
	want := map[string]bool{"__name__": true, "job": true, "host": true}
	if len(names) != 3 {
		t.Fatalf("LabelNames = %v, want 3 distinct", names)
	}
	for _, n := range names {
		if !want[n] {
			t.Fatalf("unexpected label name %q in %v", n, names)
		}
	}
	if got := bs.LabelValues("__name__"); len(got) != 2 {
		t.Fatalf("LabelValues(__name__) = %v, want [cpu http]", got)
	}
}

func TestBlockStore_BlockInfos_And_StorageStats(t *testing.T) {
	bs, err := metrics.NewBlockStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}
	lbls, _ := metrics.NewLabels(map[string]string{"__name__": "m"})
	// Seal one chunk (120 samples) then flush to make one block.
	for i := 0; i < 120; i++ {
		if err := bs.Append(lbls, int64(i)*1000, float64(i)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if _, err := bs.FlushBlock(); err != nil {
		t.Fatalf("FlushBlock: %v", err)
	}
	infos := bs.BlockInfos()
	if len(infos) != 1 {
		t.Fatalf("BlockInfos len = %d, want 1", len(infos))
	}
	if infos[0].Level != 1 || infos[0].SizeBytes <= 0 {
		t.Fatalf("BlockInfo = %+v, want level 1 and positive size", infos[0])
	}
	if infos[0].MinTime != 0 || infos[0].MaxTime != 119000 {
		t.Fatalf("BlockInfo min/max = %d/%d, want 0/119000", infos[0].MinTime, infos[0].MaxTime)
	}
	blocks, bytes := bs.StorageStats()
	if blocks != 1 || bytes != infos[0].SizeBytes {
		t.Fatalf("StorageStats = %d,%d; want 1,%d", blocks, bytes, infos[0].SizeBytes)
	}
}

func flushOneBlock(t *testing.T, bs *metrics.BlockStore, name string, base int64) {
	t.Helper()
	lbls, _ := metrics.NewLabels(map[string]string{"__name__": name})
	for i := 0; i < 120; i++ {
		if err := bs.Append(lbls, base+int64(i)*1000, float64(i)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if _, err := bs.FlushBlock(); err != nil {
		t.Fatalf("FlushBlock: %v", err)
	}
}

func metricFingerprint(t *testing.T, name string) metrics.SeriesID {
	t.Helper()
	lbls, err := metrics.NewLabels(map[string]string{"__name__": name})
	if err != nil {
		t.Fatal(err)
	}
	return lbls.Fingerprint()
}

func TestBlockStore_CompactOnce_MergesAndPreservesData(t *testing.T) {
	bs, _ := metrics.NewBlockStore(t.TempDir())
	flushOneBlock(t, bs, "m", 0)      // block A: ts 0..119000
	flushOneBlock(t, bs, "m", 200000) // block B: ts 200000..319000

	infos := bs.BlockInfos()
	if len(infos) != 2 {
		t.Fatalf("setup: want 2 blocks, got %d", len(infos))
	}
	all := []string{infos[0].ID, infos[1].ID}
	plan := func([]block.BlockInfo) [][]string { return [][]string{all} }

	n, err := bs.CompactOnce(plan)
	if err != nil {
		t.Fatalf("CompactOnce: %v", err)
	}
	if n != 1 {
		t.Fatalf("CompactOnce merged %d groups, want 1", n)
	}
	if got := len(bs.BlockInfos()); got != 1 {
		t.Fatalf("after compaction blocks = %d, want 1", got)
	}
	id := metricFingerprint(t, "m")
	got, err := bs.QueryRange(id, 0, 400000)
	if err != nil {
		t.Fatalf("QueryRange: %v", err)
	}
	if len(got) != 240 {
		t.Fatalf("merged query returned %d samples, want 240", len(got))
	}
}

func TestBlockStore_CompactOnce_ConcurrentQueriesNeverError(t *testing.T) {
	bs, _ := metrics.NewBlockStore(t.TempDir())
	flushOneBlock(t, bs, "m", 0)
	flushOneBlock(t, bs, "m", 200000)
	all := []string{bs.BlockInfos()[0].ID, bs.BlockInfos()[1].ID}
	id := metricFingerprint(t, "m")

	started, stop, wait := startQueryLoop(t, func() error {
		_, err := bs.QueryRange(id, 0, 400000)
		return err
	})
	<-started // a query has completed; the goroutine keeps querying through the merge
	if _, err := bs.CompactOnce(func([]block.BlockInfo) [][]string { return [][]string{all} }); err != nil {
		t.Fatalf("CompactOnce: %v", err)
	}
	stop()
	wait()
}

// TestBlockStore_CompactOnce_ConcurrentQueriesSeeAllSamples asserts queries running
// concurrently with compaction never miss samples: the two source blocks hold 240
// samples total and every query returns exactly 240, before, during, and after the
// merge. The handshake guarantees the query loop is live before the merge starts.
func TestBlockStore_CompactOnce_ConcurrentQueriesSeeAllSamples(t *testing.T) {
	bs, _ := metrics.NewBlockStore(t.TempDir())
	flushOneBlock(t, bs, "m", 0)      // 120 samples, ts 0..119000
	flushOneBlock(t, bs, "m", 200000) // 120 samples, ts 200000..319000
	all := []string{bs.BlockInfos()[0].ID, bs.BlockInfos()[1].ID}
	id := metricFingerprint(t, "m")

	started, stop, wait := startQueryLoop(t, func() error {
		got, err := bs.QueryRange(id, 0, 400000)
		if err != nil {
			return err
		}
		if len(got) != 240 {
			return fmt.Errorf("returned %d samples, want 240 (no data missed)", len(got))
		}
		return nil
	})
	<-started
	if _, err := bs.CompactOnce(func([]block.BlockInfo) [][]string { return [][]string{all} }); err != nil {
		t.Fatalf("CompactOnce: %v", err)
	}
	stop()
	wait()
}

func TestBlockStore_ApplyRetention_Boundary(t *testing.T) {
	bs, _ := metrics.NewBlockStore(t.TempDir())
	flushOneBlock(t, bs, "m", 0) // block MaxTime = 119000

	// retention=0 → no-op.
	if n, err := bs.ApplyRetention(time.UnixMilli(10_000_000), 0); err != nil || n != 0 {
		t.Fatalf("retention=0 = %d,%v; want 0,nil", n, err)
	}

	// cutoff == MaxTime (119000): MaxTime < cutoff is false → block KEPT (strict boundary).
	now := time.UnixMilli(119001)
	n, err := bs.ApplyRetention(now, 1*time.Millisecond) // cutoff = 119000; 119000 < 119000 is false → kept
	if err != nil {
		t.Fatalf("ApplyRetention: %v", err)
	}
	if n != 0 || len(bs.BlockInfos()) != 1 {
		t.Fatalf("at exact boundary block should be kept: deleted=%d blocks=%d", n, len(bs.BlockInfos()))
	}

	// cutoff strictly greater than MaxTime → deleted.
	n, err = bs.ApplyRetention(time.UnixMilli(119002), 1*time.Millisecond) // cutoff = 119001 > 119000 → delete
	if err != nil {
		t.Fatalf("ApplyRetention: %v", err)
	}
	if n != 1 || len(bs.BlockInfos()) != 0 {
		t.Fatalf("expired block should be deleted: deleted=%d blocks=%d", n, len(bs.BlockInfos()))
	}
}

func TestBlockStore_StartupGC_RemovesSupersededSources(t *testing.T) {
	dir := t.TempDir()
	bs, _ := metrics.NewBlockStore(dir)
	flushOneBlock(t, bs, "m", 0)      // source S1
	flushOneBlock(t, bs, "m", 200000) // source S2
	infos := bs.BlockInfos()
	s1, s2 := infos[0].ID, infos[1].ID

	// Fabricate a crash state: write a merged block whose meta lists S1+S2 as
	// Sources WITHOUT deleting the sources, so all three coexist on disk exactly
	// as they would after a crash between block.Compact and source deletion.
	blocksDir := filepath.Join(dir, "metrics", "blocks")
	tmpDir := filepath.Join(dir, "metrics", "tmp")
	r1, err := block.OpenReader(filepath.Join(blocksDir, s1))
	if err != nil {
		t.Fatalf("open s1: %v", err)
	}
	r2, err := block.OpenReader(filepath.Join(blocksDir, s2))
	if err != nil {
		t.Fatalf("open s2: %v", err)
	}
	meta, err := block.Compact(blocksDir, tmpDir, []*block.Reader{r1, r2})
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	_ = r1.Close()
	_ = r2.Close()
	_ = bs.Close()

	// Reopen: startup GC must delete S1 and S2 (listed in the merged block's
	// Sources), leaving only the merged block, and all data must remain queryable.
	reopened, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	infos2 := reopened.BlockInfos()
	if len(infos2) != 1 || infos2[0].ID != meta.BlockID {
		t.Fatalf("after GC blocks = %+v, want only merged %s", infos2, meta.BlockID)
	}
	got, err := reopened.QueryRange(metricFingerprint(t, "m"), 0, 400000)
	if err != nil {
		t.Fatalf("QueryRange: %v", err)
	}
	if len(got) != 240 {
		t.Fatalf("after GC query = %d samples, want 240", len(got))
	}
}

// makeCrashState flushes two source blocks and compacts them into a merged block
// while leaving the sources on disk — the exact state a crash between
// block.Compact and source deletion produces. Returns the source IDs, the merged
// meta, and the blocks/tmp dirs.
func makeCrashState(t *testing.T, dir string) (s1, s2 string, mergedID, blocksDir, tmpDir string) {
	t.Helper()
	bs, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}
	flushOneBlock(t, bs, "m", 0)
	flushOneBlock(t, bs, "m", 200000)
	infos := bs.BlockInfos()
	s1, s2 = infos[0].ID, infos[1].ID
	blocksDir = filepath.Join(dir, "metrics", "blocks")
	tmpDir = filepath.Join(dir, "metrics", "tmp")

	r1, err := block.OpenReader(filepath.Join(blocksDir, s1))
	if err != nil {
		t.Fatalf("open s1: %v", err)
	}
	r2, err := block.OpenReader(filepath.Join(blocksDir, s2))
	if err != nil {
		t.Fatalf("open s2: %v", err)
	}
	meta, err := block.Compact(blocksDir, tmpDir, []*block.Reader{r1, r2})
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	_ = r1.Close()
	_ = r2.Close()
	_ = bs.Close()
	return s1, s2, meta.BlockID, blocksDir, tmpDir
}

// TestNewBlockStore_CorruptSurvivor_PreservesSources is the regression for the
// startup-GC data-loss bug: when a compacted block is corrupt, its source blocks
// must NOT be reclaimed (they are the only recoverable copy). Startup fails, but
// the data survives for a later repair.
func TestNewBlockStore_CorruptSurvivor_PreservesSources(t *testing.T) {
	dir := t.TempDir()
	s1, s2, mergedID, blocksDir, _ := makeCrashState(t, dir)

	// Corrupt the merged block's index so it cannot open/validate.
	if err := os.WriteFile(filepath.Join(blocksDir, mergedID, "index"), []byte("garbage"), 0o644); err != nil {
		t.Fatalf("corrupt index: %v", err)
	}

	if _, err := metrics.NewBlockStore(dir); err == nil {
		t.Fatal("NewBlockStore: want error from corrupt survivor, got nil")
	}
	for _, id := range []string{s1, s2} {
		if _, err := os.Stat(filepath.Join(blocksDir, id)); err != nil {
			t.Fatalf("source %s was reclaimed despite a corrupt survivor: %v", id, err)
		}
	}
}

// TestNewBlockStore_CorruptSurvivorChunks_PreservesSources is the regression for
// trusting a survivor on index validation alone: a merged block with a valid index
// but a corrupt chunks file must NOT reclaim its sources. Chunks are read lazily,
// so only deep chunk validation catches this before deletion.
func TestNewBlockStore_CorruptSurvivorChunks_PreservesSources(t *testing.T) {
	dir := t.TempDir()
	s1, s2, mergedID, blocksDir, _ := makeCrashState(t, dir)

	// Truncate the merged block's chunks file; index and postings stay valid.
	if err := os.WriteFile(filepath.Join(blocksDir, mergedID, "chunks"), []byte{}, 0o644); err != nil {
		t.Fatalf("corrupt chunks: %v", err)
	}

	if _, err := metrics.NewBlockStore(dir); err == nil {
		t.Fatal("NewBlockStore: want error from survivor with corrupt chunks, got nil")
	}
	for _, id := range []string{s1, s2} {
		if _, err := os.Stat(filepath.Join(blocksDir, id)); err != nil {
			t.Fatalf("source %s was reclaimed despite corrupt survivor chunks: %v", id, err)
		}
	}
}

// TestNewBlockStore_CorruptSurvivorMaxGen_PreservesSources verifies that a survivor
// whose persisted MaxGen disagrees with its reconstructed generations is rejected
// (rather than trusted to authorize deletion of recoverable source blocks).
func TestNewBlockStore_CorruptSurvivorMaxGen_PreservesSources(t *testing.T) {
	dir := t.TempDir()
	s1, s2, mergedID, blocksDir, _ := makeCrashState(t, dir)

	metaPath := filepath.Join(blocksDir, mergedID, "meta.json")
	raw, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatalf("read meta: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal meta: %v", err)
	}
	m["max_gen"] = 999999 // disagrees with the survivor's actual chunk generations
	out, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal meta: %v", err)
	}
	if err := os.WriteFile(metaPath, out, 0o644); err != nil {
		t.Fatalf("write meta: %v", err)
	}

	if _, err := metrics.NewBlockStore(dir); err == nil {
		t.Fatal("NewBlockStore: want error from survivor with corrupt MaxGen, got nil")
	}
	for _, id := range []string{s1, s2} {
		if _, err := os.Stat(filepath.Join(blocksDir, id)); err != nil {
			t.Fatalf("source %s reclaimed despite corrupt survivor MaxGen: %v", id, err)
		}
	}
}

// TestNewBlockStore_InjectedUnrelatedSourceNotDeleted is the regression for
// trusting Sources metadata blindly: an unrelated block whose ID is injected into a
// survivor's Sources list must NOT be reclaimed, because the survivor does not
// actually contain its data. Genuine sources (provably contained) are still
// reclaimed.
func TestNewBlockStore_InjectedUnrelatedSourceNotDeleted(t *testing.T) {
	dir := t.TempDir()

	// An unrelated block U: different metric, disjoint time range.
	bs, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}
	flushOneBlock(t, bs, "unrelated", 500000) // U: ts 500000..619000
	uID := bs.BlockInfos()[0].ID
	if err := bs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Crash state for metric "m": sources s1+s2 merged into mergedID (U untouched).
	s1, s2, mergedID, blocksDir, _ := makeCrashState(t, dir)

	// Tamper: inject U's ID into the merged block's Sources list.
	metaPath := filepath.Join(blocksDir, mergedID, "meta.json")
	raw, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatalf("read meta: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal meta: %v", err)
	}
	srcs, _ := m["sources"].([]any)
	m["sources"] = append(srcs, uID)
	out, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal meta: %v", err)
	}
	if err := os.WriteFile(metaPath, out, 0o644); err != nil {
		t.Fatalf("write meta: %v", err)
	}

	reopened, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}

	// U must survive and stay queryable — it is not superseded by the merged block.
	if _, err := os.Stat(filepath.Join(blocksDir, uID)); err != nil {
		t.Fatalf("unrelated block %s deleted via injected Sources metadata: %v", uID, err)
	}
	got, err := reopened.QueryRange(metricFingerprint(t, "unrelated"), 500000, 620000)
	if err != nil {
		t.Fatalf("QueryRange unrelated: %v", err)
	}
	if len(got) != 120 {
		t.Fatalf("unrelated data = %d samples, want 120", len(got))
	}
	// The genuine sources are provably contained, so they are still reclaimed.
	for _, id := range []string{s1, s2} {
		if _, err := os.Stat(filepath.Join(blocksDir, id)); err == nil {
			t.Fatalf("genuine source %s was not reclaimed", id)
		}
	}
}

// TestNewBlockStore_SelfReferencingSourceNotDeleted is the regression for a block
// whose Sources list names its own ID: it must not be able to authorize deleting
// itself. It survives restart with all data intact.
func TestNewBlockStore_SelfReferencingSourceNotDeleted(t *testing.T) {
	dir := t.TempDir()
	bs, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}
	flushOneBlock(t, bs, "m", 0) // block A: ts 0..119000
	aID := bs.BlockInfos()[0].ID
	if err := bs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	blocksDir := filepath.Join(dir, "metrics", "blocks")
	metaPath := filepath.Join(blocksDir, aID, "meta.json")
	raw, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatalf("read meta: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal meta: %v", err)
	}
	m["sources"] = []any{aID} // self-reference
	out, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal meta: %v", err)
	}
	if err := os.WriteFile(metaPath, out, 0o644); err != nil {
		t.Fatalf("write meta: %v", err)
	}

	reopened, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if _, err := os.Stat(filepath.Join(blocksDir, aID)); err != nil {
		t.Fatalf("self-referencing block %s deleted itself: %v", aID, err)
	}
	got, err := reopened.QueryRange(metricFingerprint(t, "m"), 0, 200000)
	if err != nil {
		t.Fatalf("QueryRange: %v", err)
	}
	if len(got) != 120 {
		t.Fatalf("after restart query = %d samples, want 120", len(got))
	}
}

// TestNewBlockStore_OlderBlockCannotSupersedeCorrection is the regression for
// generation containment: an older block that lists a newer same-series correction
// block in its Sources must NOT delete it, because it lacks the correction's higher
// generation. The correction's value must still win after restart.
func TestNewBlockStore_OlderBlockCannotSupersedeCorrection(t *testing.T) {
	dir := t.TempDir()
	bs, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}
	lbls := makeLabels(t, map[string]string{"__name__": "m"})

	// appendSealed writes one full (sealable) chunk of 120 samples over the same
	// timestamps with the given value, then flushes it into a block. Blocks flush
	// only sealed chunks, so a single sample would not produce a block.
	appendSealed := func(val float64) {
		t.Helper()
		for i := 0; i < 120; i++ {
			if err := bs.Append(lbls, 1000+int64(i)*1000, val); err != nil {
				t.Fatalf("Append: %v", err)
			}
		}
		if ok, err := bs.FlushBlock(); err != nil || !ok {
			t.Fatalf("FlushBlock ok=%v err=%v", ok, err)
		}
	}

	// Block A (older): m = 1.0 across ts 1000..120000, lower generations.
	appendSealed(1.0)
	aID := bs.BlockInfos()[0].ID

	// Block B (newer): m = 2.0 correction across the same timestamps, higher generations.
	appendSealed(2.0)
	var bID string
	for _, in := range bs.BlockInfos() {
		if in.ID != aID {
			bID = in.ID
		}
	}
	if bID == "" {
		t.Fatal("setup: could not identify newer block")
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Tamper: make the older block A claim the newer correction B as a source.
	blocksDir := filepath.Join(dir, "metrics", "blocks")
	metaPath := filepath.Join(blocksDir, aID, "meta.json")
	raw, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatalf("read meta: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal meta: %v", err)
	}
	m["sources"] = []any{bID}
	out, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal meta: %v", err)
	}
	if err := os.WriteFile(metaPath, out, 0o644); err != nil {
		t.Fatalf("write meta: %v", err)
	}

	reopened, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if _, err := os.Stat(filepath.Join(blocksDir, bID)); err != nil {
		t.Fatalf("newer correction block %s was deleted by an older block: %v", bID, err)
	}
	s, found, err := reopened.QueryInstant(metricFingerprint(t, "m"), 1000)
	if err != nil {
		t.Fatalf("QueryInstant: %v", err)
	}
	if !found || s.Value != 2.0 {
		t.Fatalf("after restart m@1000 = (found=%v, %v), want the correction 2.0", found, s.Value)
	}
}

// TestNewBlockStore_ReconcilesCorruptMetaTime is the regression for trusting
// meta.json's derived time/count fields: a tampered max_time must not silently drop
// the block's data from queries. Startup reconciles the fields against the decoded
// chunks (the source of truth).
func TestNewBlockStore_ReconcilesCorruptMetaTime(t *testing.T) {
	dir := t.TempDir()
	bs, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}
	flushOneBlock(t, bs, "m", 0) // ts 0..119000
	id := bs.BlockInfos()[0].ID
	if err := bs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Corrupt max_time (and num_samples) so queries would skip the block and
	// retention would treat it as ancient.
	blocksDir := filepath.Join(dir, "metrics", "blocks")
	metaPath := filepath.Join(blocksDir, id, "meta.json")
	raw, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatalf("read meta: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal meta: %v", err)
	}
	m["max_time"] = -1
	m["num_samples"] = 1
	out, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal meta: %v", err)
	}
	if err := os.WriteFile(metaPath, out, 0o644); err != nil {
		t.Fatalf("write meta: %v", err)
	}

	reopened, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	infos := reopened.BlockInfos()
	if len(infos) != 1 || infos[0].MinTime != 0 || infos[0].MaxTime != 119000 {
		t.Fatalf("meta not reconciled: got %+v, want MinTime 0 MaxTime 119000", infos)
	}
	got, err := reopened.QueryRange(metricFingerprint(t, "m"), 0, 200000)
	if err != nil {
		t.Fatalf("QueryRange: %v", err)
	}
	if len(got) != 120 {
		t.Fatalf("query after reconcile = %d samples, want 120 (tampered meta dropped data)", len(got))
	}
}

// TestMemoryStore_RejectsAppendWhenGenerationExhausted verifies the counter
// reaching the generation bound is an explicit error, not a silent write rejection.
func TestMemoryStore_RejectsAppendWhenGenerationExhausted(t *testing.T) {
	ms := metrics.NewMemoryStore()
	ms.EnsureGenFloor(chunk.MaxGeneration + 1) // simulate an exhausted counter
	err := ms.Append(makeLabels(t, map[string]string{"__name__": "m"}), 1000, 1.0)
	if !errors.Is(err, metrics.ErrGenerationExhausted) {
		t.Errorf("Append at exhausted generation = %v, want ErrGenerationExhausted", err)
	}
}

// TestNewBlockStore_ReconstructsGenFloorFromChunks verifies the generation floor is
// rebuilt from the generations actually stored in each block's chunks, not from a
// (here, understated) persisted Meta.MaxGen — so a new write still outranks the
// persisted sample at the same timestamp.
func TestNewBlockStore_ReconstructsGenFloorFromChunks(t *testing.T) {
	dir := t.TempDir()
	bs, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}

	const T = int64(1_000_000)
	samples := [][2]int64{{T, 10}}
	for i := int64(1); i < 120; i++ {
		samples = append(samples, [2]int64{T + i*1000, 0})
	}
	flushSamples(t, bs, "m", samples) // block's max generation is well above its T sample's
	blockID := bs.BlockInfos()[0].ID
	_ = bs.Close()

	// Understate the persisted MaxGen; a trust-meta floor would seed far too low.
	metaPath := filepath.Join(dir, "metrics", "blocks", blockID, "meta.json")
	raw, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatalf("read meta: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal meta: %v", err)
	}
	m["max_gen"] = 0
	out, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal meta: %v", err)
	}
	if err := os.WriteFile(metaPath, out, 0o644); err != nil {
		t.Fatalf("write meta: %v", err)
	}

	bs2, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer bs2.Close()
	if err := bs2.Append(makeLabels(t, map[string]string{"__name__": "m"}), T, 99); err != nil {
		t.Fatalf("Append: %v", err)
	}
	got, err := bs2.QueryRange(metricFingerprint(t, "m"), T, T)
	if err != nil {
		t.Fatalf("QueryRange: %v", err)
	}
	if len(got) != 1 || got[0].Value != 99 {
		t.Fatalf("m@T = %v, want value 99 (new write must outrank the persisted sample via the reconstructed floor)", got)
	}
}

// TestNewBlockStore_CorruptSupersededSource_Recovers verifies the converse: when
// the survivor is valid, a corrupt source it supersedes is reclaimed without
// failing startup, and the merged data stays queryable.
func TestNewBlockStore_CorruptSupersededSource_Recovers(t *testing.T) {
	dir := t.TempDir()
	s1, _, mergedID, blocksDir, _ := makeCrashState(t, dir)

	// Corrupt a source; the valid merged block supersedes it.
	if err := os.WriteFile(filepath.Join(blocksDir, s1, "index"), []byte("garbage"), 0o644); err != nil {
		t.Fatalf("corrupt source index: %v", err)
	}

	bs, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}
	defer bs.Close()
	infos := bs.BlockInfos()
	if len(infos) != 1 || infos[0].ID != mergedID {
		t.Fatalf("after recovery blocks = %+v, want only merged %s", infos, mergedID)
	}
	got, err := bs.QueryRange(metricFingerprint(t, "m"), 0, 400000)
	if err != nil {
		t.Fatalf("QueryRange: %v", err)
	}
	if len(got) != 240 {
		t.Fatalf("after recovery query = %d samples, want 240", len(got))
	}
}

// flushSamples appends the given (ts,value) samples for one series and flushes a
// block. The caller must supply enough samples (>=120) to seal a chunk.
func flushSamples(t *testing.T, bs *metrics.BlockStore, name string, samples [][2]int64) {
	t.Helper()
	lbls := makeLabels(t, map[string]string{"__name__": name})
	for _, s := range samples {
		if err := bs.Append(lbls, s[0], float64(s[1])); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	ok, err := bs.FlushBlock()
	if err != nil {
		t.Fatalf("FlushBlock: %v", err)
	}
	if !ok {
		t.Fatalf("FlushBlock: expected a sealed flush (need >=120 samples to seal)")
	}
}

// flushLabeledBlock appends 120 samples for each label set, then flushes them as
// one block.
func flushLabeledBlock(t *testing.T, bs *metrics.BlockStore, base int64, labelSets ...map[string]string) {
	t.Helper()
	for _, ls := range labelSets {
		lbls := makeLabels(t, ls)
		for i := 0; i < 120; i++ {
			if err := bs.Append(lbls, base+int64(i)*1000, float64(i)); err != nil {
				t.Fatalf("Append: %v", err)
			}
		}
	}
	ok, err := bs.FlushBlock()
	if err != nil {
		t.Fatalf("FlushBlock: %v", err)
	}
	if !ok {
		t.Fatalf("FlushBlock: expected a sealed flush")
	}
}

func countDirs(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir %s: %v", dir, err)
	}
	n := 0
	for _, e := range entries {
		if e.IsDir() {
			n++
		}
	}
	return n
}

func selectHosts(t *testing.T, bs *metrics.BlockStore) []string {
	t.Helper()
	ms, err := bs.SelectSeries(metrics.Selector{MetricName: "m"})
	if err != nil {
		t.Fatalf("SelectSeries: %v", err)
	}
	set := make(map[string]struct{})
	for _, s := range ms {
		set[s.Labels.Map()["host"]] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for h := range set {
		out = append(out, h)
	}
	sort.Strings(out)
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestBlockStore_LastWriteWins_ConsistentAcrossRuntimeRestartCompaction proves
// that for a duplicate timestamp written across two blocks, the later-written
// block wins identically at runtime, after a restart, and after compaction. The
// newer block deliberately has the smaller MinTime (an out-of-order correction),
// so a MinTime-based ordering would diverge between these three paths.
func TestBlockStore_LastWriteWins_ConsistentAcrossRuntimeRestartCompaction(t *testing.T) {
	dir := t.TempDir()
	const T = int64(1_000_000)

	a := make([][2]int64, 0, 120)
	for i := 0; i < 120; i++ {
		a = append(a, [2]int64{T + int64(i)*1000, int64(100 + i)}) // T -> 100
	}
	b := [][2]int64{{1, 7}, {T, 999}} // out-of-order ts=1 makes B.MinTime < A.MinTime; T -> 999
	for i := 0; i < 118; i++ {
		b = append(b, [2]int64{3_000_000 + int64(i)*1000, int64(i)})
	}

	id := metricFingerprint(t, "m")
	assertWins := func(bs *metrics.BlockStore, phase string) {
		t.Helper()
		s, found, err := bs.QueryInstant(id, T)
		if err != nil || !found {
			t.Fatalf("%s: QueryInstant(T) found=%v err=%v", phase, found, err)
		}
		if s.Value != 999 {
			t.Fatalf("%s: QueryInstant(T)=%v, want 999 (later-written block must win)", phase, s.Value)
		}
		rng, err := bs.QueryRange(id, T, T)
		if err != nil {
			t.Fatalf("%s: QueryRange: %v", phase, err)
		}
		if len(rng) != 1 || rng[0].Value != 999 {
			t.Fatalf("%s: QueryRange(T,T)=%v, want single value 999", phase, rng)
		}
	}

	bs1, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}
	flushSamples(t, bs1, "m", a) // lower generations
	flushSamples(t, bs1, "m", b) // higher generations (written later)
	assertWins(bs1, "runtime")
	_ = bs1.Close()

	bs2, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	assertWins(bs2, "restart") // winner is decided by per-sample generation, not load order

	infos := bs2.BlockInfos()
	if len(infos) != 2 {
		t.Fatalf("want 2 blocks before compaction, got %d", len(infos))
	}
	group := []string{infos[0].ID, infos[1].ID}
	if n, err := bs2.CompactOnce(func([]block.BlockInfo) [][]string { return [][]string{group} }); err != nil || n != 1 {
		t.Fatalf("CompactOnce = %d, %v; want 1, nil", n, err)
	}
	assertWins(bs2, "post-compaction")
	_ = bs2.Close()

	bs3, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("reopen after compaction: %v", err)
	}
	defer bs3.Close()
	if got := len(bs3.BlockInfos()); got != 1 {
		t.Fatalf("after compaction+restart blocks = %d, want 1", got)
	}
	assertWins(bs3, "post-compaction restart")
}

// onlyNewBlockID returns the one block ID in bs not already in known, recording it.
func onlyNewBlockID(t *testing.T, bs *metrics.BlockStore, known map[string]bool) string {
	t.Helper()
	for _, info := range bs.BlockInfos() {
		if !known[info.ID] {
			known[info.ID] = true
			return info.ID
		}
	}
	t.Fatal("no new block found")
	return ""
}

// TestBlockStore_PartialCompaction_PreservesLastWriteWins is the regression for the
// corner a single per-block generation cannot resolve: compaction merges block A
// (old value of series x) with an unrelated, newer block B, while block C — holding
// a newer correction of x — is left out of the group. Per-sample generations keep
// C's correction winning after the merge; a per-block generation on the merged
// block (max of A and B) would wrongly outrank C.
func TestBlockStore_PartialCompaction_PreservesLastWriteWins(t *testing.T) {
	dir := t.TempDir()
	bs, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}
	defer bs.Close()

	const T = int64(1_000_000)
	known := map[string]bool{}

	// Block A: series x, original value at T (lowest generations).
	aSamples := [][2]int64{{T, 1}}
	for i := int64(1); i < 120; i++ {
		aSamples = append(aSamples, [2]int64{T + i*1000, 0})
	}
	flushSamples(t, bs, "x", aSamples)
	idA := onlyNewBlockID(t, bs, known)

	// Block C: series x, newer correction at T (higher generations than A).
	cSamples := [][2]int64{{T, 2}}
	for i := int64(1); i < 120; i++ {
		cSamples = append(cSamples, [2]int64{5_000_000 + i*1000, 0})
	}
	flushSamples(t, bs, "x", cSamples)
	_ = onlyNewBlockID(t, bs, known) // block C — intentionally excluded from compaction

	// Block B: unrelated series y, newest of all (highest generations).
	ySamples := make([][2]int64, 0, 120)
	for i := int64(0); i < 120; i++ {
		ySamples = append(ySamples, [2]int64{7_000_000 + i*1000, 0})
	}
	flushSamples(t, bs, "y", ySamples)
	idB := onlyNewBlockID(t, bs, known)

	// Compact A + B only, leaving C (the newer correction of x) behind.
	group := []string{idA, idB}
	if n, err := bs.CompactOnce(func([]block.BlockInfo) [][]string { return [][]string{group} }); err != nil || n != 1 {
		t.Fatalf("CompactOnce = %d, %v; want 1, nil", n, err)
	}

	got, err := bs.QueryRange(metricFingerprint(t, "x"), T, T)
	if err != nil {
		t.Fatalf("QueryRange: %v", err)
	}
	if len(got) != 1 || got[0].Value != 2 {
		t.Fatalf("x@T after partial compaction = %v, want value 2 (the newer correction must still win)", got)
	}
}

// TestBlockStore_CompactOnce_PreservesLabelIndex verifies the merged block's
// regenerated postings/index answer label queries identically to the sources.
func TestBlockStore_CompactOnce_PreservesLabelIndex(t *testing.T) {
	dir := t.TempDir()
	bs, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}
	defer bs.Close()

	flushLabeledBlock(t, bs, 0,
		map[string]string{"__name__": "m", "host": "a"},
		map[string]string{"__name__": "m", "host": "b"})
	flushLabeledBlock(t, bs, 200000,
		map[string]string{"__name__": "m", "host": "a"},
		map[string]string{"__name__": "m", "host": "c"})

	wantHosts := []string{"a", "b", "c"}
	if before := selectHosts(t, bs); !equalStrings(before, wantHosts) {
		t.Fatalf("pre-compaction hosts = %v, want %v", before, wantHosts)
	}

	infos := bs.BlockInfos()
	group := []string{infos[0].ID, infos[1].ID}
	if n, err := bs.CompactOnce(func([]block.BlockInfo) [][]string { return [][]string{group} }); err != nil || n != 1 {
		t.Fatalf("CompactOnce = %d, %v; want 1, nil", n, err)
	}

	if got := selectHosts(t, bs); !equalStrings(got, wantHosts) {
		t.Fatalf("post-compaction SelectSeries hosts = %v, want %v", got, wantHosts)
	}
	if got := bs.LabelValues("host"); !equalStrings(got, wantHosts) {
		t.Fatalf("post-compaction LabelValues(host) = %v, want %v", got, wantHosts)
	}
	only, err := bs.SelectSeries(metrics.Selector{MetricName: "m", Matchers: []metrics.Matcher{{Name: "host", Value: "b"}}})
	if err != nil {
		t.Fatalf("SelectSeries(host=b): %v", err)
	}
	if len(only) != 1 {
		t.Fatalf("post-compaction SelectSeries(host=b) = %d series, want 1", len(only))
	}
}

// TestBlockStore_ApplyRetention_DeleteFailureKeepsReadable forces the reclaim
// rename to fail and asserts the block stays in the live set and queryable, and
// that the reported deletion count is 0.
func TestBlockStore_ApplyRetention_DeleteFailureKeepsReadable(t *testing.T) {
	dir := t.TempDir()
	bs, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}
	defer bs.Close()
	flushOneBlock(t, bs, "m", 0) // block MaxTime 119000

	// Replace tmp/ with a regular file so renaming blocks/<id> -> tmp/<id> fails.
	tmpDir := filepath.Join(dir, "metrics", "tmp")
	if err := os.RemoveAll(tmpDir); err != nil {
		t.Fatalf("rm tmp: %v", err)
	}
	if err := os.WriteFile(tmpDir, []byte("x"), 0o644); err != nil {
		t.Fatalf("write tmp file: %v", err)
	}

	n, err := bs.ApplyRetention(time.UnixMilli(10_000_000), time.Millisecond)
	if err == nil {
		t.Fatal("ApplyRetention: want a deletion error, got nil")
	}
	if n != 0 {
		t.Fatalf("ApplyRetention deleted=%d, want 0 (deletion failed)", n)
	}
	if got := len(bs.BlockInfos()); got != 1 {
		t.Fatalf("after failed deletion blocks = %d, want 1 (must stay readable)", got)
	}
	rng, err := bs.QueryRange(metricFingerprint(t, "m"), 0, 200000)
	if err != nil {
		t.Fatalf("QueryRange after failed deletion: %v", err)
	}
	if len(rng) != 120 {
		t.Fatalf("after failed deletion query = %d samples, want 120 (still readable)", len(rng))
	}
}

// TestBlockStore_ApplyRetention_CleanupFailureSurfaced forces the post-rename
// RemoveAll to fail and asserts the failure is returned (not swallowed) while the
// block still counts as reclaimed and leaves the live set.
func TestBlockStore_ApplyRetention_CleanupFailureSurfaced(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("runs as root; directory permissions are not enforced")
	}
	dir := t.TempDir()
	bs, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}
	defer bs.Close()
	flushOneBlock(t, bs, "m", 0)

	// Inject an undeletable subtree: a read-only directory containing a file. The
	// block dir itself stays writable, so renaming it into tmp/ succeeds, but the
	// recursive RemoveAll of the reclaimed copy fails on the read-only subdir.
	blockID := bs.BlockInfos()[0].ID
	badDir := filepath.Join(dir, "metrics", "blocks", blockID, "baddir")
	if err := os.Mkdir(badDir, 0o755); err != nil {
		t.Fatalf("mkdir baddir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(badDir, "f"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write baddir file: %v", err)
	}
	if err := os.Chmod(badDir, 0o555); err != nil {
		t.Fatalf("chmod baddir: %v", err)
	}
	// Restore perms so t.TempDir cleanup can remove the reclaimed tmp copy.
	t.Cleanup(func() {
		_ = os.Chmod(badDir, 0o755)
		_ = os.Chmod(filepath.Join(dir, "metrics", "tmp", blockID, "baddir"), 0o755)
	})

	n, err := bs.ApplyRetention(time.UnixMilli(10_000_000), time.Millisecond)
	if err == nil {
		t.Fatal("ApplyRetention: want a surfaced cleanup error, got nil")
	}
	if n != 1 {
		t.Fatalf("ApplyRetention deleted=%d, want 1 (reclaimed from the live set)", n)
	}
	if got := len(bs.BlockInfos()); got != 0 {
		t.Fatalf("after reclaim blocks = %d, want 0 (gone from live set)", got)
	}
}

// TestBlockStore_ApplyRetention_ConcurrentQueriesNeverError exercises the
// lock-drain: queries issued continuously while retention deletes blocks never error.
func TestBlockStore_ApplyRetention_ConcurrentQueriesNeverError(t *testing.T) {
	dir := t.TempDir()
	bs, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}
	defer bs.Close()
	flushOneBlock(t, bs, "m", 0)
	flushOneBlock(t, bs, "m", 200000)
	id := metricFingerprint(t, "m")

	started, stop, wait := startQueryLoop(t, func() error {
		got, err := bs.QueryRange(id, 0, 400000)
		if err != nil {
			return err
		}
		// The block-set swap is atomic under the write lock, so a concurrent query
		// sees either both blocks (240) or neither (0) — never a torn partial read.
		if len(got) != 0 && len(got) != 240 {
			return fmt.Errorf("torn read during retention: %d samples", len(got))
		}
		return nil
	})
	<-started // a query has completed; the goroutine keeps querying through retention
	if _, err := bs.ApplyRetention(time.UnixMilli(10_000_000), time.Millisecond); err != nil {
		t.Fatalf("ApplyRetention: %v", err)
	}
	stop()
	wait()
}

// TestBlockStore_Deletion_LeavesNoPartialDir asserts compaction source deletion
// and retention deletion both leave the blocks and tmp directories clean.
func TestBlockStore_Deletion_LeavesNoPartialDir(t *testing.T) {
	dir := t.TempDir()
	bs, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}
	defer bs.Close()
	blocksDir := filepath.Join(dir, "metrics", "blocks")
	tmpDir := filepath.Join(dir, "metrics", "tmp")

	flushOneBlock(t, bs, "m", 0)
	flushOneBlock(t, bs, "m", 200000)

	infos := bs.BlockInfos()
	group := []string{infos[0].ID, infos[1].ID}
	if n, err := bs.CompactOnce(func([]block.BlockInfo) [][]string { return [][]string{group} }); err != nil || n != 1 {
		t.Fatalf("CompactOnce = %d, %v; want 1, nil", n, err)
	}
	if got := countDirs(t, blocksDir); got != 1 {
		t.Fatalf("after compaction blocks/ has %d dirs, want 1 (sources safe-deleted)", got)
	}
	if got := countDirs(t, tmpDir); got != 0 {
		t.Fatalf("after compaction tmp/ has %d dirs, want 0", got)
	}

	if _, err := bs.ApplyRetention(time.UnixMilli(10_000_000), time.Millisecond); err != nil {
		t.Fatalf("ApplyRetention: %v", err)
	}
	if got := countDirs(t, blocksDir); got != 0 {
		t.Fatalf("after retention blocks/ has %d dirs, want 0", got)
	}
	if got := countDirs(t, tmpDir); got != 0 {
		t.Fatalf("after retention tmp/ has %d dirs, want 0", got)
	}
}
