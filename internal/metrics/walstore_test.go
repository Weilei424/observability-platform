package metrics_test

import (
	"errors"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/storage/wal"
)

// failingWriter always returns an error from WriteRecord.
type failingWriter struct{}

func (f *failingWriter) WriteRecord(_ []wal.LabelPair, _ int64, _ float64) error {
	return errors.New("simulated WAL write failure")
}

func (f *failingWriter) SegmentIndex() int { return 0 }

func TestWALStore_AppendFailsWhenWALFails(t *testing.T) {
	dataDir := t.TempDir()
	bs, err := metrics.NewBlockStore(dataDir)
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}
	store := metrics.NewWALStore(&failingWriter{}, bs, dataDir)

	labels, err := metrics.NewLabels(map[string]string{"__name__": "cpu_usage", "env": "test"})
	if err != nil {
		t.Fatalf("NewLabels: %v", err)
	}

	if err := store.Append(labels, 1000, 1.0); err == nil {
		t.Fatal("expected error when WAL write fails, got nil")
	}

	series := bs.SelectSeries(metrics.Selector{MetricName: "cpu_usage"})
	if len(series) != 0 {
		t.Errorf("BlockStore has %d series after failed WAL write, want 0", len(series))
	}
}

func TestWALStore_AppendDelegatesToMemory(t *testing.T) {
	dir := t.TempDir()
	walDir := dir + "/metrics/wal"
	w, err := wal.Open(walDir, 128<<20, 1)
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	defer w.Close()

	bs, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}
	store := metrics.NewWALStore(w, bs, dir)

	labels, err := metrics.NewLabels(map[string]string{"__name__": "req_total", "service": "api"})
	if err != nil {
		t.Fatalf("NewLabels: %v", err)
	}
	if err := store.Append(labels, 1000, 42.0); err != nil {
		t.Fatalf("Append: %v", err)
	}

	series := bs.SelectSeries(metrics.Selector{MetricName: "req_total"})
	if len(series) != 1 {
		t.Fatalf("BlockStore has %d series, want 1", len(series))
	}
}

func TestWALStore_ReplayRestoresSeries(t *testing.T) {
	dir := t.TempDir()
	walDir := dir + "/metrics/wal"
	w1, err := wal.Open(walDir, 128<<20, 1)
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}

	bs1, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}
	store1 := metrics.NewWALStore(w1, bs1, dir)

	labels, _ := metrics.NewLabels(map[string]string{"__name__": "disk_read_bytes", "device": "sda"})
	samples := [][2]float64{{1000, 100}, {2000, 200}, {3000, 300}}
	for _, s := range samples {
		if err := store1.Append(labels, int64(s[0]), s[1]); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := w1.Close(); err != nil {
		t.Fatalf("Close wal: %v", err)
	}

	// Replay WAL into a fresh BlockStore.
	bs2, err := metrics.NewBlockStore(dir)
	if err != nil {
		t.Fatalf("NewBlockStore: %v", err)
	}
	checkpoint := metrics.ReadCheckpoint(dir)
	if err := wal.ReplayFrom(walDir, checkpoint, func(pairs []wal.LabelPair, tsMs int64, value float64) {
		lm := make(map[string]string, len(pairs))
		for _, p := range pairs {
			lm[p.Name] = p.Value
		}
		lbs, err := metrics.NewLabels(lm)
		if err != nil {
			t.Errorf("NewLabels during replay: %v", err)
			return
		}
		if err := bs2.Append(lbs, tsMs, value); err != nil {
			t.Errorf("Append during replay: %v", err)
		}
	}); err != nil {
		t.Fatalf("ReplayFrom: %v", err)
	}

	id := labels.Fingerprint()
	got := bs2.QueryRange(id, 1000, 3000)
	if len(got) != 3 {
		t.Fatalf("QueryRange returned %d samples after replay, want 3", len(got))
	}
}
