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

func TestWALStore_AppendFailsWhenWALFails(t *testing.T) {
	mem := metrics.NewMemoryStore()
	store := metrics.NewWALStore(&failingWriter{}, mem)

	labels, err := metrics.NewLabels(map[string]string{"__name__": "cpu_usage", "env": "test"})
	if err != nil {
		t.Fatalf("NewLabels: %v", err)
	}

	if err := store.Append(labels, 1000, 1.0); err == nil {
		t.Fatal("expected error when WAL write fails, got nil")
	}

	// MemoryStore must not have been written.
	series := mem.SelectSeries(metrics.Selector{MetricName: "cpu_usage"})
	if len(series) != 0 {
		t.Errorf("MemoryStore has %d series after failed WAL write, want 0", len(series))
	}
}

func TestWALStore_AppendDelegatesToMemory(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(dir, 128<<20, 1)
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	defer w.Close()

	mem := metrics.NewMemoryStore()
	store := metrics.NewWALStore(w, mem)

	labels, err := metrics.NewLabels(map[string]string{"__name__": "req_total", "service": "api"})
	if err != nil {
		t.Fatalf("NewLabels: %v", err)
	}
	if err := store.Append(labels, 1000, 42.0); err != nil {
		t.Fatalf("Append: %v", err)
	}

	series := mem.SelectSeries(metrics.Selector{MetricName: "req_total"})
	if len(series) != 1 {
		t.Fatalf("MemoryStore has %d series, want 1", len(series))
	}
}

func TestWALStore_ReplayRestoresSeries(t *testing.T) {
	dir := t.TempDir()
	w1, err := wal.Open(dir, 128<<20, 1)
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}

	mem1 := metrics.NewMemoryStore()
	store1 := metrics.NewWALStore(w1, mem1)

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

	// Replay WAL into a fresh MemoryStore.
	mem2 := metrics.NewMemoryStore()
	if err := wal.Replay(dir, func(pairs []wal.LabelPair, tsMs int64, value float64) {
		lm := make(map[string]string, len(pairs))
		for _, p := range pairs {
			lm[p.Name] = p.Value
		}
		lbs, err := metrics.NewLabels(lm)
		if err != nil {
			t.Errorf("NewLabels during replay: %v", err)
			return
		}
		if err := mem2.Append(lbs, tsMs, value); err != nil {
			t.Errorf("Append during replay: %v", err)
		}
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}

	id := labels.Fingerprint()
	got := mem2.QueryRange(id, 1000, 3000)
	if len(got) != 3 {
		t.Fatalf("QueryRange returned %d samples after replay, want 3", len(got))
	}
	for i, s := range got {
		if s.TimestampMs != int64(samples[i][0]) || s.Value != samples[i][1] {
			t.Errorf("sample[%d] = {%d, %v}, want {%d, %v}",
				i, s.TimestampMs, s.Value, int64(samples[i][0]), samples[i][1])
		}
	}
}
