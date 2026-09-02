package observability

import (
	"strings"
	"testing"
)

type fakeCard struct{ s, n, p int }

func (f fakeCard) Cardinality() (int, int, int) { return f.s, f.n, f.p }

type fakeStorage struct {
	blocks int
	bytes  int64
}

func (f fakeStorage) StorageStats() (int, int64) { return f.blocks, f.bytes }

func TestNewRegistry_ExposesCardinality(t *testing.T) {
	reg, _ := NewRegistry(fakeCard{s: 5, n: 3, p: 7}, nil)
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	want := map[string]float64{
		"obs_active_series":     5,
		"obs_label_names_total": 3,
		"obs_label_pairs_total": 7,
	}
	got := map[string]float64{}
	for _, mf := range mfs {
		if !strings.HasPrefix(mf.GetName(), "obs_") {
			continue
		}
		got[mf.GetName()] = mf.GetMetric()[0].GetGauge().GetValue()
	}
	for name, v := range want {
		if got[name] != v {
			t.Fatalf("%s = %v, want %v", name, got[name], v)
		}
	}
}

func TestNewRegistry_ExposesStorageStats(t *testing.T) {
	reg, m := NewRegistry(fakeCard{s: 1, n: 1, p: 1}, fakeStorage{blocks: 4, bytes: 2048})
	if m == nil {
		t.Fatal("expected non-nil Metrics handle")
	}
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	got := map[string]float64{}
	for _, mf := range mfs {
		if len(mf.GetMetric()) > 0 && mf.GetMetric()[0].GetGauge() != nil {
			got[mf.GetName()] = mf.GetMetric()[0].GetGauge().GetValue()
		}
	}
	if got["obs_blocks_total"] != 4 || got["obs_blocks_bytes"] != 2048 {
		t.Fatalf("storage gauges = %v, want blocks 4 / bytes 2048", got)
	}
}

func TestNewRegistry_PushMetricsRegistered(t *testing.T) {
	reg, m := NewRegistry(fakeCard{}, fakeStorage{})
	m.CompactionsTotal.Inc()
	m.FlushesTotal.Add(2)
	mfs, _ := reg.Gather()
	names := map[string]bool{}
	for _, mf := range mfs {
		names[mf.GetName()] = true
	}
	for _, want := range []string{"obs_compactions_total", "obs_compaction_duration_seconds", "obs_retention_deleted_blocks_total", "obs_flushes_total", "obs_flush_failures_total", "obs_compaction_failures_total"} {
		if !names[want] {
			t.Errorf("missing metric %s", want)
		}
	}
}
