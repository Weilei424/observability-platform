package observability

import (
	"strings"
	"testing"

	dto "github.com/prometheus/client_model/go"
)

type fakeCard struct{ s, n, p int }

func (f fakeCard) Cardinality() (int, int, int) { return f.s, f.n, f.p }

type fakeStorage struct {
	blocks int
	bytes  int64
}

func (f fakeStorage) StorageStats() (int, int64) { return f.blocks, f.bytes }

func TestNewRegistry_ExposesCardinality(t *testing.T) {
	reg, _ := NewRegistry(RegistryOptions{Cardinality: fakeCard{s: 5, n: 3, p: 7}})
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
	reg, inst := NewRegistry(RegistryOptions{Cardinality: fakeCard{s: 1, n: 1, p: 1}, Storage: fakeStorage{blocks: 4, bytes: 2048}})
	if inst == nil {
		t.Fatal("expected non-nil Instruments handle")
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
	reg, inst := NewRegistry(RegistryOptions{Cardinality: fakeCard{}, Storage: fakeStorage{}})
	inst.Maintenance.CompactionsTotal.Inc()
	inst.Maintenance.FlushesTotal.Add(2)
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

// TestNewRegistry_FailureCountersScrapeZeroBeforeAnyFailure guards against the
// absent -> 1 gap: a CounterVec label child does not exist until its first
// Inc, so without preinitialization Prometheus would see the series jump
// straight from absent to 1 on the very first failure, and rate() needs two
// points in its window to render anything from that jump — a single 500, a
// single rejected sample, or a single collector error could be invisible on
// a dashboard whose only job is to show failures. NewIngestMetrics
// preinitializes every closed-set reason to zero, so the real sequence
// across scrapes is baseline 0, then 1 after one failure, never absent -> 1.
func TestNewRegistry_FailureCountersScrapeZeroBeforeAnyFailure(t *testing.T) {
	reg, inst := NewRegistry(RegistryOptions{Cardinality: fakeCard{}})

	// Baseline scrape, before any failure has happened at all.
	baseline, err := reg.Gather()
	if err != nil {
		t.Fatalf("baseline Gather: %v", err)
	}
	if got := counterValue(baseline, "obs_samples_rejected_total", "reason", "value"); got == nil {
		t.Fatal(`baseline: obs_samples_rejected_total{reason="value"} is absent, want present at 0`)
	} else if *got != 0 {
		t.Fatalf(`baseline obs_samples_rejected_total{reason="value"} = %v, want 0`, *got)
	}
	if got := counterValue(baseline, "obs_log_lines_rejected_total", "reason", "line"); got == nil {
		t.Fatal(`baseline: obs_log_lines_rejected_total{reason="line"} is absent, want present at 0`)
	} else if *got != 0 {
		t.Fatalf(`baseline obs_log_lines_rejected_total{reason="line"} = %v, want 0`, *got)
	}

	// A single failure event of each kind.
	inst.Ingest.SamplesRejected.WithLabelValues("value").Inc()
	inst.Ingest.LogLinesRejected.WithLabelValues("line").Inc()

	// Follow-up scrape: the series read 0 -> 1, never absent -> 1.
	followUp, err := reg.Gather()
	if err != nil {
		t.Fatalf("follow-up Gather: %v", err)
	}
	if got := counterValue(followUp, "obs_samples_rejected_total", "reason", "value"); got == nil || *got != 1 {
		t.Fatalf(`after one failure obs_samples_rejected_total{reason="value"} = %v, want 1`, got)
	}
	if got := counterValue(followUp, "obs_log_lines_rejected_total", "reason", "line"); got == nil || *got != 1 {
		t.Fatalf(`after one failure obs_log_lines_rejected_total{reason="line"} = %v, want 1`, got)
	}
	// An untouched reason still reads 0, not absent, on the follow-up scrape too.
	if got := counterValue(followUp, "obs_samples_rejected_total", "reason", "name"); got == nil || *got != 0 {
		t.Fatalf(`after one failure obs_samples_rejected_total{reason="name"} = %v, want 0 (untouched, not absent)`, got)
	}
}

// counterValue returns the value of the counter metric family "name" whose
// labels include labelName=labelValue, or nil if no matching series exists
// in families at all — as opposed to a series that exists but reads zero.
func counterValue(families []*dto.MetricFamily, name, labelName, labelValue string) *float64 {
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == labelName && lp.GetValue() == labelValue {
					v := m.GetCounter().GetValue()
					return &v
				}
			}
		}
	}
	return nil
}
