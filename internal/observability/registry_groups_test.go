package observability

import (
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func families(t *testing.T, opts RegistryOptions) map[string]bool {
	t.Helper()
	reg, _ := NewRegistry(opts)
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]bool{}
	for _, mf := range mfs {
		out[mf.GetName()] = true
	}
	return out
}

func TestOmittedGroupsAreNotRegistered(t *testing.T) {
	groups := map[InstrumentGroups][]string{
		FlushGroup:      {"obs_flushes_total", "obs_flush_failures_total"},
		CompactionGroup: {"obs_compactions_total", "obs_compaction_failures_total", "obs_compaction_duration_seconds", "obs_retention_deleted_blocks_total"},
		IngestGroup:     {"obs_samples_ingested_total", "obs_log_lines_ingested_total", "obs_samples_rejected_total", "obs_log_lines_rejected_total"},
		LogFlushGroup:   {"obs_log_flushes_total", "obs_log_flush_failures_total"},
	}
	all := families(t, RegistryOptions{})
	for _, names := range groups {
		for _, n := range names {
			if !all[n] {
				t.Errorf("the zero-value registry lacks %s; all-in-one must keep every group", n)
			}
		}
	}
	for g, names := range groups {
		got := families(t, RegistryOptions{Omit: g})
		for _, n := range names {
			if got[n] {
				t.Errorf("omitting group %b still registers %s", g, n)
			}
		}
	}
	if none := families(t, RegistryOptions{Omit: AllGroups}); none["obs_flushes_total"] || none["obs_samples_ingested_total"] {
		t.Error("AllGroups left a push-model group registered")
	}
}

func TestLogFlushObserveCountsSuccessesAndFailures(t *testing.T) {
	m := NewLogFlushMetrics()
	m.Observe(nil)
	m.Observe(nil)
	m.Observe(errors.New("store down"))
	if got := testutil.ToFloat64(m.Flushes); got != 2 {
		t.Errorf("flushes = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.Failures); got != 1 {
		t.Errorf("failures = %v, want 1", got)
	}
}
