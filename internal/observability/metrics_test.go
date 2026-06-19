package observability

import (
	"strings"
	"testing"
)

type fakeCard struct{ s, n, p int }

func (f fakeCard) Cardinality() (int, int, int) { return f.s, f.n, f.p }

func TestNewRegistry_ExposesCardinality(t *testing.T) {
	reg := NewRegistry(fakeCard{s: 5, n: 3, p: 7})
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
