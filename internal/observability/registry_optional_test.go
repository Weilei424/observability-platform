package observability

import "testing"

// Stateless components have no series to count: a registry without a
// cardinality source must omit the gauges, never report a zero.
func TestRegistryWithoutCardinalityOmitsTheGauges(t *testing.T) {
	reg, _ := NewRegistry(RegistryOptions{})
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		switch mf.GetName() {
		case "obs_active_series", "obs_label_names_total", "obs_label_pairs_total":
			t.Errorf("%s registered with no cardinality source", mf.GetName())
		}
	}
}
