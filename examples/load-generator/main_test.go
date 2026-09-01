package main

import (
	"maps"
	"testing"
)

// TestWithInstance covers the per-pod series identity that makes
// loadGenerator.replicas > 1 safe. The counters in main are process-local, so
// two replicas writing the same label set are two independent counters under one
// series identity — interleaved samples at identical timestamps, which rate()
// reads as repeated counter resets.
func TestWithInstance(t *testing.T) {
	base := map[string]string{"service": "api", "method": "GET", "status": "200"}

	t.Run("omitted when unset", func(t *testing.T) {
		got := withInstance("", maps.Clone(base))
		if v, ok := got["instance"]; ok {
			t.Errorf("instance = %q with none configured; Compose and `go run` must keep their existing series", v)
		}
		if len(got) != len(base) {
			t.Errorf("labels = %v, want %v unchanged", got, base)
		}
	})

	t.Run("added when set", func(t *testing.T) {
		got := withInstance("observability-producers-load-generator-abc12", maps.Clone(base))
		if got["instance"] != "observability-producers-load-generator-abc12" {
			t.Errorf("instance = %q, want the configured pod name", got["instance"])
		}
		for k, v := range base {
			if got[k] != v {
				t.Errorf("label %q = %q, want %q — the base labels must survive", k, got[k], v)
			}
		}
	})

	t.Run("two replicas produce distinct label sets", func(t *testing.T) {
		a := withInstance("pod-a", maps.Clone(base))
		b := withInstance("pod-b", maps.Clone(base))
		if maps.Equal(a, b) {
			t.Errorf("both replicas produced the identical label set %v", a)
		}
	})
}
