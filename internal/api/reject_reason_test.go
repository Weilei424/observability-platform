package api

import "testing"

// The security-relevant test. ValidationError.Field carries the CLIENT-SUPPLIED
// label name on most of its construction sites (internal/labels/validation.go),
// so a classifier that passes Field through lets any client mint an unbounded
// number of metric label values by posting garbage label names.
func TestRejectReasonsAreABoundedSet(t *testing.T) {
	metricAllowed := map[string]bool{"name": true, "labels": true, "timestamp": true, "value": true, "append": true, "other": true}
	logAllowed := map[string]bool{"labels": true, "timestamp": true, "line": true, "values": true, "append": true, "other": true}

	hostile := []string{
		"user_supplied_label_0", "\x00", "a very long label name that a client chose",
		"__name__ evil", "", "sess_9f2b1c",
	}
	for _, f := range hostile {
		if got := metricRejectReason(f); !metricAllowed[got] {
			t.Errorf("metricRejectReason(%q) = %q, which is outside the allowed set", f, got)
		}
		if got := logRejectReason(f); !logAllowed[got] {
			t.Errorf("logRejectReason(%q) = %q, which is outside the allowed set", f, got)
		}
	}
}

func TestMetricRejectReasonMapping(t *testing.T) {
	cases := map[string]string{
		"__name__":     "name",
		"timestamp_ms": "timestamp",
		"value":        "value",
		"unknown":      "other",
		"labels":       "labels",
		"service":      "labels", // a client-supplied label name
	}
	for field, want := range cases {
		if got := metricRejectReason(field); got != want {
			t.Errorf("metricRejectReason(%q) = %q, want %q", field, got, want)
		}
	}
}

func TestLogRejectReasonMapping(t *testing.T) {
	cases := map[string]string{
		"values":       "values",
		"timestamp":    "timestamp",
		"timestamp_ns": "timestamp",
		"line":         "line",
		"unknown":      "other",
		"stream":       "labels",
		"service":      "labels", // a client-supplied label name
	}
	for field, want := range cases {
		if got := logRejectReason(field); got != want {
			t.Errorf("logRejectReason(%q) = %q, want %q", field, got, want)
		}
	}
}
