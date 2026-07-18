package metrics

import (
	"fmt"
	"regexp"
)

var (
	labelNameRe  = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	metricNameRe = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)
)

// validateMetricName enforces the metrics-domain requirement that m contains a
// valid __name__. Generic label-name/value validation lives in internal/labels.
func validateMetricName(m map[string]string) error {
	name, ok := m["__name__"]
	if !ok {
		return &ValidationError{Field: "__name__", Message: "required"}
	}
	if !metricNameRe.MatchString(name) {
		return &ValidationError{Field: "__name__", Message: fmt.Sprintf("invalid metric name %q", name)}
	}
	if len(name) > 65535 {
		return &ValidationError{Field: "__name__", Message: "metric name exceeds 65535-byte limit"}
	}
	return nil
}

// ValidateSample checks that a Sample is acceptable for ingestion.
// All float64 values (including NaN, +Inf, -Inf) and all int64 timestamps are accepted.
func ValidateSample(s Sample) error {
	return nil
}
