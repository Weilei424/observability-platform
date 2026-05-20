package metrics

import (
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"
)

var (
	labelNameRe  = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	metricNameRe = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)
)

// ValidationError is a typed error returned by label and sample validation.
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation error: %s: %s", e.Field, e.Message)
}

// validateLabelMap checks that m contains a valid __name__ and valid label names and values.
// Limits are sized to match WAL encoding constraints: ≤255 total labels, ≤255-byte name, ≤65535-byte value.
func validateLabelMap(m map[string]string) error {
	if len(m) > 255 {
		return &ValidationError{Field: "labels", Message: "too many labels: maximum is 255"}
	}
	name, ok := m["__name__"]
	if !ok {
		return &ValidationError{Field: "__name__", Message: "required"}
	}
	if !metricNameRe.MatchString(name) {
		return &ValidationError{Field: "__name__", Message: fmt.Sprintf("invalid metric name %q", name)}
	}
	for k, v := range m {
		if k == "__name__" {
			continue
		}
		if strings.HasPrefix(k, "__") {
			return &ValidationError{Field: k, Message: "label name with __ prefix is reserved"}
		}
		if !labelNameRe.MatchString(k) {
			return &ValidationError{Field: k, Message: fmt.Sprintf("invalid label name %q", k)}
		}
		if len(k) > 255 {
			return &ValidationError{Field: k, Message: "label name exceeds 255-byte limit"}
		}
		if !utf8.ValidString(v) {
			return &ValidationError{Field: k, Message: "label value must be valid UTF-8"}
		}
		if len(v) > 65535 {
			return &ValidationError{Field: k, Message: "label value exceeds 65535-byte limit"}
		}
	}
	return nil
}

// ValidateSample checks that a Sample is acceptable for ingestion.
// All float64 values (including NaN, +Inf, -Inf) and all int64 timestamps are accepted.
func ValidateSample(s Sample) error {
	return nil
}
