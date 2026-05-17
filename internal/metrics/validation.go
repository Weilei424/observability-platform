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
func validateLabelMap(m map[string]string) error {
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
		if !utf8.ValidString(v) {
			return &ValidationError{Field: k, Message: "label value must be valid UTF-8"}
		}
	}
	return nil
}

// ValidateSample checks that a Sample is acceptable for ingestion.
// All float64 values (including NaN, +Inf, -Inf) and all int64 timestamps are accepted.
func ValidateSample(s Sample) error {
	return nil
}
