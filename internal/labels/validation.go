package labels

import (
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"
)

var labelNameRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// ValidationError is the shared typed error returned by label validation.
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation error: %s: %s", e.Field, e.Message)
}

// validateLabelMap applies generic label rules. __name__ is permitted as a label
// name but is NOT required, and its value is validated only generically — the
// metric-name charset rule is a metrics-domain concern enforced by the caller.
// Limits match the WAL encoding constraints: ≤255 labels, ≤255-byte name,
// ≤65535-byte value.
func validateLabelMap(m map[string]string) error {
	if len(m) > 255 {
		return &ValidationError{Field: "labels", Message: "too many labels: maximum is 255"}
	}
	for k, v := range m {
		if k != "__name__" {
			if strings.HasPrefix(k, "__") {
				return &ValidationError{Field: k, Message: "label name with __ prefix is reserved"}
			}
			if !labelNameRe.MatchString(k) {
				return &ValidationError{Field: k, Message: fmt.Sprintf("invalid label name %q", k)}
			}
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
