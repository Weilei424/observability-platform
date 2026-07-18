package logs

import "github.com/masonwheeler/observability-platform/internal/labels"

// NewStreamLabels validates a stream label set: the generic shared label rules
// plus the requirement that at least one label is present (empty {} is rejected).
// __name__ is not required and carries no special meaning for streams.
func NewStreamLabels(m map[string]string) (StreamLabels, error) {
	if len(m) == 0 {
		return StreamLabels{}, &ValidationError{Field: "labels", Message: "stream must have at least one label"}
	}
	return labels.New(m)
}

// StreamIDOf derives the stream fingerprint from a stream label set.
func StreamIDOf(l StreamLabels) StreamID {
	return StreamID(l.Hash())
}
