package metrics

import "github.com/masonwheeler/observability-platform/internal/labels"

// NewLabels validates, normalizes, and fingerprints a metric label set. It
// enforces the metrics-domain rule that __name__ is present and a valid metric
// name, then delegates generic validation and construction to the shared package.
func NewLabels(m map[string]string) (Labels, error) {
	if err := validateMetricName(m); err != nil {
		return Labels{}, err
	}
	return labels.New(m)
}

// newOutputLabels builds a Labels for aggregation output. Unlike NewLabels,
// __name__ is not required and no validation is performed.
func newOutputLabels(m map[string]string) Labels {
	return labels.NewUnvalidated(m)
}
