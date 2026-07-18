package metrics

import "github.com/masonwheeler/observability-platform/internal/labels"

// SeriesID is a uint64 fingerprint derived from a normalized label set.
type SeriesID uint64

// Label is a single name/value pair (shared labels type).
type Label = labels.Label

// Labels is the shared, immutable, normalized label set with a cached fingerprint.
// Construct metric label sets via NewLabels (which requires __name__); construct
// aggregation output labels via newOutputLabels.
type Labels = labels.Labels

// ValidationError is the shared typed validation error.
type ValidationError = labels.ValidationError

// Sample is a single timestamped float64 value belonging to a series.
// Gen is the sample's write generation, used to resolve last-write-wins for equal
// timestamps across memory and blocks. It is an internal dedup key, not part of
// query output.
type Sample struct {
	SeriesID    SeriesID
	TimestampMs int64
	Value       float64
	Gen         int64
}
