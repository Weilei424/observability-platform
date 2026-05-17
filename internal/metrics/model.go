package metrics

// SeriesID is a uint64 fingerprint derived from a normalized label set.
type SeriesID uint64

// Label is a single name/value pair.
type Label struct {
	Name  string
	Value string
}

// Labels is an immutable, normalized set of labels with a cached fingerprint.
// Always construct via NewLabels — never create the zero value directly.
type Labels struct {
	pairs []Label
	fp    SeriesID
}

// Sample is a single timestamped float64 value belonging to a series.
type Sample struct {
	SeriesID    SeriesID
	TimestampMs int64
	Value       float64
}
