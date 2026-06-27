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
// Gen is the sample's write generation, used to resolve last-write-wins for equal
// timestamps across memory and blocks. It is an internal dedup key, not part of
// query output.
type Sample struct {
	SeriesID    SeriesID
	TimestampMs int64
	Value       float64
	Gen         int64
}
