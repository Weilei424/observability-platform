package logs

import "github.com/masonwheeler/observability-platform/internal/labels"

// StreamID is a uint64 fingerprint derived from a normalized stream label set.
type StreamID uint64

// StreamLabels identifies a log stream. It is the shared labels type; unlike a
// metric, a stream carries no __name__.
type StreamLabels = labels.Labels

// ValidationError is the shared typed validation error.
type ValidationError = labels.ValidationError

// LogEntry is a single timestamped log line belonging to a stream.
type LogEntry struct {
	StreamID    StreamID
	TimestampNs int64 // Unix nanoseconds (Loki-native precision)
	Line        string
}
