package logs

import "fmt"

// MaxLineBytes is the maximum accepted log line size, matching Loki's default
// max_line_size (256 KiB).
const MaxLineBytes = 256 * 1024

// ValidateEntry checks a LogEntry's timestamp and line size. Stream-label
// validation is performed separately by NewStreamLabels.
//
// Out-of-order policy: the model does NOT enforce monotonic append. Out-of-order
// lines are accepted; ordering is resolved at query time by timestamp. An exact
// duplicate is same stream + same timestamp + same line; same timestamp with a
// different line keeps both. Buffering and enforcement are ingest/storage
// concerns handled in Phases 4.2/4.3.
func ValidateEntry(e LogEntry) error {
	if e.TimestampNs <= 0 {
		return &ValidationError{Field: "timestamp_ns", Message: "must be a positive Unix nanosecond timestamp"}
	}
	if len(e.Line) > MaxLineBytes {
		return &ValidationError{Field: "line", Message: fmt.Sprintf("log line exceeds %d-byte limit", MaxLineBytes)}
	}
	return nil
}
