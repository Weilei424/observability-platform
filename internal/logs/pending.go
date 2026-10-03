package logs

// PendingEntry is a validated log line not yet appended: what the push handler
// holds between validating a batch and applying it, locally or through the ring.
type PendingEntry struct {
	Labels      StreamLabels
	TimestampNs int64
	Line        string
}
