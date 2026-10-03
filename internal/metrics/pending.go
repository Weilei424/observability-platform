package metrics

// PendingSample is a validated sample not yet appended: what a write handler
// holds between validating a batch and applying it, locally or through the ring.
type PendingSample struct {
	Labels      Labels
	TimestampMs int64
	Value       float64
}
