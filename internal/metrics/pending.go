package metrics

// PendingSample is a validated sample not yet appended: what a write handler
// holds between validating a batch and applying it, locally or through the ring.
type PendingSample struct {
	Labels      Labels
	TimestampMs int64
	Value       float64
	// Gen is the write generation the gateway stamped at admission, carried to
	// every replica so they all store the same one; 0 means the ingester
	// assigns its own.
	Gen int64
}
