package api

// metricRejectReason maps a validation error's Field onto a closed set of metric
// label values.
//
// Field must never reach a metric label directly: internal/labels/validation.go
// builds most of its errors with Field set to the client-supplied label name, so
// forwarding it would let a client grow the registry without bound by posting
// junk label names — a memory-growth attack through the observability endpoint.
// Every unrecognised field collapses into an existing bucket.
func metricRejectReason(field string) string {
	switch field {
	case "__name__":
		return "name"
	case "timestamp_ms":
		return "timestamp"
	case "value":
		return "value"
	case "unknown":
		// The handler's fallback for a non-ValidationError error.
		return "other"
	default:
		// Everything else NewLabels produces is either the literal "labels" or a
		// label name, and both are label problems.
		return "labels"
	}
}

// logRejectReason is metricRejectReason's counterpart for the Loki push path. Same
// rule: Field is client-influenced and never becomes a label value unchanged.
func logRejectReason(field string) string {
	switch field {
	case "values":
		return "values"
	case "timestamp", "timestamp_ns":
		return "timestamp"
	case "line":
		return "line"
	case "unknown":
		return "other"
	default:
		// "stream", or a client-supplied stream label name.
		return "labels"
	}
}
