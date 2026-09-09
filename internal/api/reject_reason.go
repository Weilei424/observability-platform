package api

// metricRejectReason maps a validation error's Field onto a closed set of metric
// label values.
//
// Field must never reach a metric label directly: internal/labels/validation.go
// builds most of its errors with Field set to the client-supplied label name, so
// forwarding it would let a client grow the registry without bound by posting
// junk label names — a memory-growth attack through the observability endpoint.
// Every unrecognised field collapses into an existing bucket.
//
// This function only classifies per-item validation failures. Two more reasons
// in the closed set are assigned directly by the ingest handler, never through
// this function: "append" (the item passed validation but the write to storage
// failed) and "batch" (the sample was itself valid but was discarded only
// because a sibling in the same atomically-rejected batch was invalid).
//
// "batch" here never means an abandoned append tail: handleIngestMetrics
// attempts every append even after one fails, so a failed write is always
// "append". Only the Loki push path abandons a tail — see logRejectReason.
// The full closed set is: name, timestamp, value, labels, other, append, batch.
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
//
// "batch" is broader on this path than on the metrics one: besides collateral
// from an atomically-rejected batch, handleLokiPush returns on the first append
// failure, so every line after it is abandoned and counted as "batch" too.
//
// As with metricRejectReason, "append" and "batch" are assigned directly by the
// push handler rather than through this function — see metricRejectReason's
// comment for what each means. The full closed set is: values, timestamp, line,
// labels, other, append, batch.
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
