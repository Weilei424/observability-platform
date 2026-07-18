package logs_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/logs"
)

func TestNewStreamLabels_SameLabelsDifferentOrder_SameID(t *testing.T) {
	a, err := logs.NewStreamLabels(map[string]string{"service": "api", "level": "error"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	b, err := logs.NewStreamLabels(map[string]string{"level": "error", "service": "api"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if logs.StreamIDOf(a) != logs.StreamIDOf(b) {
		t.Errorf("expected same StreamID, got %d vs %d", logs.StreamIDOf(a), logs.StreamIDOf(b))
	}
}

func TestNewStreamLabels_DifferentLabels_DifferentID(t *testing.T) {
	a, _ := logs.NewStreamLabels(map[string]string{"service": "api"})
	b, _ := logs.NewStreamLabels(map[string]string{"service": "web"})
	if logs.StreamIDOf(a) == logs.StreamIDOf(b) {
		t.Error("expected different StreamIDs for different label sets")
	}
}

func TestNewStreamLabels_Empty_Rejected(t *testing.T) {
	_, err := logs.NewStreamLabels(map[string]string{})
	if err == nil {
		t.Fatal("expected error for empty stream label set")
	}
	var ve *logs.ValidationError
	if !errors.As(err, &ve) {
		t.Errorf("expected *ValidationError, got %T", err)
	}
}

func TestNewStreamLabels_InvalidLabel_Rejected(t *testing.T) {
	_, err := logs.NewStreamLabels(map[string]string{"bad-name": "v"})
	if err == nil {
		t.Fatal("expected error for invalid label name")
	}
	var ve *logs.ValidationError
	if !errors.As(err, &ve) {
		t.Errorf("expected *ValidationError, got %T", err)
	}
}

func TestValidateEntry_Timestamp(t *testing.T) {
	if err := logs.ValidateEntry(logs.LogEntry{TimestampNs: 0, Line: "x"}); err == nil {
		t.Error("expected error for zero timestamp")
	}
	if err := logs.ValidateEntry(logs.LogEntry{TimestampNs: -1, Line: "x"}); err == nil {
		t.Error("expected error for negative timestamp")
	}
	if err := logs.ValidateEntry(logs.LogEntry{TimestampNs: 1710000000000000000, Line: "x"}); err != nil {
		t.Errorf("unexpected error for positive timestamp: %v", err)
	}
}

func TestValidateEntry_LineSize(t *testing.T) {
	atLimit := logs.LogEntry{TimestampNs: 1, Line: strings.Repeat("a", logs.MaxLineBytes)}
	if err := logs.ValidateEntry(atLimit); err != nil {
		t.Errorf("line at limit must be accepted: %v", err)
	}
	over := logs.LogEntry{TimestampNs: 1, Line: strings.Repeat("a", logs.MaxLineBytes+1)}
	if err := logs.ValidateEntry(over); err == nil {
		t.Error("expected error for line over limit")
	}
	empty := logs.LogEntry{TimestampNs: 1, Line: ""}
	if err := logs.ValidateEntry(empty); err != nil {
		t.Errorf("empty line must be accepted: %v", err)
	}
}

func TestValidateEntry_ErrorIsTyped(t *testing.T) {
	err := logs.ValidateEntry(logs.LogEntry{TimestampNs: 0, Line: "x"})
	var ve *logs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T", err)
	}
	if ve.Field != "timestamp_ns" {
		t.Errorf("expected Field=timestamp_ns, got %q", ve.Field)
	}
}
