package wal

import (
	"math"
	"testing"
)

func TestEncodeDecodeRecord(t *testing.T) {
	labels := []LabelPair{
		{Name: "__name__", Value: "http_requests_total"},
		{Name: "service", Value: "api"},
	}
	tsMs := int64(1_700_000_000_000)
	value := 42.5

	encoded := encodeRecord(labels, tsMs, value)
	if len(encoded) < 4 {
		t.Fatalf("encoded length %d < 4", len(encoded))
	}

	body := encoded[4:]
	gotLabels, gotTs, gotVal, ok := decodeRecord(body)
	if !ok {
		t.Fatal("decodeRecord returned ok=false")
	}
	if gotTs != tsMs {
		t.Errorf("tsMs = %d, want %d", gotTs, tsMs)
	}
	if gotVal != value {
		t.Errorf("value = %v, want %v", gotVal, value)
	}
	if len(gotLabels) != len(labels) {
		t.Fatalf("label count = %d, want %d", len(gotLabels), len(labels))
	}
	for i, lp := range labels {
		if gotLabels[i].Name != lp.Name || gotLabels[i].Value != lp.Value {
			t.Errorf("label[%d] = {%q,%q}, want {%q,%q}", i, gotLabels[i].Name, gotLabels[i].Value, lp.Name, lp.Value)
		}
	}
}

func TestEncodeDecodeRecord_NaNInf(t *testing.T) {
	labels := []LabelPair{{Name: "__name__", Value: "m"}}
	for _, v := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		encoded := encodeRecord(labels, 0, v)
		_, _, gotVal, ok := decodeRecord(encoded[4:])
		if !ok {
			t.Fatalf("decodeRecord failed for value %v", v)
		}
		if math.IsNaN(v) {
			if !math.IsNaN(gotVal) {
				t.Errorf("want NaN, got %v", gotVal)
			}
		} else if gotVal != v {
			t.Errorf("got %v, want %v", gotVal, v)
		}
	}
}

func TestDecodeRecord_TruncatedBodyReturnsFalse(t *testing.T) {
	labels := []LabelPair{{Name: "__name__", Value: "m"}}
	encoded := encodeRecord(labels, 1000, 1.0)
	body := encoded[4:]
	// Feed only half the body — must not panic and must return ok=false.
	_, _, _, ok := decodeRecord(body[:len(body)/2])
	if ok {
		t.Error("expected ok=false for truncated body")
	}
}

func TestEncodeDecodeRecord_ZeroLabels(t *testing.T) {
	encoded := encodeRecord(nil, 5000, 3.14)
	labels, tsMs, val, ok := decodeRecord(encoded[4:])
	if !ok {
		t.Fatal("decodeRecord returned ok=false for zero-label record")
	}
	if len(labels) != 0 {
		t.Errorf("got %d labels, want 0", len(labels))
	}
	if tsMs != 5000 {
		t.Errorf("tsMs = %d, want 5000", tsMs)
	}
	if val != 3.14 {
		t.Errorf("val = %v, want 3.14", val)
	}
}

func TestEncodeDecodeRecord_EmptyNameValue(t *testing.T) {
	labels := []LabelPair{{Name: "", Value: ""}, {Name: "__name__", Value: "m"}}
	encoded := encodeRecord(labels, 0, 0)
	got, _, _, ok := decodeRecord(encoded[4:])
	if !ok {
		t.Fatal("decodeRecord returned ok=false")
	}
	if len(got) != 2 {
		t.Fatalf("got %d labels, want 2", len(got))
	}
	if got[0].Name != "" || got[0].Value != "" {
		t.Errorf("label[0] = {%q,%q}, want {\"\",\"\"}", got[0].Name, got[0].Value)
	}
}
