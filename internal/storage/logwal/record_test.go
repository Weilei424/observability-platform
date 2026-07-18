package logwal

import "testing"

func roundTrip(t *testing.T, labels []LabelPair, tsNs int64, line string) {
	t.Helper()
	if err := validateLabels(labels); err != nil {
		t.Fatalf("validateLabels: %v", err)
	}
	buf := encodeRecord(labels, tsNs, line)
	// buf includes the 4-byte length prefix; decodeRecord takes the body only.
	body := buf[4:]
	gotLabels, gotTs, gotLine, ok := decodeRecord(body)
	if !ok {
		t.Fatalf("decodeRecord ok=false")
	}
	if gotTs != tsNs {
		t.Errorf("tsNs = %d, want %d", gotTs, tsNs)
	}
	if gotLine != line {
		t.Errorf("line = %q, want %q", gotLine, line)
	}
	if len(gotLabels) != len(labels) {
		t.Fatalf("label count = %d, want %d", len(gotLabels), len(labels))
	}
	for i := range labels {
		if gotLabels[i] != labels[i] {
			t.Errorf("label[%d] = %+v, want %+v", i, gotLabels[i], labels[i])
		}
	}
}

func TestEncodeDecode_RoundTrip(t *testing.T) {
	roundTrip(t, []LabelPair{{"service", "api"}}, 1700000000000000000, "hello world")
	roundTrip(t, []LabelPair{{"a", "1"}, {"service", "api"}}, 1, "")                 // empty line
	roundTrip(t, []LabelPair{{"svc", "ingester"}}, 42, "λ multi-byte 日本語 line")     // UTF-8
	roundTrip(t, nil, 99, "no labels line")                                          // zero labels
}

func TestEncodeDecode_MaxLine(t *testing.T) {
	line := string(make([]byte, 256*1024)) // MaxLineBytes worth of NULs
	roundTrip(t, []LabelPair{{"service", "api"}}, 123, line)
}

func TestDecode_Truncated(t *testing.T) {
	buf := encodeRecord([]LabelPair{{"service", "api"}}, 5, "line")
	body := buf[4:]
	if _, _, _, ok := decodeRecord(body[:len(body)-1]); ok {
		t.Error("decodeRecord should reject a truncated body")
	}
}

func TestDecode_TrailingBytes(t *testing.T) {
	buf := encodeRecord([]LabelPair{{"service", "api"}}, 5, "line")
	body := append(buf[4:], 0x00) // one extra byte
	if _, _, _, ok := decodeRecord(body); ok {
		t.Error("decodeRecord should reject trailing bytes (exact-consumption rule)")
	}
}

func TestValidateLabels_Limits(t *testing.T) {
	if err := validateLabels([]LabelPair{{Name: string(make([]byte, 256)), Value: "v"}}); err == nil {
		t.Error("expected error for name > 255 bytes")
	}
	if err := validateLabels([]LabelPair{{Name: "n", Value: string(make([]byte, 65536))}}); err == nil {
		t.Error("expected error for value > 65535 bytes")
	}
	tooMany := make([]LabelPair, 256)
	if err := validateLabels(tooMany); err == nil {
		t.Error("expected error for > 255 labels")
	}
}
