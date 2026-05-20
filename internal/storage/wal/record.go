package wal

import (
	"encoding/binary"
	"fmt"
	"math"
)

const recordTypeSample byte = 0x01

// maxRecordBodyBytes is the theoretical maximum body size given encoding limits:
// 255 labels × (1-byte name length + 255-byte name + 2-byte value length + 65535-byte value)
// + 2 bytes (type + label count) + 16 bytes (timestamp + value).
const maxRecordBodyBytes uint32 = 255*(1+255+2+65535) + 2 + 16 // ≈ 16.8 MB

// LabelPair is a single name/value label stored in a WAL record.
type LabelPair struct {
	Name  string
	Value string
}

// RecordWriter is the write interface satisfied by *WAL.
// WALStore depends on this interface so tests can inject a fake.
type RecordWriter interface {
	WriteRecord(labels []LabelPair, tsMs int64, value float64) error
}

// validateLabels returns an error if labels violate WAL encoding limits.
// This is called by WriteRecord before encoding so the WAL never panics or
// silently truncates data.
func validateLabels(labels []LabelPair) error {
	if len(labels) > 255 {
		return fmt.Errorf("wal: label count %d exceeds 1-byte limit (255)", len(labels))
	}
	for _, lp := range labels {
		if len(lp.Name) > 255 {
			return fmt.Errorf("wal: label name %q length %d exceeds 1-byte limit (255)", lp.Name, len(lp.Name))
		}
		if len(lp.Value) > 65535 {
			return fmt.Errorf("wal: label value for %q length %d exceeds 2-byte limit (65535)", lp.Name, len(lp.Value))
		}
	}
	return nil
}

// encodeRecord serializes a sample record including the 4-byte length prefix.
// Format: [4-byte body len][type byte][label count][labels...][8-byte tsMs][8-byte value]
// Callers must validate labels with validateLabels before calling.
func encodeRecord(labels []LabelPair, tsMs int64, value float64) []byte {
	bodyLen := 2 // type byte + label count byte
	for _, lp := range labels {
		bodyLen += 1 + len(lp.Name) + 2 + len(lp.Value)
	}
	bodyLen += 16 // 8-byte timestamp + 8-byte value

	buf := make([]byte, 4+bodyLen)
	binary.BigEndian.PutUint32(buf[0:4], uint32(bodyLen))

	pos := 4
	buf[pos] = recordTypeSample
	pos++
	buf[pos] = byte(len(labels))
	pos++

	for _, lp := range labels {
		buf[pos] = byte(len(lp.Name))
		pos++
		copy(buf[pos:], lp.Name)
		pos += len(lp.Name)
		binary.BigEndian.PutUint16(buf[pos:pos+2], uint16(len(lp.Value)))
		pos += 2
		copy(buf[pos:], lp.Value)
		pos += len(lp.Value)
	}

	binary.BigEndian.PutUint64(buf[pos:pos+8], uint64(tsMs))
	pos += 8
	binary.BigEndian.PutUint64(buf[pos:pos+8], math.Float64bits(value))
	return buf
}

// decodeRecord parses a record body (the bytes after the 4-byte length prefix).
// Returns (nil, 0, 0, false) on any parse error.
func decodeRecord(b []byte) (labels []LabelPair, tsMs int64, value float64, ok bool) {
	if len(b) < 2 {
		return nil, 0, 0, false
	}
	pos := 0
	if b[pos] != recordTypeSample {
		return nil, 0, 0, false
	}
	pos++

	labelCount := int(b[pos])
	pos++

	labels = make([]LabelPair, 0, labelCount)
	for i := 0; i < labelCount; i++ {
		if pos+1 > len(b) {
			return nil, 0, 0, false
		}
		nameLen := int(b[pos])
		pos++
		if pos+nameLen > len(b) {
			return nil, 0, 0, false
		}
		name := string(b[pos : pos+nameLen])
		pos += nameLen

		if pos+2 > len(b) {
			return nil, 0, 0, false
		}
		valueLen := int(binary.BigEndian.Uint16(b[pos : pos+2]))
		pos += 2
		if pos+valueLen > len(b) {
			return nil, 0, 0, false
		}
		val := string(b[pos : pos+valueLen])
		pos += valueLen

		labels = append(labels, LabelPair{Name: name, Value: val})
	}

	if pos+16 != len(b) {
		// Require exact consumption: too few bytes means truncation, trailing
		// bytes mean a malformed record — both are corrupt.
		return nil, 0, 0, false
	}
	tsMs = int64(binary.BigEndian.Uint64(b[pos : pos+8]))
	value = math.Float64frombits(binary.BigEndian.Uint64(b[pos+8 : pos+16]))
	return labels, tsMs, value, true
}
