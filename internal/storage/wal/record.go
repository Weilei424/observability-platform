package wal

import (
	"encoding/binary"
	"fmt"
	"math"
)

const recordTypeSample byte = 0x01

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

// encodeRecord serializes a sample record including the 4-byte length prefix.
// Format: [4-byte body len][type byte][label count][labels...][8-byte tsMs][8-byte value]
func encodeRecord(labels []LabelPair, tsMs int64, value float64) []byte {
	bodyLen := 2 // type byte + label count byte
	for _, lp := range labels {
		bodyLen += 1 + len(lp.Name) + 2 + len(lp.Value)
	}
	bodyLen += 16 // 8-byte timestamp + 8-byte value

	buf := make([]byte, 4+bodyLen)
	binary.BigEndian.PutUint32(buf[0:4], uint32(bodyLen))

	if len(labels) > 255 {
		panic(fmt.Sprintf("wal: encodeRecord: label count %d exceeds 1-byte limit (255)", len(labels)))
	}

	pos := 4
	buf[pos] = recordTypeSample
	pos++
	buf[pos] = byte(len(labels))
	pos++

	for _, lp := range labels {
		if len(lp.Name) > 255 {
			panic(fmt.Sprintf("wal: encodeRecord: label name %q length %d exceeds 1-byte limit (255)", lp.Name, len(lp.Name)))
		}
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
