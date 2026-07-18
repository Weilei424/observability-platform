package logwal

import (
	"encoding/binary"
	"fmt"
)

const recordTypeLog byte = 0x01

// maxRecordBodyBytes bounds the body size Replay will allocate for one record:
// 255 labels × (1-byte name len + 255-byte name + 2-byte value len + 65535-byte value)
// + 2 (type + label count) + 8 (tsNs) + 4 (line len) + 262144 (max line, 256 KiB).
const maxRecordBodyBytes uint32 = 255*(1+255+2+65535) + 2 + 8 + 4 + 256*1024

// LabelPair is a single name/value label stored in a log WAL record.
type LabelPair struct {
	Name  string
	Value string
}

// RecordWriter is the write interface satisfied by *LogWAL. The logs WALStore
// depends on this interface so tests can inject a fake.
type RecordWriter interface {
	WriteRecord(labels []LabelPair, tsNs int64, line string) error
	SegmentIndex() int
}

// validateLabels returns an error if labels violate the WAL encoding limits.
// Called by WriteRecord before encoding so the WAL never panics or truncates.
func validateLabels(labels []LabelPair) error {
	if len(labels) > 255 {
		return fmt.Errorf("logwal: label count %d exceeds 1-byte limit (255)", len(labels))
	}
	for _, lp := range labels {
		if len(lp.Name) > 255 {
			return fmt.Errorf("logwal: label name %q length %d exceeds 1-byte limit (255)", lp.Name, len(lp.Name))
		}
		if len(lp.Value) > 65535 {
			return fmt.Errorf("logwal: label value for %q length %d exceeds 2-byte limit (65535)", lp.Name, len(lp.Value))
		}
	}
	return nil
}

// encodeRecord serializes a log record including the 4-byte length prefix.
// Format: [4-byte body len][type byte][label count][labels...][8-byte tsNs][4-byte line len][line]
// Callers must validate labels with validateLabels before calling.
func encodeRecord(labels []LabelPair, tsNs int64, line string) []byte {
	bodyLen := 2 // type byte + label count byte
	for _, lp := range labels {
		bodyLen += 1 + len(lp.Name) + 2 + len(lp.Value)
	}
	bodyLen += 8 + 4 + len(line) // tsNs + line length prefix + line bytes

	buf := make([]byte, 4+bodyLen)
	binary.BigEndian.PutUint32(buf[0:4], uint32(bodyLen))

	pos := 4
	buf[pos] = recordTypeLog
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

	binary.BigEndian.PutUint64(buf[pos:pos+8], uint64(tsNs))
	pos += 8
	binary.BigEndian.PutUint32(buf[pos:pos+4], uint32(len(line)))
	pos += 4
	copy(buf[pos:], line)
	return buf
}

// decodeRecord parses a record body (the bytes after the 4-byte length prefix).
// Returns (nil, 0, "", false) on any parse error.
func decodeRecord(b []byte) (labels []LabelPair, tsNs int64, line string, ok bool) {
	if len(b) < 2 {
		return nil, 0, "", false
	}
	pos := 0
	if b[pos] != recordTypeLog {
		return nil, 0, "", false
	}
	pos++

	labelCount := int(b[pos])
	pos++

	labels = make([]LabelPair, 0, labelCount)
	for i := 0; i < labelCount; i++ {
		if pos+1 > len(b) {
			return nil, 0, "", false
		}
		nameLen := int(b[pos])
		pos++
		if pos+nameLen > len(b) {
			return nil, 0, "", false
		}
		name := string(b[pos : pos+nameLen])
		pos += nameLen

		if pos+2 > len(b) {
			return nil, 0, "", false
		}
		valueLen := int(binary.BigEndian.Uint16(b[pos : pos+2]))
		pos += 2
		if pos+valueLen > len(b) {
			return nil, 0, "", false
		}
		val := string(b[pos : pos+valueLen])
		pos += valueLen

		labels = append(labels, LabelPair{Name: name, Value: val})
	}

	// tsNs (8) + line length (4) must be present.
	if pos+12 > len(b) {
		return nil, 0, "", false
	}
	tsNs = int64(binary.BigEndian.Uint64(b[pos : pos+8]))
	pos += 8
	lineLen := int(binary.BigEndian.Uint32(b[pos : pos+4]))
	pos += 4
	// Require exact consumption: too few bytes means truncation, trailing bytes
	// mean a malformed record — both are corrupt.
	if pos+lineLen != len(b) {
		return nil, 0, "", false
	}
	line = string(b[pos : pos+lineLen])
	return labels, tsNs, line, true
}
