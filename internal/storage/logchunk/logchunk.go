// Package logchunk encodes timestamped log lines into a compressed, self-describing
// chunk. It is dependency-free (stdlib only) and must not import internal/logs.
package logchunk

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

// magic prefixes every v1 log chunk: 0x9C sentinel, "LC", version 0x01.
var magic = [4]byte{0x9C, 'L', 'C', 0x01}

// crcTable is CRC-32/Castagnoli, the same polynomial Prometheus/Loki chunks use.
var crcTable = crc32.MakeTable(crc32.Castagnoli)

const (
	version byte = 1
	// headerLen: magic|ver|minTs|maxTs|numEntries|uncompLen|compLen|headerCRC|payloadCRC = 41.
	// headerCRC (Castagnoli) covers bytes [0:33] so the timestamp bounds and counts
	// are authenticated by a header-only read; payloadCRC covers the compressed body.
	headerLen      = 4 + 1 + 8 + 8 + 4 + 4 + 4 + 4 + 4
	headerCRCScope = 33 // bytes [0:33] covered by headerCRC

	// MaxUncompressedBytes bounds the entry-block buffer FromBytes will allocate
	// from an attacker/corruption-controlled length field, and is the hard cap a
	// chunk's uncompressed size must stay under to remain decodable. Writers must
	// keep each chunk's UncompressedBytes() at or below this (see splitting in the
	// logs store) so a large flush can never produce an unreadable chunk.
	MaxUncompressedBytes = 128 << 20
)

type entry struct {
	tsNs int64
	line string
}

// Chunk accumulates log entries and serializes them as one DEFLATE-compressed block.
type Chunk struct {
	minTs, maxTs int64
	lastTs       int64
	entries      []entry
	uncompressed int // exact length of the encoded (pre-compression) entry block
}

// NewChunk returns an empty chunk ready for Append.
func NewChunk() *Chunk { return &Chunk{} }

// Append adds one entry. Timestamps may be out of order; min/max track the extremes.
func (c *Chunk) Append(tsNs int64, line string) {
	var d int64
	if len(c.entries) == 0 {
		c.minTs, c.maxTs = tsNs, tsNs
		d = tsNs
	} else {
		d = tsNs - c.lastTs
		if tsNs < c.minTs {
			c.minTs = tsNs
		}
		if tsNs > c.maxTs {
			c.maxTs = tsNs
		}
	}
	c.lastTs = tsNs
	c.uncompressed += signedVarintLen(d) + uvarintLen(uint64(len(line))) + len(line)
	c.entries = append(c.entries, entry{tsNs: tsNs, line: line})
}

// MinTs returns the smallest timestamp (0 for an empty chunk).
func (c *Chunk) MinTs() int64 { return c.minTs }

// MaxTs returns the largest timestamp (0 for an empty chunk).
func (c *Chunk) MaxTs() int64 { return c.maxTs }

// NumEntries returns the number of appended entries.
func (c *Chunk) NumEntries() int { return len(c.entries) }

// UncompressedBytes returns the exact length of the encoded entry block before
// compression (equal to the serialized header's uncompressedLen field).
func (c *Chunk) UncompressedBytes() int { return c.uncompressed }

// encodeEntries serializes entries: first ts absolute (signed varint), the rest as
// signed-varint deltas; each line as uvarint length + raw bytes.
func (c *Chunk) encodeEntries() []byte {
	buf := make([]byte, 0, c.uncompressed)
	var tmp [binary.MaxVarintLen64]byte
	var prev int64
	for i, e := range c.entries {
		if i == 0 {
			buf = append(buf, tmp[:binary.PutVarint(tmp[:], e.tsNs)]...)
		} else {
			buf = append(buf, tmp[:binary.PutVarint(tmp[:], e.tsNs-prev)]...)
		}
		prev = e.tsNs
		buf = append(buf, tmp[:binary.PutUvarint(tmp[:], uint64(len(e.line)))]...)
		buf = append(buf, e.line...)
	}
	return buf
}

// Bytes serializes the chunk:
// magic(4)|version(1)|minTs(8)|maxTs(8)|numEntries(4)|uncompLen(4)|compLen(4)|headerCRC(4)|payloadCRC(4)|DEFLATE(block)
// Both CRCs are Castagnoli: headerCRC covers bytes [0:33] so the bounds/counts are
// authenticated by a header-only read; payloadCRC covers the compressed payload
// (raw DEFLATE has no integrity check of its own).
func (c *Chunk) Bytes() []byte {
	block := c.encodeEntries()
	var cbuf bytes.Buffer
	fw, _ := flate.NewWriter(&cbuf, flate.DefaultCompression) // level is constant/valid
	_, _ = fw.Write(block)
	_ = fw.Close()
	compressed := cbuf.Bytes()

	out := make([]byte, headerLen, headerLen+len(compressed))
	copy(out[0:4], magic[:])
	out[4] = version
	binary.BigEndian.PutUint64(out[5:13], uint64(c.minTs))
	binary.BigEndian.PutUint64(out[13:21], uint64(c.maxTs))
	binary.BigEndian.PutUint32(out[21:25], uint32(len(c.entries)))
	binary.BigEndian.PutUint32(out[25:29], uint32(len(block)))
	binary.BigEndian.PutUint32(out[29:33], uint32(len(compressed)))
	binary.BigEndian.PutUint32(out[33:37], crc32.Checksum(out[:headerCRCScope], crcTable)) // header CRC
	binary.BigEndian.PutUint32(out[37:41], crc32.Checksum(compressed, crcTable))           // payload CRC
	out = append(out, compressed...)
	return out
}

// HeaderLen is the fixed size of a serialized chunk's header (the bytes preceding
// the compressed payload). A reader that only needs the timestamp bounds can read
// exactly this many bytes and call PeekBounds, avoiding the payload entirely.
const HeaderLen = headerLen

// PeekBounds reads a chunk's min/max timestamps and entry count from the fixed
// header WITHOUT decompressing (or checksumming) the payload — the cheap path used
// to rebuild an index from chunk headers. It validates magic and version only; full
// payload integrity is verified by FromBytes when the chunk is actually read. data
// must contain at least HeaderLen bytes.
func PeekBounds(data []byte) (minTs, maxTs int64, numEntries int, err error) {
	if len(data) < headerLen ||
		data[0] != magic[0] || data[1] != magic[1] || data[2] != magic[2] || data[3] != magic[3] {
		return 0, 0, 0, errors.New("logchunk.PeekBounds: unrecognized chunk format")
	}
	if data[4] != version {
		return 0, 0, 0, fmt.Errorf("logchunk.PeekBounds: unsupported version %d", data[4])
	}
	// Authenticate the header before trusting the bounds — a header-only read has no
	// decoded payload to cross-check them against.
	if crc32.Checksum(data[:headerCRCScope], crcTable) != binary.BigEndian.Uint32(data[33:37]) {
		return 0, 0, 0, errors.New("logchunk.PeekBounds: chunk header checksum mismatch")
	}
	minTs = int64(binary.BigEndian.Uint64(data[5:13]))
	maxTs = int64(binary.BigEndian.Uint64(data[13:21]))
	numEntries = int(binary.BigEndian.Uint32(data[21:25]))
	return minTs, maxTs, numEntries, nil
}

// FromBytes reconstructs a chunk from Bytes output, validating every field: magic,
// version, the header checksum, the uncompressed-size cap, the declared compressed
// length, the payload checksum, and that the decoded entries' min/max match the
// header.
func FromBytes(data []byte) (*Chunk, error) {
	if len(data) < headerLen ||
		data[0] != magic[0] || data[1] != magic[1] || data[2] != magic[2] || data[3] != magic[3] {
		return nil, errors.New("logchunk.FromBytes: unrecognized chunk format")
	}
	if data[4] != version {
		return nil, fmt.Errorf("logchunk.FromBytes: unsupported version %d", data[4])
	}
	if crc32.Checksum(data[:headerCRCScope], crcTable) != binary.BigEndian.Uint32(data[33:37]) {
		return nil, errors.New("logchunk.FromBytes: chunk header checksum mismatch")
	}
	minTs := int64(binary.BigEndian.Uint64(data[5:13]))
	maxTs := int64(binary.BigEndian.Uint64(data[13:21]))
	numEntries := binary.BigEndian.Uint32(data[21:25])
	uncompLen := binary.BigEndian.Uint32(data[25:29])
	compLen := binary.BigEndian.Uint32(data[29:33])
	payloadCRC := binary.BigEndian.Uint32(data[37:41])
	if uncompLen > MaxUncompressedBytes {
		return nil, fmt.Errorf("logchunk.FromBytes: uncompressed length %d exceeds maximum", uncompLen)
	}
	if headerLen+int(compLen) != len(data) {
		return nil, fmt.Errorf("logchunk.FromBytes: declared compressed length %d does not match data", compLen)
	}
	compressed := data[headerLen : headerLen+int(compLen)]
	if crc32.Checksum(compressed, crcTable) != payloadCRC {
		return nil, errors.New("logchunk.FromBytes: chunk payload checksum mismatch")
	}

	fr := flate.NewReader(bytes.NewReader(compressed))
	block := make([]byte, uncompLen)
	if _, err := io.ReadFull(fr, block); err != nil {
		return nil, fmt.Errorf("logchunk.FromBytes: decompress: %w", err)
	}
	// Any bytes beyond uncompLen — or a stream fault surfacing here — mean a
	// corrupt/oversized payload.
	var extra [1]byte
	if n, err := fr.Read(extra[:]); n != 0 || (err != nil && err != io.EOF) {
		return nil, errors.New("logchunk.FromBytes: decompressed payload longer than declared or corrupt")
	}
	_ = fr.Close()

	c, err := decodeEntries(block, numEntries)
	if err != nil {
		return nil, err
	}
	if c.minTs != minTs || c.maxTs != maxTs {
		return nil, fmt.Errorf("logchunk.FromBytes: header min/max (%d/%d) does not match decoded (%d/%d)",
			minTs, maxTs, c.minTs, c.maxTs)
	}
	return c, nil
}

// decodeEntries parses the entry block into a chunk, requiring exact consumption.
func decodeEntries(block []byte, n uint32) (*Chunk, error) {
	c := &Chunk{}
	pos := 0
	var prev int64
	for i := uint32(0); i < n; i++ {
		d, m := binary.Varint(block[pos:])
		if m <= 0 {
			return nil, fmt.Errorf("logchunk.FromBytes: bad timestamp varint at entry %d", i)
		}
		pos += m
		ts := d
		if i != 0 {
			ts = prev + d
		}
		prev = ts

		ll, m2 := binary.Uvarint(block[pos:])
		if m2 <= 0 {
			return nil, fmt.Errorf("logchunk.FromBytes: bad line length at entry %d", i)
		}
		pos += m2
		// Compare in unsigned space: int(ll) would wrap negative for a huge forged
		// length and slip past a signed bounds check, panicking on the slice below.
		if ll > uint64(len(block)-pos) {
			return nil, fmt.Errorf("logchunk.FromBytes: line at entry %d exceeds block", i)
		}
		line := string(block[pos : pos+int(ll)])
		pos += int(ll)
		c.Append(ts, line)
	}
	if pos != len(block) {
		return nil, fmt.Errorf("logchunk.FromBytes: %d trailing bytes in entry block", len(block)-pos)
	}
	return c, nil
}

// Iterator yields entries in insertion order.
type Iterator struct {
	entries []entry
	i       int
}

// Iterator returns an iterator positioned before the first entry.
func (c *Chunk) Iterator() *Iterator { return &Iterator{entries: c.entries, i: -1} }

// Next advances to the next entry, returning false when exhausted.
func (it *Iterator) Next() bool { it.i++; return it.i < len(it.entries) }

// At returns the current entry's timestamp and line. Call only after Next() == true.
func (it *Iterator) At() (int64, string) { return it.entries[it.i].tsNs, it.entries[it.i].line }

// Err always returns nil: all entries are decoded eagerly in FromBytes.
func (it *Iterator) Err() error { return nil }

func signedVarintLen(v int64) int {
	var b [binary.MaxVarintLen64]byte
	return binary.PutVarint(b[:], v)
}
func uvarintLen(v uint64) int { var b [binary.MaxVarintLen64]byte; return binary.PutUvarint(b[:], v) }
