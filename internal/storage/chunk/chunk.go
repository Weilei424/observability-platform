package chunk

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"
)

// ErrChunkFull is returned by Append when the chunk has been sealed.
var ErrChunkFull = errors.New("chunk is full")

const (
	maxSamples = uint16(120)
	maxSpanMs  = int64(7_200_000) // 2 hours
)

// Chunk stores time-series samples using Gorilla/XOR encoding.
// Seal threshold: 120 samples OR 2-hour time span, whichever comes first.
type Chunk struct {
	minTs        int64
	maxTs        int64
	numSamples   uint16
	sealed       bool
	bw           bitsWriter
	lastTs       int64
	lastDelta    int64
	lastVal      uint64
	lastLeading  uint8
	lastTrailing uint8
}

// NewChunk returns an empty, open Chunk ready for appends.
func NewChunk() *Chunk {
	return &Chunk{lastLeading: 0xff} // 0xff = sentinel: no previous XOR block
}

// Bytes serializes the chunk to a byte slice suitable for persistence.
// Format: [8]minTs | [8]maxTs | [2]numSamples | encoded bitstream.
// The deserialized chunk is sealed (read-only); call FromBytes to reconstruct it.
func (c *Chunk) Bytes() []byte {
	data := c.bw.bytes()
	out := make([]byte, 18+len(data))
	binary.BigEndian.PutUint64(out[0:8], uint64(c.minTs))
	binary.BigEndian.PutUint64(out[8:16], uint64(c.maxTs))
	binary.BigEndian.PutUint16(out[16:18], c.numSamples)
	copy(out[18:], data)
	return out
}

// FromBytes reconstructs a sealed, read-only Chunk from data produced by Bytes.
// Returns an error if data is too short to contain a valid header.
func FromBytes(data []byte) (*Chunk, error) {
	if len(data) < 18 {
		return nil, fmt.Errorf("chunk.FromBytes: data too short (%d bytes)", len(data))
	}
	minTs := int64(binary.BigEndian.Uint64(data[0:8]))
	maxTs := int64(binary.BigEndian.Uint64(data[8:16]))
	numSamples := binary.BigEndian.Uint16(data[16:18])
	payload := make([]byte, len(data)-18)
	copy(payload, data[18:])
	return &Chunk{
		minTs:       minTs,
		maxTs:       maxTs,
		numSamples:  numSamples,
		sealed:      true,
		bw:          bitsWriter{buf: payload},
		lastLeading: 0xff,
	}, nil
}

// Append encodes tsMs and val into the chunk.
// Returns ErrChunkFull immediately if the chunk is already sealed.
// The threshold-crossing sample is written before sealing: the chunk is sealed
// after storing the sample that first meets the count or time-span threshold.
// Subsequent appends return ErrChunkFull. This matches Prometheus head chunk behavior.
func (c *Chunk) Append(tsMs int64, val float64) error {
	if c.sealed {
		return ErrChunkFull
	}
	v := math.Float64bits(val)
	if c.numSamples == 0 {
		// First sample: absolute timestamp (64 bits) + raw value (64 bits)
		c.bw.writeBits(uint64(tsMs), 64)
		c.bw.writeBits(v, 64)
		c.minTs = tsMs
		c.maxTs = tsMs
		c.lastTs = tsMs
		c.lastVal = v
		c.numSamples++
		return nil
	}
	delta := tsMs - c.lastTs
	dod := delta - c.lastDelta
	c.writeTimestampDod(dod)
	c.lastDelta = delta
	c.lastTs = tsMs
	if tsMs < c.minTs {
		c.minTs = tsMs
	}
	if tsMs > c.maxTs {
		c.maxTs = tsMs
	}
	c.writeValueXOR(v)
	c.lastVal = v
	c.numSamples++
	if c.numSamples >= maxSamples || (c.maxTs-c.minTs) >= maxSpanMs {
		c.sealed = true
	}
	return nil
}

// Sealed reports whether the chunk has been sealed.
func (c *Chunk) Sealed() bool { return c.sealed }

// MinTs returns the minimum timestamp stored in the chunk.
func (c *Chunk) MinTs() int64 { return c.minTs }

// MaxTs returns the maximum timestamp stored in the chunk.
func (c *Chunk) MaxTs() int64 { return c.maxTs }

// NumSamples returns the number of samples encoded in the chunk.
func (c *Chunk) NumSamples() int { return int(c.numSamples) }

// Iterator returns a new Iterator that decodes samples from this chunk in order.
func (c *Chunk) Iterator() *Iterator {
	return &Iterator{
		br:          newBitsReader(c.bw.bytes()),
		total:       int(c.numSamples),
		lastLeading: 0xff,
	}
}

// Iterator decodes samples from a Chunk in insertion order.
type Iterator struct {
	br           bitsReader
	total        int
	n            int
	lastTs       int64
	lastDelta    int64
	lastVal      uint64
	lastLeading  uint8
	lastTrailing uint8
	curTs        int64
	curVal       float64
	err          error
}

// Next advances to the next sample. Returns false when exhausted or on error.
func (it *Iterator) Next() bool {
	if it.n >= it.total || it.err != nil {
		return false
	}
	if it.n == 0 {
		// First sample: read absolute timestamp + raw float64
		ts, err := it.br.readBits(64)
		if err != nil {
			it.err = err
			return false
		}
		val, err := it.br.readBits(64)
		if err != nil {
			it.err = err
			return false
		}
		it.lastTs = int64(ts)
		it.lastVal = val
		it.curTs = int64(ts)
		it.curVal = math.Float64frombits(val)
		it.n++
		return true
	}
	dod, err := it.decodeTimestamp()
	if err != nil {
		it.err = err
		return false
	}
	delta := it.lastDelta + dod
	ts := it.lastTs + delta
	it.lastDelta = delta
	it.lastTs = ts

	val, err := it.decodeValue()
	if err != nil {
		it.err = err
		return false
	}
	it.lastVal = val
	it.curTs = ts
	it.curVal = math.Float64frombits(val)
	it.n++
	return true
}

// At returns the timestamp and value of the current sample.
// Must only be called after a successful Next().
func (it *Iterator) At() (tsMs int64, val float64) {
	return it.curTs, it.curVal
}

// Err returns any error encountered during iteration.
// Returns nil if the iterator exhausted normally.
func (it *Iterator) Err() error { return it.err }

// --- bitsWriter ---

type bitsWriter struct {
	buf   []byte
	cur   byte
	nBits uint8 // bits written into cur (0 = empty; when it reaches 8, flush cur to buf)
}

func (w *bitsWriter) writeBit(b uint8) {
	if b != 0 {
		w.cur |= 1 << (7 - w.nBits)
	}
	w.nBits++
	if w.nBits == 8 {
		w.buf = append(w.buf, w.cur)
		w.cur = 0
		w.nBits = 0
	}
}

func (w *bitsWriter) writeBits(val uint64, n uint8) {
	if n == 0 {
		return
	}
	for i := int(n) - 1; i >= 0; i-- {
		w.writeBit(uint8((val >> uint(i)) & 1))
	}
}

// bytes returns a copy of the encoded bytes, including any partially-filled byte.
func (w *bitsWriter) bytes() []byte {
	if w.nBits == 0 {
		out := make([]byte, len(w.buf))
		copy(out, w.buf)
		return out
	}
	out := make([]byte, len(w.buf)+1)
	copy(out, w.buf)
	out[len(w.buf)] = w.cur
	return out
}

// --- bitsReader ---

type bitsReader struct {
	buf   []byte
	pos   int
	cur   byte
	nBits uint8 // bits remaining in cur to read (0 = exhausted; load next byte from buf)
}

func newBitsReader(buf []byte) bitsReader {
	return bitsReader{buf: buf}
}

func (r *bitsReader) readBit() (uint8, error) {
	if r.nBits == 0 {
		if r.pos >= len(r.buf) {
			return 0, io.EOF
		}
		r.cur = r.buf[r.pos]
		r.pos++
		r.nBits = 8
	}
	b := (r.cur >> 7) & 1
	r.cur <<= 1
	r.nBits--
	return b, nil
}

func (r *bitsReader) readBits(n uint8) (uint64, error) {
	if n > 64 {
		n = 64
	}
	var val uint64
	for i := uint8(0); i < n; i++ {
		b, err := r.readBit()
		if err != nil {
			return 0, err
		}
		val = (val << 1) | uint64(b)
	}
	return val, nil
}

// --- encoding helpers ---

// writeTimestampDod encodes a delta-of-delta using Gorilla variable-length encoding.
// Control bits select the number of data bits:
//
//	0        → dod = 0
//	10 + 7b  → dod in [-64, 63]
//	110 + 9b → dod in [-256, 255]
//	1110+12b → dod in [-2048, 2047]
//	1111+64b → any other value
func (c *Chunk) writeTimestampDod(dod int64) {
	switch {
	case dod == 0:
		c.bw.writeBit(0)
	case dod >= -64 && dod <= 63:
		c.bw.writeBits(0b10, 2)
		c.bw.writeBits(zigzagEncode(dod), 7)
	case dod >= -256 && dod <= 255:
		c.bw.writeBits(0b110, 3)
		c.bw.writeBits(zigzagEncode(dod), 9)
	case dod >= -2048 && dod <= 2047:
		c.bw.writeBits(0b1110, 4)
		c.bw.writeBits(zigzagEncode(dod), 12)
	default:
		c.bw.writeBits(0b1111, 4)
		c.bw.writeBits(zigzagEncode(dod), 64)
	}
}

// writeValueXOR encodes a float64 value (as its uint64 bit pattern) using XOR compression.
// Control bits:
//
//	0         → XOR is 0 (same value as previous)
//	1 0 + Mb  → reuse previous leading/trailing zero block, write M significant bits
//	1 1 +5b+6b+Mb → new block: 5 bits for leading zeros, 6 bits for (sigBits-1), M significant bits
//
// Leading zeros are capped at 31 to fit in 5 bits. sigBits-1 is stored so that 64 sig bits
// encodes as 63 (the maximum 6-bit value).
func (c *Chunk) writeValueXOR(val uint64) {
	xor := c.lastVal ^ val
	if xor == 0 {
		c.bw.writeBit(0)
		return
	}
	lz := uint8(bits.LeadingZeros64(xor))
	tz := uint8(bits.TrailingZeros64(xor))
	if lz > 31 {
		lz = 31
	}
	sigBits := 64 - lz - tz

	c.bw.writeBit(1)
	if c.lastLeading != 0xff && lz >= c.lastLeading && tz >= c.lastTrailing {
		// Reuse previous block. The condition lz>=lastLeading && tz>=lastTrailing means the
		// new XOR fits within the previous block's window. We write prevSigBits using
		// xor>>lastTrailing, which is correct because the stripped low bits are zero.
		// When tz>lastTrailing the written block is larger than strictly necessary
		// (prevSigBits > 64-lz-tz), trading a few extra bits for simpler state management.
		c.bw.writeBit(0)
		prevSigBits := 64 - c.lastLeading - c.lastTrailing
		c.bw.writeBits(xor>>c.lastTrailing, prevSigBits)
		return
	}
	// New block
	c.bw.writeBit(1)
	c.bw.writeBits(uint64(lz), 5)
	c.bw.writeBits(uint64(sigBits-1), 6) // stored as sigBits-1 so 64→63 fits in 6 bits
	c.bw.writeBits(xor>>tz, sigBits)
	c.lastLeading = lz
	c.lastTrailing = tz
}

func (it *Iterator) decodeTimestamp() (int64, error) {
	b0, err := it.br.readBit()
	if err != nil {
		return 0, err
	}
	if b0 == 0 {
		return 0, nil
	}
	b1, err := it.br.readBit()
	if err != nil {
		return 0, err
	}
	if b1 == 0 {
		// 10: 7 bits zigzag
		v, err := it.br.readBits(7)
		if err != nil {
			return 0, err
		}
		return zigzagDecode(v), nil
	}
	b2, err := it.br.readBit()
	if err != nil {
		return 0, err
	}
	if b2 == 0 {
		// 110: 9 bits zigzag
		v, err := it.br.readBits(9)
		if err != nil {
			return 0, err
		}
		return zigzagDecode(v), nil
	}
	b3, err := it.br.readBit()
	if err != nil {
		return 0, err
	}
	if b3 == 0 {
		// 1110: 12 bits zigzag
		v, err := it.br.readBits(12)
		if err != nil {
			return 0, err
		}
		return zigzagDecode(v), nil
	}
	// 1111: 64 bits zigzag
	v, err := it.br.readBits(64)
	if err != nil {
		return 0, err
	}
	return zigzagDecode(v), nil
}

func (it *Iterator) decodeValue() (uint64, error) {
	b0, err := it.br.readBit()
	if err != nil {
		return 0, err
	}
	if b0 == 0 {
		// XOR was 0: same value as previous
		return it.lastVal, nil
	}
	b1, err := it.br.readBit()
	if err != nil {
		return 0, err
	}
	if b1 == 0 {
		// Reuse previous block
		sigBits := uint8(64) - it.lastLeading - it.lastTrailing
		v, err := it.br.readBits(sigBits)
		if err != nil {
			return 0, err
		}
		xor := v << it.lastTrailing
		return it.lastVal ^ xor, nil
	}
	// New block
	lzBits, err := it.br.readBits(5)
	if err != nil {
		return 0, err
	}
	sbMinus1, err := it.br.readBits(6)
	if err != nil {
		return 0, err
	}
	sigBits := uint8(sbMinus1) + 1
	v, err := it.br.readBits(sigBits)
	if err != nil {
		return 0, err
	}
	lz := uint8(lzBits)
	tz := 64 - lz - sigBits
	it.lastLeading = lz
	it.lastTrailing = tz
	xor := v << tz
	return it.lastVal ^ xor, nil
}

func zigzagEncode(v int64) uint64 { return uint64((v << 1) ^ (v >> 63)) }
func zigzagDecode(v uint64) int64 { return int64((v >> 1) ^ -(v & 1)) }
