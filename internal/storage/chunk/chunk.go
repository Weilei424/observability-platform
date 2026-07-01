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

	// chunkHeaderLen is the fixed-size header preceding the bitstream:
	// magic(4) | minTs(8) | maxTs(8) | numSamples(2) | bitstreamLen(4).
	chunkHeaderLen = 26

	// MaxGeneration bounds a sample's write generation well below math.MaxInt64 so
	// that seeding the startup counter (highest generation + 1) can never overflow.
	// The ingest path treats a counter beyond this as generation exhaustion and
	// fails the append explicitly rather than silently rejecting every write.
	MaxGeneration = int64(1) << 62
)

// chunkMagicV1 prefixes every generation-aware chunk. It is an explicit multi-byte
// version tag, not a guess about timestamp bytes: a chunk that does not begin with
// it is rejected outright (pre-generation chunks are unsupported), so no payload is
// ever silently misread as another format. The 0x9C sentinel keeps the prefix clear
// of a millisecond timestamp's leading bytes; the trailing 0x01 is the version.
var chunkMagicV1 = [4]byte{0x9C, 'C', 'H', 0x01}

// Chunk stores time-series samples using Gorilla/XOR encoding, plus a per-sample
// write generation used for last-write-wins deduplication.
// Seal threshold: 120 samples OR 2-hour time span, whichever comes first.
type Chunk struct {
	minTs        int64
	maxTs        int64
	numSamples   uint16
	sealed       bool
	bw           bitsWriter
	gens         []int64 // per-sample write generation, insertion order
	maxGen       int64
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
// Format: [4]magic | [8]minTs | [8]maxTs | [2]numSamples | [4]bitstreamLen |
// bitstream | genSection. genSection holds numSamples generations: the first as an
// unsigned varint, the rest as signed varint deltas.
// The deserialized chunk is sealed (read-only); call FromBytes to reconstruct it.
func (c *Chunk) Bytes() []byte {
	bitstream := c.bw.bytes()

	var genBuf []byte
	var tmp [binary.MaxVarintLen64]byte
	var prev int64
	for i, g := range c.gens {
		if i == 0 {
			genBuf = append(genBuf, tmp[:binary.PutUvarint(tmp[:], uint64(g))]...)
		} else {
			genBuf = append(genBuf, tmp[:binary.PutVarint(tmp[:], g-prev)]...)
		}
		prev = g
	}

	out := make([]byte, chunkHeaderLen, chunkHeaderLen+len(bitstream)+len(genBuf))
	copy(out[0:4], chunkMagicV1[:])
	binary.BigEndian.PutUint64(out[4:12], uint64(c.minTs))
	binary.BigEndian.PutUint64(out[12:20], uint64(c.maxTs))
	binary.BigEndian.PutUint16(out[20:22], c.numSamples)
	binary.BigEndian.PutUint32(out[22:26], uint32(len(bitstream)))
	out = append(out, bitstream...)
	out = append(out, genBuf...)
	return out
}

// FromBytes reconstructs a sealed, read-only Chunk from data produced by Bytes.
// It requires the chunkMagicV1 prefix; any other input (including pre-generation
// chunks) is rejected outright. It eagerly decodes all declared samples to catch
// corrupt payloads, validates that the decoded min/max timestamps match the header,
// and validates the per-sample generation section.
func FromBytes(data []byte) (*Chunk, error) {
	if len(data) < chunkHeaderLen ||
		data[0] != chunkMagicV1[0] || data[1] != chunkMagicV1[1] ||
		data[2] != chunkMagicV1[2] || data[3] != chunkMagicV1[3] {
		return nil, errors.New("chunk.FromBytes: unrecognized chunk format (pre-generation chunks are unsupported; clear the data directory)")
	}
	minTs := int64(binary.BigEndian.Uint64(data[4:12]))
	maxTs := int64(binary.BigEndian.Uint64(data[12:20]))
	numSamples := binary.BigEndian.Uint16(data[20:22])
	bitLen := binary.BigEndian.Uint32(data[22:26])
	if uint64(chunkHeaderLen)+uint64(bitLen) > uint64(len(data)) {
		return nil, fmt.Errorf("chunk.FromBytes: bitstream length %d exceeds data", bitLen)
	}
	payload := make([]byte, bitLen)
	copy(payload, data[chunkHeaderLen:chunkHeaderLen+int(bitLen)])
	c, err := decodeAndValidate(minTs, maxTs, numSamples, payload)
	if err != nil {
		return nil, err
	}
	gens, err := decodeGens(data[chunkHeaderLen+int(bitLen):], int(numSamples))
	if err != nil {
		return nil, err
	}
	c.gens = gens
	for _, g := range gens {
		if g > c.maxGen {
			c.maxGen = g
		}
	}
	return c, nil
}

// decodeAndValidate builds a sealed chunk from header fields and the ts/val
// bitstream payload, eagerly decoding all samples to catch corruption. It does not
// populate per-sample generations.
func decodeAndValidate(minTs, maxTs int64, numSamples uint16, payload []byte) (*Chunk, error) {
	c := &Chunk{
		minTs:       minTs,
		maxTs:       maxTs,
		numSamples:  numSamples,
		sealed:      true,
		bw:          bitsWriter{buf: payload},
		lastLeading: 0xff,
	}
	// An empty chunk must have zero min/max and no payload bytes.
	if numSamples == 0 {
		if minTs != 0 || maxTs != 0 {
			return nil, fmt.Errorf("chunk.FromBytes: numSamples=0 but header min/max are non-zero (%d/%d)", minTs, maxTs)
		}
		if len(payload) != 0 {
			return nil, fmt.Errorf("chunk.FromBytes: numSamples=0 but payload is non-empty (%d bytes)", len(payload))
		}
		return c, nil
	}
	// Eagerly decode all samples to catch corrupt payloads before returning.
	it := c.Iterator()
	var gotMin, gotMax int64
	first := true
	n := 0
	for it.Next() {
		ts, _ := it.At()
		if first {
			gotMin = ts
			gotMax = ts
			first = false
		} else {
			if ts < gotMin {
				gotMin = ts
			}
			if ts > gotMax {
				gotMax = ts
			}
		}
		n++
	}
	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("chunk.FromBytes: decode error at sample %d: %w", n, err)
	}
	if n != int(numSamples) {
		return nil, fmt.Errorf("chunk.FromBytes: decoded %d samples, header declared %d", n, numSamples)
	}
	if gotMin != minTs || gotMax != maxTs {
		return nil, fmt.Errorf("chunk.FromBytes: header min/max (%d/%d) does not match decoded (%d/%d)",
			minTs, maxTs, gotMin, gotMax)
	}
	// All declared samples decoded; any unread full bytes are trailing garbage.
	// Remaining bits in the current partial byte are zero-padding and are harmless.
	if it.br.pos < len(it.br.buf) {
		return nil, fmt.Errorf("chunk.FromBytes: %d trailing bytes after %d decoded samples",
			len(it.br.buf)-it.br.pos, numSamples)
	}
	return c, nil
}

// decodeGens decodes count generations: the first as an unsigned varint, the rest
// as signed varint deltas. It requires the section to be fully consumed.
func decodeGens(data []byte, count int) ([]int64, error) {
	gens := make([]int64, 0, count)
	pos := 0
	for i := 0; i < count; i++ {
		if i == 0 {
			v, n := binary.Uvarint(data[pos:])
			if n <= 0 {
				return nil, fmt.Errorf("chunk.FromBytes: bad generation varint at sample 0")
			}
			if v > uint64(MaxGeneration) {
				return nil, fmt.Errorf("chunk.FromBytes: generation at sample 0 out of range")
			}
			gens = append(gens, int64(v))
			pos += n
			continue
		}
		d, n := binary.Varint(data[pos:])
		if n <= 0 {
			return nil, fmt.Errorf("chunk.FromBytes: bad generation delta at sample %d", i)
		}
		prev := gens[i-1]
		sum := prev + d
		if (d > 0 && sum < prev) || (d < 0 && sum > prev) {
			return nil, fmt.Errorf("chunk.FromBytes: generation overflow at sample %d", i)
		}
		if sum < 0 || sum > MaxGeneration {
			return nil, fmt.Errorf("chunk.FromBytes: generation %d at sample %d out of range", sum, i)
		}
		gens = append(gens, sum)
		pos += n
	}
	if pos != len(data) {
		return nil, fmt.Errorf("chunk.FromBytes: %d trailing bytes in generation section", len(data)-pos)
	}
	return gens, nil
}

// Append encodes tsMs, val, and the sample's write generation into the chunk.
// Returns ErrChunkFull immediately if the chunk is already sealed.
// The threshold-crossing sample is written before sealing: the chunk is sealed
// after storing the sample that first meets the count or time-span threshold.
// Subsequent appends return ErrChunkFull. This matches Prometheus head chunk behavior.
func (c *Chunk) Append(tsMs int64, val float64, gen int64) error {
	if c.sealed {
		return ErrChunkFull
	}
	if gen < 0 || gen > MaxGeneration {
		return fmt.Errorf("chunk: generation %d out of range [0, %d]", gen, MaxGeneration)
	}
	c.gens = append(c.gens, gen)
	if gen > c.maxGen {
		c.maxGen = gen
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
	// maxTs >= minTs always holds, so a true span this large only wraps to a
	// negative value when it exceeds the int64 range (e.g. MinInt64..MaxInt64
	// timestamps). Treat that overflow as "span exceeded" so extreme timestamps
	// cannot bypass sealing.
	span := c.maxTs - c.minTs
	if c.numSamples >= maxSamples || span < 0 || span >= maxSpanMs {
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

// MaxGen returns the maximum write generation among the chunk's samples (0 for an
// empty or legacy chunk).
func (c *Chunk) MaxGen() int64 { return c.maxGen }

// Iterator returns a new Iterator that decodes samples from this chunk in order.
func (c *Chunk) Iterator() *Iterator {
	return &Iterator{
		br:          newBitsReader(c.bw.bytes()),
		total:       int(c.numSamples),
		gens:        c.gens,
		lastLeading: 0xff,
	}
}

// Iterator decodes samples from a Chunk in insertion order.
type Iterator struct {
	br           bitsReader
	total        int
	n            int
	gens         []int64
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

// Gen returns the write generation of the current sample. Must only be called
// after a successful Next(). Returns 0 when generations are unavailable (a legacy
// chunk decoded without per-sample generations).
func (it *Iterator) Gen() int64 {
	idx := it.n - 1
	if idx < 0 || idx >= len(it.gens) {
		return 0
	}
	return it.gens[idx]
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
