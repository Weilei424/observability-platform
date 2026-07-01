package chunk_test

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/storage/chunk"
)

// TestChunk_ExtremeTimestampsSeal is the regression for the span-overflow bug: a
// chunk spanning MinInt64..MaxInt64 has a true span far beyond the two-hour seal
// threshold, but the naive maxTs-minTs subtraction overflows to a negative value.
// The sealing check must treat that as span-exceeded, not leave the chunk open.
func TestChunk_ExtremeTimestampsSeal(t *testing.T) {
	c := chunk.NewChunk()
	if err := c.Append(math.MinInt64, 1.0, 1); err != nil {
		t.Fatalf("Append min: %v", err)
	}
	if c.Sealed() {
		t.Fatal("chunk sealed after a single sample")
	}
	if err := c.Append(math.MaxInt64, 2.0, 2); err != nil {
		t.Fatalf("Append max: %v", err)
	}
	if !c.Sealed() {
		t.Fatal("chunk with a MinInt64..MaxInt64 span was not sealed (span overflow bypassed sealing)")
	}
}

// --- round-trip tests ---

func TestChunk_RoundTrip_VariedValues(t *testing.T) {
	c := chunk.NewChunk()
	type sample struct {
		ts  int64
		val float64
	}
	samples := []sample{
		{1000, 1.5}, {2000, 2.7}, {3000, 0.1}, {4000, 100.0}, {5000, -3.14},
	}
	for i, s := range samples {
		if err := c.Append(s.ts, s.val, int64(i)); err != nil {
			t.Fatalf("Append(%d, %f): %v", s.ts, s.val, err)
		}
	}
	it := c.Iterator()
	for i, want := range samples {
		if !it.Next() {
			t.Fatalf("sample %d: Next() = false", i)
		}
		gotTs, gotVal := it.At()
		if gotTs != want.ts {
			t.Errorf("sample %d: ts = %d, want %d", i, gotTs, want.ts)
		}
		if gotVal != want.val {
			t.Errorf("sample %d: val = %f, want %f", i, gotVal, want.val)
		}
	}
	if it.Next() {
		t.Error("Next() returned true after all samples exhausted")
	}
	if err := it.Err(); err != nil {
		t.Errorf("iterator error: %v", err)
	}
}

func TestChunk_RoundTrip_ConstantValue(t *testing.T) {
	c := chunk.NewChunk()
	for i := 0; i < 10; i++ {
		if err := c.Append(int64(i*1000), 42.0, int64(i)); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	it := c.Iterator()
	for i := 0; i < 10; i++ {
		if !it.Next() {
			t.Fatalf("sample %d: Next() = false", i)
		}
		_, val := it.At()
		if val != 42.0 {
			t.Errorf("sample %d: val = %f, want 42.0", i, val)
		}
	}
	if err := it.Err(); err != nil {
		t.Errorf("iterator error: %v", err)
	}
}

func TestChunk_RoundTrip_MonotonicCounter(t *testing.T) {
	c := chunk.NewChunk()
	for i := 0; i < 20; i++ {
		if err := c.Append(int64(i*15000), float64(i*100), int64(i)); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	it := c.Iterator()
	for i := 0; i < 20; i++ {
		if !it.Next() {
			t.Fatalf("sample %d: Next() = false", i)
		}
		gotTs, gotVal := it.At()
		if gotTs != int64(i*15000) || gotVal != float64(i*100) {
			t.Errorf("sample %d: got (%d, %f), want (%d, %f)",
				i, gotTs, gotVal, int64(i*15000), float64(i*100))
		}
	}
	if err := it.Err(); err != nil {
		t.Errorf("iterator error: %v", err)
	}
}

func TestChunk_RoundTrip_RegularScrapeInterval(t *testing.T) {
	// delta-of-delta = 0 for every tick — the cheapest timestamp path
	c := chunk.NewChunk()
	for i := 0; i < 30; i++ {
		if err := c.Append(int64(i*15000), float64(i), int64(i)); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	it := c.Iterator()
	for i := 0; i < 30; i++ {
		if !it.Next() {
			t.Fatalf("sample %d: Next() = false", i)
		}
		gotTs, _ := it.At()
		if gotTs != int64(i*15000) {
			t.Errorf("sample %d: ts = %d, want %d", i, gotTs, int64(i*15000))
		}
	}
	if err := it.Err(); err != nil {
		t.Errorf("iterator error: %v", err)
	}
}

func TestChunk_RoundTrip_IrregularTimestamps(t *testing.T) {
	c := chunk.NewChunk()
	timestamps := []int64{100, 250, 310, 900, 1001, 5000}
	values := []float64{1.1, 2.2, 3.3, 4.4, 5.5, 6.6}
	for i, ts := range timestamps {
		if err := c.Append(ts, values[i], int64(i)); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	it := c.Iterator()
	for i := range timestamps {
		if !it.Next() {
			t.Fatalf("sample %d: Next() = false", i)
		}
		gotTs, gotVal := it.At()
		if gotTs != timestamps[i] || gotVal != values[i] {
			t.Errorf("sample %d: got (%d, %f), want (%d, %f)",
				i, gotTs, gotVal, timestamps[i], values[i])
		}
	}
	if err := it.Err(); err != nil {
		t.Errorf("iterator error: %v", err)
	}
}

func TestChunk_RoundTrip_NaNAndInf(t *testing.T) {
	c := chunk.NewChunk()
	vals := []float64{math.NaN(), math.Inf(1), math.Inf(-1), 1.0}
	for i, v := range vals {
		if err := c.Append(int64(i*1000), v, int64(i)); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	it := c.Iterator()
	for i, want := range vals {
		if !it.Next() {
			t.Fatalf("sample %d: Next() = false", i)
		}
		_, got := it.At()
		if math.IsNaN(want) {
			if !math.IsNaN(got) {
				t.Errorf("sample %d: got %f, want NaN", i, got)
			}
		} else if got != want {
			t.Errorf("sample %d: got %f, want %f", i, got, want)
		}
	}
	if err := it.Err(); err != nil {
		t.Errorf("iterator error: %v", err)
	}
}

// --- seal threshold tests ---

func TestChunk_SealByCount(t *testing.T) {
	c := chunk.NewChunk()
	for i := 0; i < 120; i++ {
		if err := c.Append(int64(i*1000), float64(i), int64(i)); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if !c.Sealed() {
		t.Error("chunk should be sealed after 120 samples")
	}
	if c.NumSamples() != 120 {
		t.Errorf("NumSamples() = %d, want 120", c.NumSamples())
	}
	if err := c.Append(120000, 120.0, 120); err != chunk.ErrChunkFull {
		t.Errorf("121st Append: got %v, want ErrChunkFull", err)
	}
}

func TestChunk_SealByTimeSpan(t *testing.T) {
	c := chunk.NewChunk()
	if err := c.Append(0, 1.0, 1); err != nil {
		t.Fatalf("first Append: %v", err)
	}
	if c.Sealed() {
		t.Error("should not be sealed after 1 sample")
	}
	// 2 hours + 1 ms
	if err := c.Append(7_200_001, 2.0, 2); err != nil {
		t.Fatalf("second Append: %v", err)
	}
	if !c.Sealed() {
		t.Error("chunk should be sealed after 2-hour span")
	}
	if err := c.Append(14_400_000, 3.0, 3); err != chunk.ErrChunkFull {
		t.Errorf("3rd Append on sealed chunk: got %v, want ErrChunkFull", err)
	}
	if c.MinTs() != 0 {
		t.Errorf("MinTs() = %d, want 0", c.MinTs())
	}
	if c.MaxTs() != 7_200_001 {
		t.Errorf("MaxTs() = %d, want 7200001", c.MaxTs())
	}
	// Both samples must be readable
	it := c.Iterator()
	count := 0
	for it.Next() {
		count++
	}
	if count != 2 {
		t.Errorf("got %d samples from sealed chunk, want 2", count)
	}
	if err := it.Err(); err != nil {
		t.Errorf("iterator error: %v", err)
	}
}

// --- edge case tests ---

func TestChunk_SingleSample(t *testing.T) {
	c := chunk.NewChunk()
	if err := c.Append(5000, 3.14, 1); err != nil {
		t.Fatalf("Append: %v", err)
	}
	it := c.Iterator()
	if !it.Next() {
		t.Fatal("Next() = false for single-sample chunk")
	}
	ts, val := it.At()
	if ts != 5000 || val != 3.14 {
		t.Errorf("got (%d, %f), want (5000, 3.14)", ts, val)
	}
	if it.Next() {
		t.Error("Next() = true after only sample")
	}
}

func TestChunk_EmptyChunk(t *testing.T) {
	c := chunk.NewChunk()
	if c.NumSamples() != 0 {
		t.Errorf("NumSamples() = %d, want 0", c.NumSamples())
	}
	it := c.Iterator()
	if it.Next() {
		t.Error("Next() = true on empty chunk")
	}
}

// --- serialization tests ---

func TestChunk_BytesFromBytes_RoundTrip(t *testing.T) {
	c := chunk.NewChunk()
	type sample struct {
		ts  int64
		val float64
	}
	want := []sample{
		{1000, 1.1}, {2000, 2.2}, {3000, 3.3}, {4000, 4.4}, {5000, 5.5},
	}
	for i, s := range want {
		if err := c.Append(s.ts, s.val, int64(i)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	data := c.Bytes()
	c2, err := chunk.FromBytes(data)
	if err != nil {
		t.Fatalf("FromBytes: %v", err)
	}

	if c2.NumSamples() != len(want) {
		t.Errorf("NumSamples() = %d, want %d", c2.NumSamples(), len(want))
	}
	if c2.MinTs() != want[0].ts {
		t.Errorf("MinTs() = %d, want %d", c2.MinTs(), want[0].ts)
	}
	if c2.MaxTs() != want[len(want)-1].ts {
		t.Errorf("MaxTs() = %d, want %d", c2.MaxTs(), want[len(want)-1].ts)
	}
	if !c2.Sealed() {
		t.Error("FromBytes chunk should be sealed")
	}

	it := c2.Iterator()
	for i, s := range want {
		if !it.Next() {
			t.Fatalf("sample %d: Next() = false", i)
		}
		gotTs, gotVal := it.At()
		if gotTs != s.ts || gotVal != s.val {
			t.Errorf("sample %d: got (%d, %f), want (%d, %f)", i, gotTs, gotVal, s.ts, s.val)
		}
		if it.Gen() != int64(i) {
			t.Errorf("sample %d: gen = %d, want %d", i, it.Gen(), int64(i))
		}
	}
	if it.Next() {
		t.Error("Next() = true after all samples exhausted")
	}
	if err := it.Err(); err != nil {
		t.Errorf("iterator error: %v", err)
	}
}

// v1Header builds the 26-byte generation-aware chunk header (magic + fields).
func v1Header(minTs, maxTs int64, numSamples uint16, bitstreamLen uint32) []byte {
	h := make([]byte, 26)
	h[0], h[1], h[2], h[3] = 0x9C, 'C', 'H', 0x01
	binary.BigEndian.PutUint64(h[4:12], uint64(minTs))
	binary.BigEndian.PutUint64(h[12:20], uint64(maxTs))
	binary.BigEndian.PutUint16(h[20:22], numSamples)
	binary.BigEndian.PutUint32(h[22:26], bitstreamLen)
	return h
}

func TestChunk_FromBytes_TooShort(t *testing.T) {
	// Correct magic but fewer than the 26 header bytes.
	_, err := chunk.FromBytes([]byte{0x9C, 'C', 'H', 0x01, 0x00})
	if err == nil {
		t.Error("expected error for truncated data, got nil")
	}
}

func TestChunk_FromBytes_RejectsUnknownFormat(t *testing.T) {
	// A 26+ byte payload without the magic prefix (e.g. a pre-generation chunk) is rejected.
	if _, err := chunk.FromBytes(make([]byte, 30)); err == nil {
		t.Error("expected error for chunk without the v1 magic, got nil")
	}
}

func TestChunk_FromBytes_ZeroSamplesNonEmptyPayload(t *testing.T) {
	// numSamples=0 with a non-empty bitstream is corrupt.
	data := append(v1Header(0, 0, 0, 4), 0, 0, 0, 0)
	if _, err := chunk.FromBytes(data); err == nil {
		t.Error("expected error for numSamples=0 with non-empty payload, got nil")
	}
}

func TestChunk_FromBytes_ZeroSamplesNonZeroMinMax(t *testing.T) {
	// numSamples=0 with a non-zero minTs is corrupt.
	if _, err := chunk.FromBytes(v1Header(1000, 0, 0, 0)); err == nil {
		t.Error("expected error for numSamples=0 with non-zero minTs, got nil")
	}
}

func TestChunk_FromBytes_ZeroSamplesValid(t *testing.T) {
	// A freshly serialized empty chunk round-trips to a valid empty chunk.
	c, err := chunk.FromBytes(chunk.NewChunk().Bytes())
	if err != nil {
		t.Fatalf("unexpected error for valid empty chunk: %v", err)
	}
	if c.NumSamples() != 0 {
		t.Errorf("NumSamples() = %d, want 0", c.NumSamples())
	}
	if c.Iterator().Next() {
		t.Error("Next() = true on empty deserialized chunk")
	}
}

func TestChunk_FromBytes_CorruptPayload(t *testing.T) {
	// A header declaring 5 samples with a 4-byte zero bitstream cannot decode.
	data := append(v1Header(1000, 5000, 5, 4), 0, 0, 0, 0)
	if _, err := chunk.FromBytes(data); err == nil {
		t.Error("expected error for corrupt payload, got nil")
	}
}

func TestChunk_FromBytes_WrongSampleCount(t *testing.T) {
	// Serialize a 3-sample chunk, then doctor the numSamples header field to 5.
	c := chunk.NewChunk()
	for i := 0; i < 3; i++ {
		if err := c.Append(int64((i+1)*1000), float64(i), int64(i)); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	data := c.Bytes()
	data[20], data[21] = 0, 5 // numSamples is at [20:22]; claim 5 when only 3 are encoded
	if _, err := chunk.FromBytes(data); err == nil {
		t.Error("expected error for mismatched sample count, got nil")
	}
}

func TestChunk_FromBytes_WrongMinMax(t *testing.T) {
	// Serialize a real chunk, then corrupt the minTs header field.
	c := chunk.NewChunk()
	for i := 0; i < 3; i++ {
		if err := c.Append(int64((i+1)*1000), float64(i), int64(i)); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	data := c.Bytes()
	for i := 4; i < 12; i++ { // minTs occupies [4:12]
		data[i] = 0
	}
	if _, err := chunk.FromBytes(data); err == nil {
		t.Error("expected error for wrong minTs in header, got nil")
	}
}

func TestChunk_FromBytes_TrailingGarbage(t *testing.T) {
	// A valid serialized chunk with extra bytes appended must be rejected.
	c := chunk.NewChunk()
	for i := 0; i < 5; i++ {
		if err := c.Append(int64((i+1)*1000), float64(i), int64(i)); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	data := append(c.Bytes(), 0x00, 0xff) // trailing garbage
	if _, err := chunk.FromBytes(data); err == nil {
		t.Error("expected error for trailing garbage bytes, got nil")
	}
}

func TestChunk_Append_RejectsOutOfRangeGeneration(t *testing.T) {
	c := chunk.NewChunk()
	if err := c.Append(1000, 1.0, -1); err == nil {
		t.Error("expected error for negative generation, got nil")
	}
	if err := c.Append(1000, 1.0, int64(1)<<62+1); err == nil {
		t.Error("expected error for too-large generation, got nil")
	}
	if c.NumSamples() != 0 {
		t.Errorf("rejected appends left %d samples, want 0", c.NumSamples())
	}
}

func TestChunk_FromBytes_RejectsOutOfRangeGeneration(t *testing.T) {
	// Replace a single-sample chunk's generation section with a uvarint exceeding MaxInt64.
	c := chunk.NewChunk()
	if err := c.Append(1000, 1.0, 5); err != nil {
		t.Fatalf("Append: %v", err)
	}
	data := c.Bytes()
	genStart := 26 + int(binary.BigEndian.Uint32(data[22:26]))
	overflow := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01} // > math.MaxInt64
	corrupted := append(append([]byte{}, data[:genStart]...), overflow...)
	if _, err := chunk.FromBytes(corrupted); err == nil {
		t.Error("expected error for out-of-range generation, got nil")
	}
}

func TestChunk_BytesFromBytes_SealedByCount(t *testing.T) {
	c := chunk.NewChunk()
	for i := 0; i < 120; i++ {
		if err := c.Append(int64(i*1000), float64(i), int64(i)); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if !c.Sealed() {
		t.Fatal("chunk should be sealed after 120 samples")
	}

	data := c.Bytes()
	c2, err := chunk.FromBytes(data)
	if err != nil {
		t.Fatalf("FromBytes: %v", err)
	}
	if c2.NumSamples() != 120 {
		t.Errorf("NumSamples() = %d, want 120", c2.NumSamples())
	}
	it := c2.Iterator()
	n := 0
	for it.Next() {
		n++
	}
	if n != 120 {
		t.Errorf("iterated %d samples, want 120", n)
	}
	if err := it.Err(); err != nil {
		t.Errorf("iterator error: %v", err)
	}
}
