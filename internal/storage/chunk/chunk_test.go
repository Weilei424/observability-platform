package chunk_test

import (
	"math"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/storage/chunk"
)

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
	for _, s := range samples {
		if err := c.Append(s.ts, s.val); err != nil {
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
		if err := c.Append(int64(i*1000), 42.0); err != nil {
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
		if err := c.Append(int64(i*15000), float64(i*100)); err != nil {
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
		if err := c.Append(int64(i*15000), float64(i)); err != nil {
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
		if err := c.Append(ts, values[i]); err != nil {
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
		if err := c.Append(int64(i*1000), v); err != nil {
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
		if err := c.Append(int64(i*1000), float64(i)); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if !c.Sealed() {
		t.Error("chunk should be sealed after 120 samples")
	}
	if c.NumSamples() != 120 {
		t.Errorf("NumSamples() = %d, want 120", c.NumSamples())
	}
	if err := c.Append(120000, 120.0); err != chunk.ErrChunkFull {
		t.Errorf("121st Append: got %v, want ErrChunkFull", err)
	}
}

func TestChunk_SealByTimeSpan(t *testing.T) {
	c := chunk.NewChunk()
	if err := c.Append(0, 1.0); err != nil {
		t.Fatalf("first Append: %v", err)
	}
	if c.Sealed() {
		t.Error("should not be sealed after 1 sample")
	}
	// 2 hours + 1 ms
	if err := c.Append(7_200_001, 2.0); err != nil {
		t.Fatalf("second Append: %v", err)
	}
	if !c.Sealed() {
		t.Error("chunk should be sealed after 2-hour span")
	}
	if err := c.Append(14_400_000, 3.0); err != chunk.ErrChunkFull {
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
	if err := c.Append(5000, 3.14); err != nil {
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
	for _, s := range want {
		if err := c.Append(s.ts, s.val); err != nil {
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
	}
	if it.Next() {
		t.Error("Next() = true after all samples exhausted")
	}
	if err := it.Err(); err != nil {
		t.Errorf("iterator error: %v", err)
	}
}

func TestChunk_FromBytes_TooShort(t *testing.T) {
	_, err := chunk.FromBytes([]byte{0x00, 0x01})
	if err == nil {
		t.Error("expected error for truncated data, got nil")
	}
}

func TestChunk_FromBytes_ZeroSamplesNonEmptyPayload(t *testing.T) {
	// numSamples=0 header with a non-empty payload is corrupt.
	data := make([]byte, 18+4) // header-only + 4 garbage bytes
	// minTs=0, maxTs=0, numSamples=0 (all zeros already), payload=[0,0,0,0]
	_, err := chunk.FromBytes(data)
	if err == nil {
		t.Error("expected error for numSamples=0 with non-empty payload, got nil")
	}
}

func TestChunk_FromBytes_ZeroSamplesNonZeroMinMax(t *testing.T) {
	// numSamples=0 header with non-zero minTs is corrupt.
	data := make([]byte, 18) // header only, no payload
	// Set minTs=1000 but numSamples=0
	data[7] = 0xe8 // big-endian 1000 in last byte of uint64
	_, err := chunk.FromBytes(data)
	if err == nil {
		t.Error("expected error for numSamples=0 with non-zero minTs, got nil")
	}
}

func TestChunk_FromBytes_ZeroSamplesValid(t *testing.T) {
	// numSamples=0, minTs=0, maxTs=0, empty payload — this is a valid empty chunk.
	data := make([]byte, 18)
	c, err := chunk.FromBytes(data)
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
	// Build a valid header declaring 5 samples but supply a zeroed payload.
	// The eager decoder must return an error rather than silently succeeding.
	data := make([]byte, 18+4) // header + 4 bytes of garbage (zeros)
	// minTs = 1000, maxTs = 5000, numSamples = 5
	putUint64BE := func(b []byte, v uint64) { b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7] = byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8), byte(v) }
	putUint64BE(data[0:8], 1000)
	putUint64BE(data[8:16], 5000)
	data[16], data[17] = 0, 5 // numSamples = 5
	// payload is all zeros — too short to encode 5 samples
	_, err := chunk.FromBytes(data)
	if err == nil {
		t.Error("expected error for corrupt payload, got nil")
	}
}

func TestChunk_FromBytes_WrongSampleCount(t *testing.T) {
	// Serialize a 3-sample chunk, then doctor the numSamples header field to 5.
	c := chunk.NewChunk()
	for i := 0; i < 3; i++ {
		if err := c.Append(int64((i+1)*1000), float64(i)); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	data := c.Bytes()
	data[16], data[17] = 0, 5 // claim 5 samples when only 3 are encoded
	_, err := chunk.FromBytes(data)
	if err == nil {
		t.Error("expected error for mismatched sample count, got nil")
	}
}

func TestChunk_FromBytes_WrongMinMax(t *testing.T) {
	// Serialize a real chunk, then corrupt the minTs header field.
	c := chunk.NewChunk()
	for i := 0; i < 3; i++ {
		if err := c.Append(int64((i+1)*1000), float64(i)); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	data := c.Bytes()
	// Overwrite minTs with an obviously wrong value (0 instead of 1000)
	for i := 0; i < 8; i++ {
		data[i] = 0
	}
	_, err := chunk.FromBytes(data)
	if err == nil {
		t.Error("expected error for wrong minTs in header, got nil")
	}
}

func TestChunk_BytesFromBytes_SealedByCount(t *testing.T) {
	c := chunk.NewChunk()
	for i := 0; i < 120; i++ {
		if err := c.Append(int64(i*1000), float64(i)); err != nil {
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
