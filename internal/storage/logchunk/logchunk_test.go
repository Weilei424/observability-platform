package logchunk

import (
	"encoding/binary"
	"hash/crc32"
	"strings"
	"testing"
)

func build(entries [][2]any) *Chunk {
	c := NewChunk()
	for _, e := range entries {
		c.Append(int64(e[0].(int)), e[1].(string))
	}
	return c
}

func TestChunk_RoundTrip(t *testing.T) {
	cases := map[string][][2]any{
		"empty":        {},
		"single":       {{100, "hello"}},
		"many":         {{100, "a"}, {150, "b"}, {275, "c"}, {275, "d"}},
		"out of order": {{300, "late"}, {100, "early"}, {200, "mid"}},
		"utf8":         {{100, "héllo → 世界"}},
		"empty line":   {{100, ""}, {200, "x"}},
	}
	for name, entries := range cases {
		t.Run(name, func(t *testing.T) {
			c := build(entries)
			got, err := FromBytes(c.Bytes())
			if err != nil {
				t.Fatalf("FromBytes: %v", err)
			}
			if got.NumEntries() != len(entries) {
				t.Fatalf("NumEntries = %d, want %d", got.NumEntries(), len(entries))
			}
			if got.MinTs() != c.MinTs() || got.MaxTs() != c.MaxTs() {
				t.Fatalf("min/max = %d/%d, want %d/%d", got.MinTs(), got.MaxTs(), c.MinTs(), c.MaxTs())
			}
			it := c.Iterator()
			it2 := got.Iterator()
			for it.Next() {
				if !it2.Next() {
					t.Fatal("decoded chunk has fewer entries")
				}
				ts1, l1 := it.At()
				ts2, l2 := it2.At()
				if ts1 != ts2 || l1 != l2 {
					t.Fatalf("entry mismatch: (%d,%q) vs (%d,%q)", ts1, l1, ts2, l2)
				}
			}
			if it2.Next() {
				t.Fatal("decoded chunk has more entries")
			}
		})
	}
}

func TestChunk_CompressionShrinksRepetitiveBlock(t *testing.T) {
	c := NewChunk()
	line := strings.Repeat("the quick brown fox ", 20)
	rawBytes := 0
	for i := 0; i < 500; i++ {
		c.Append(int64(1000+i), line)
		rawBytes += len(line)
	}
	if got := len(c.Bytes()); got >= rawBytes {
		t.Fatalf("compressed size %d not smaller than raw %d", got, rawBytes)
	}
}

func TestChunk_UncompressedBytesMatchesHeader(t *testing.T) {
	c := build([][2]any{{100, "abc"}, {200, "defgh"}})
	// header uncompressedLen lives at bytes [25:29]; see layout.
	data := c.Bytes()
	hdr := int(uint32(data[25])<<24 | uint32(data[26])<<16 | uint32(data[27])<<8 | uint32(data[28]))
	if hdr != c.UncompressedBytes() {
		t.Fatalf("header uncompressedLen %d != UncompressedBytes() %d", hdr, c.UncompressedBytes())
	}
}

func TestPeekBounds(t *testing.T) {
	c := build([][2]any{{300, "a"}, {100, "b"}, {200, "c"}})
	data := c.Bytes()
	minTs, maxTs, n, err := PeekBounds(data)
	if err != nil {
		t.Fatalf("PeekBounds: %v", err)
	}
	if minTs != 100 || maxTs != 300 || n != 3 {
		t.Fatalf("PeekBounds = (%d,%d,%d), want (100,300,3)", minTs, maxTs, n)
	}
	if _, _, _, err := PeekBounds(data[:10]); err == nil {
		t.Error("expected error for a short header")
	}
	bad := append([]byte(nil), data...)
	bad[0] ^= 0xff
	if _, _, _, err := PeekBounds(bad); err == nil {
		t.Error("expected error for bad magic")
	}
}

func TestFromBytes_Rejects(t *testing.T) {
	good := build([][2]any{{100, "a"}, {200, "b"}}).Bytes()

	t.Run("short", func(t *testing.T) {
		if _, err := FromBytes(good[:10]); err == nil {
			t.Error("expected error for short input")
		}
	})
	t.Run("bad magic", func(t *testing.T) {
		bad := append([]byte(nil), good...)
		bad[0] ^= 0xff
		if _, err := FromBytes(bad); err == nil {
			t.Error("expected error for bad magic")
		}
	})
	t.Run("bad version", func(t *testing.T) {
		bad := append([]byte(nil), good...)
		bad[4] = 0x7f
		if _, err := FromBytes(bad); err == nil {
			t.Error("expected error for bad version")
		}
	})
	t.Run("trailing bytes", func(t *testing.T) {
		bad := append(append([]byte(nil), good...), 0x00)
		if _, err := FromBytes(bad); err == nil {
			t.Error("expected error for trailing bytes")
		}
	})
	t.Run("corrupt compressed payload", func(t *testing.T) {
		bad := append([]byte(nil), good...)
		bad[len(bad)-1] ^= 0xff
		if _, err := FromBytes(bad); err == nil {
			t.Error("expected error for corrupt payload")
		}
	})
	t.Run("oversized uncompLen", func(t *testing.T) {
		// Force the uncompLen header field (bytes [25:29]) far above the 128 MiB cap,
		// re-signing the header CRC so the cap check — not the CRC check — is what
		// rejects it.
		bad := append([]byte(nil), good...)
		binary.BigEndian.PutUint32(bad[25:29], 0xffffffff)
		binary.BigEndian.PutUint32(bad[33:37], crc32.Checksum(bad[:headerCRCScope], crcTable))
		if _, err := FromBytes(bad); err == nil {
			t.Error("expected error for oversized uncompLen")
		}
	})
	t.Run("tampered header min/max", func(t *testing.T) {
		// Corrupt the header minTs (bytes [5:13]) so it disagrees with the decoded
		// entries. CRC covers only the compressed payload, so this reaches the check.
		bad := append([]byte(nil), good...)
		bad[5] ^= 0xff
		if _, err := FromBytes(bad); err == nil {
			t.Error("expected error for tampered header min/max")
		}
	})
}

func TestFromBytes_RejectsLegacyVersion(t *testing.T) {
	// A version-1 chunk (pre header-CRC, 37-byte header) must be rejected via the
	// version discriminator at offset 4, not misread as the new layout.
	good := build([][2]any{{100, "a"}, {200, "b"}}).Bytes()
	v1 := append([]byte(nil), good...)
	v1[4] = 1
	if _, err := FromBytes(v1); err == nil {
		t.Error("FromBytes should reject a version-1 chunk")
	}
	if _, _, _, err := PeekBounds(v1); err == nil {
		t.Error("PeekBounds should reject a version-1 chunk")
	}
}

// TestDecodeEntries_RejectsOverflowLineLength guards the unsigned bounds check in
// decodeEntries: a forged huge line-length must return an error, not panic on the
// slice (int(ll) would wrap negative and slip past a signed comparison).
func TestDecodeEntries_RejectsOverflowLineLength(t *testing.T) {
	var buf []byte
	var tmp [binary.MaxVarintLen64]byte
	buf = append(buf, tmp[:binary.PutVarint(tmp[:], 100)]...)         // first ts
	buf = append(buf, tmp[:binary.PutUvarint(tmp[:], ^uint64(0))]...) // huge line length
	// no line bytes follow
	if _, err := decodeEntries(buf, 1); err == nil {
		t.Error("expected error for overflow line length (must not panic)")
	}
}
