// internal/storage/block/postings_test.go
package block

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/storage/chunk"
	"github.com/masonwheeler/observability-platform/internal/storage/index"
)

func sealedChunk(t *testing.T, ts int64) *chunk.Chunk {
	t.Helper()
	c := chunk.NewChunk()
	for i := int64(0); i < 120; i++ { // fill to the 120-sample seal threshold
		if err := c.Append(ts+i, float64(i)); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	return c
}

func writeTestBlock(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	blocksDir := filepath.Join(root, "blocks")
	tmpDir := filepath.Join(root, "tmp")
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		t.Fatal(err)
	}
	w, err := NewWriter(blocksDir, tmpDir)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	_ = w.AddSeries(1, []LabelPair{{"__name__", "http"}, {"job", "api"}}, []*chunk.Chunk{sealedChunk(t, 1000)})
	_ = w.AddSeries(2, []LabelPair{{"__name__", "http"}, {"job", "web"}}, []*chunk.Chunk{sealedChunk(t, 1000)})
	meta, err := w.Commit()
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	return filepath.Join(blocksDir, meta.BlockID)
}

func TestReader_Postings_Persisted(t *testing.T) {
	dir := writeTestBlock(t)
	if _, err := os.Stat(filepath.Join(dir, "postings")); err != nil {
		t.Fatalf("postings file not written: %v", err)
	}
	r, err := OpenReader(dir)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	got, err := r.Postings([]index.Pair{{Name: "__name__", Value: "http"}, {Name: "job", Value: "api"}})
	if err != nil {
		t.Fatalf("Postings: %v", err)
	}
	if !reflect.DeepEqual(got, []uint64{1}) {
		t.Fatalf("Postings = %v, want [1]", got)
	}
	// Empty matchers -> allRefs sentinel -> every series in the block.
	all, err := r.Postings(nil)
	if err != nil {
		t.Fatalf("Postings(nil): %v", err)
	}
	if !reflect.DeepEqual(all, []uint64{1, 2}) {
		t.Fatalf("Postings(nil) = %v, want [1 2]", all)
	}
	if got := r.LabelValues("job"); !reflect.DeepEqual(got, []string{"api", "web"}) {
		t.Fatalf("LabelValues(job) = %v, want [api web]", got)
	}
	if got := r.LabelNames(); !reflect.DeepEqual(got, []string{"__name__", "job"}) {
		t.Fatalf("LabelNames = %v, want [__name__ job]", got)
	}
}

func TestReader_Postings_RebuildFallback(t *testing.T) {
	dir := writeTestBlock(t)
	// Simulate a block written before this change.
	if err := os.Remove(filepath.Join(dir, "postings")); err != nil {
		t.Fatalf("remove postings: %v", err)
	}
	r, err := OpenReader(dir)
	if err != nil {
		t.Fatalf("OpenReader (fallback): %v", err)
	}
	defer r.Close()
	got, err := r.Postings([]index.Pair{{Name: "job", Value: "web"}})
	if err != nil {
		t.Fatalf("fallback Postings: %v", err)
	}
	if !reflect.DeepEqual(got, []uint64{2}) {
		t.Fatalf("fallback Postings = %v, want [2]", got)
	}
}

// TestReader_Postings_HugeListCount_FailsAtOpen verifies that an inflated list
// count is caught eagerly at OpenReader time, so corruption surfaces as a load
// failure rather than being lazily converted into silently-missing query data.
func TestReader_Postings_HugeListCount_FailsAtOpen(t *testing.T) {
	dir := writeTestBlock(t)
	path := filepath.Join(dir, "postings")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read postings: %v", err)
	}
	// Inflate the first list's 4-byte count (at postingsHeaderSz) so its body
	// would run past the offset table.
	binary.BigEndian.PutUint32(data[postingsHeaderSz:], 0x0FFFFFFF)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write corrupt postings: %v", err)
	}
	if _, err := OpenReader(dir); err == nil {
		t.Fatal("OpenReader on inflated postings list count: want error, got nil")
	}
}

// TestReader_Postings_ReducedListCount_FailsAtOpen verifies that a list count
// smaller than its true length (which still fits inside the data region, so the
// upper-bound check alone would pass) is rejected at open. Such a count would
// otherwise silently drop trailing IDs from query results.
func TestReader_Postings_ReducedListCount_FailsAtOpen(t *testing.T) {
	dir := writeTestBlock(t)
	path := filepath.Join(dir, "postings")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read postings: %v", err)
	}
	// The allRefs sentinel list is written first, so its 4-byte count sits
	// immediately after the header. Reduce it by one.
	cnt := binary.BigEndian.Uint32(data[postingsHeaderSz:])
	if cnt < 2 {
		t.Fatalf("allRefs count = %d, want >= 2 to reduce", cnt)
	}
	binary.BigEndian.PutUint32(data[postingsHeaderSz:], cnt-1)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write corrupt postings: %v", err)
	}
	if _, err := OpenReader(dir); err == nil {
		t.Fatal("OpenReader on reduced postings list count: want error, got nil")
	}
}

// TestReader_Postings_UnknownIDInList_FailsAtOpen verifies that a postings list
// referencing a series ID not present in the block index is rejected at open.
func TestReader_Postings_UnknownIDInList_FailsAtOpen(t *testing.T) {
	dir := writeTestBlock(t)
	path := filepath.Join(dir, "postings")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read postings: %v", err)
	}
	// The allRefs list body begins with its 4-byte count at postingsHeaderSz,
	// followed by its first 8-byte series ID. Overwrite that first ID with a
	// value that does not exist in the block (series IDs are 1 and 2).
	firstIDOff := postingsHeaderSz + 4
	binary.BigEndian.PutUint64(data[firstIDOff:], 999999)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write corrupt postings: %v", err)
	}
	if _, err := OpenReader(dir); err == nil {
		t.Fatal("OpenReader on unknown postings ID: want error, got nil")
	}
}

func TestBuildSeriesByID_RejectsDuplicateID(t *testing.T) {
	entries := []SeriesEntry{
		{ID: 1, Labels: []LabelPair{{"__name__", "a"}}},
		{ID: 1, Labels: []LabelPair{{"__name__", "b"}}},
	}
	if _, err := buildSeriesByID(entries); err == nil {
		t.Fatal("buildSeriesByID with duplicate ID: want error, got nil")
	}
}

func TestReader_Postings_CorruptIsError(t *testing.T) {
	dir := writeTestBlock(t)
	// Truncate the footer so the offset table cannot be located.
	if err := os.WriteFile(filepath.Join(dir, "postings"), []byte("OPPI\x01garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenReader(dir); err == nil {
		t.Fatal("OpenReader on corrupt postings: want error, got nil")
	}
}

// TestReader_Postings_CorruptOffsetIsError verifies that a corrupt postings
// file does not cause a panic or a huge allocation. Two sub-tests cover the two
// hardened paths:
//
//  1. A list offset stored in the offset table that leaves no room for the
//     4-byte count header (caught at open time by the tightened upper-bound check).
//  2. A list count field that is inflated to a huge value so the claimed body
//     would exceed the offset-table boundary (caught in readList before allocating).
func TestReader_Postings_CorruptOffsetIsError(t *testing.T) {
	t.Run("offset_too_close_to_otOffset", func(t *testing.T) {
		dir := writeTestBlock(t)
		path := filepath.Join(dir, "postings")

		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read postings: %v", err)
		}
		if len(data) < postingsFooterSz {
			t.Fatalf("postings file too short: %d bytes", len(data))
		}

		// Parse otOffset from the last 8 bytes.
		otOffset := int64(binary.BigEndian.Uint64(data[len(data)-postingsFooterSz:]))

		// The offset table starts at otOffset; its first 4 bytes are the entry
		// count, then each entry is: len(name)(4) + name + len(val)(4) + val +
		// listOffset(8). Skip the count (4 bytes) and the first entry's key
		// strings to reach the first listOffset field.
		pos := int(otOffset) + 4 // skip numEntries
		// skip name string: 4-byte len + data
		nameLen := int(binary.BigEndian.Uint32(data[pos:]))
		pos += 4 + nameLen
		// skip value string: 4-byte len + data
		valLen := int(binary.BigEndian.Uint32(data[pos:]))
		pos += 4 + valLen
		// pos now points at the 8-byte listOffset of the first entry.
		// Overwrite it with otOffset-2 (within 2 bytes of the OT: no room for
		// the 4-byte count, so off+4 > otOffset).
		corruptOff := uint64(otOffset - 2)
		binary.BigEndian.PutUint64(data[pos:], corruptOff)

		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatalf("write corrupt postings: %v", err)
		}

		_, openErr := OpenReader(dir)
		if openErr == nil {
			t.Fatal("OpenReader with corrupt list offset: want error, got nil (possible huge allocation risk)")
		}
		// Just confirm error is non-nil; message format may vary.
	})

	t.Run("huge_list_count", func(t *testing.T) {
		dir := writeTestBlock(t)
		path := filepath.Join(dir, "postings")

		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read postings: %v", err)
		}
		if len(data) < postingsFooterSz {
			t.Fatalf("postings file too short: %d bytes", len(data))
		}

		// Parse otOffset from the last 8 bytes.
		otOffset := int64(binary.BigEndian.Uint64(data[len(data)-postingsFooterSz:]))

		// The first real list in the data region begins at postingsHeaderSz (5).
		// Overwrite its 4-byte count with a huge value so
		// off + 4 + cnt*8 >> otOffset.
		listCountOff := postingsHeaderSz
		binary.BigEndian.PutUint32(data[listCountOff:], 0x0FFFFFFF)

		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatalf("write corrupt postings: %v", err)
		}

		// OpenReader succeeds because the offset-table check only validates
		// that the stored list offset itself is in range; it does not read the
		// list body yet. The body-bounds check fires when the list is read.
		r, openErr := OpenReader(dir)
		if openErr != nil {
			// If the offset check caught it, that's also acceptable.
			return
		}
		defer r.Close()

		// Trigger a read of the corrupt list (allRefs sentinel, empty matchers).
		_, qErr := r.Postings(nil)
		if qErr == nil {
			t.Fatalf("Postings(nil) with huge count: want error, got nil (possible huge allocation risk); otOffset=%d", otOffset)
		}
	})
}
