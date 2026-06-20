// internal/storage/block/postings.go
package block

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/masonwheeler/observability-platform/internal/storage/index"
)

var postingsMagic = [4]byte{'O', 'P', 'P', 'I'}

const (
	postingsVersion  byte = 1
	postingsHeaderSz      = 5 // magic(4) + version(1)
	postingsFooterSz      = 8 // offsetTableOffset(8)
)

// blockPostings is one block's inverted index. Two implementations: filePostings
// (offset-table seek over the persisted file) and memPostings (rebuilt in
// memory when the file is absent).
type blockPostings interface {
	// Select returns the sorted series IDs matching all matchers (AND). Empty
	// matchers return the allRefs list (every series in the block).
	Select(matchers []index.Pair) ([]uint64, error)
	LabelNames() []string
	LabelValues(name string) []string
	Close() error
}

// --- writer ---

// writePostings serializes one postings list per distinct label pair across all
// series to dir/postings, preceded by an allRefs sentinel list, with an offset
// table and trailing footer. Lists are sorted ascending by series ID.
func writePostings(dir string, series []seriesData) error {
	mp := index.NewMemPostings()
	for _, sd := range series {
		pairs := make([]index.Pair, 0, len(sd.labels))
		for _, lp := range sd.labels {
			pairs = append(pairs, index.Pair{Name: lp.Name, Value: lp.Value})
		}
		mp.Add(sd.id, pairs)
	}

	// Entry order: allRefs sentinel ("","") first, then sorted (name,value).
	type entry struct{ name, value string }
	entries := []entry{{"", ""}}
	for _, name := range mp.LabelNames() {
		for _, val := range mp.LabelValues(name) {
			entries = append(entries, entry{name, val})
		}
	}

	var buf bytes.Buffer
	buf.Write(postingsMagic[:])
	buf.WriteByte(postingsVersion)

	var tmp4 [4]byte
	var tmp8 [8]byte
	offsets := make([]int64, len(entries))
	for i, e := range entries {
		offsets[i] = int64(buf.Len())
		ids := mp.Postings(e.name, e.value)
		binary.BigEndian.PutUint32(tmp4[:], uint32(len(ids)))
		buf.Write(tmp4[:])
		for _, id := range ids {
			binary.BigEndian.PutUint64(tmp8[:], id)
			buf.Write(tmp8[:])
		}
	}

	offsetTableOffset := int64(buf.Len())
	binary.BigEndian.PutUint32(tmp4[:], uint32(len(entries)))
	buf.Write(tmp4[:])
	for i, e := range entries {
		binary.BigEndian.PutUint32(tmp4[:], uint32(len(e.name)))
		buf.Write(tmp4[:])
		buf.WriteString(e.name)
		binary.BigEndian.PutUint32(tmp4[:], uint32(len(e.value)))
		buf.Write(tmp4[:])
		buf.WriteString(e.value)
		binary.BigEndian.PutUint64(tmp8[:], uint64(offsets[i]))
		buf.Write(tmp8[:])
	}

	binary.BigEndian.PutUint64(tmp8[:], uint64(offsetTableOffset))
	buf.Write(tmp8[:])

	path := filepath.Join(dir, "postings")
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		return fmt.Errorf("block: write postings: %w", err)
	}
	return syncPath(path)
}

// --- loader ---

// loadPostings opens dir/postings for offset-table seeking. If the file is
// absent it rebuilds an in-memory index from entries (forward index). A present
// but corrupt/truncated file returns an error.
func loadPostings(dir string, entries []SeriesEntry) (blockPostings, error) {
	path := filepath.Join(dir, "postings")
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return rebuildPostings(entries), nil
	}
	if err != nil {
		return nil, fmt.Errorf("block: open postings: %w", err)
	}
	fp, err := newFilePostings(f, buildMemPostings(entries))
	if err != nil {
		f.Close()
		return nil, err
	}
	return fp, nil
}

// buildMemPostings constructs the in-memory inverted index implied by the
// forward index. It is the authoritative expected form of the persisted
// postings and is used both as the rebuild fallback and to validate the
// persisted file at open.
func buildMemPostings(entries []SeriesEntry) *index.MemPostings {
	mp := index.NewMemPostings()
	for _, se := range entries {
		pairs := make([]index.Pair, 0, len(se.Labels))
		for _, lp := range se.Labels {
			pairs = append(pairs, index.Pair{Name: lp.Name, Value: lp.Value})
		}
		mp.Add(se.ID, pairs)
	}
	return mp
}

func rebuildPostings(entries []SeriesEntry) blockPostings {
	return &memPostings{idx: buildMemPostings(entries)}
}

// --- memPostings (fallback) ---

type memPostings struct{ idx *index.MemPostings }

func (m *memPostings) Select(matchers []index.Pair) ([]uint64, error) {
	return m.idx.Select(matchers), nil
}
func (m *memPostings) LabelNames() []string             { return m.idx.LabelNames() }
func (m *memPostings) LabelValues(name string) []string { return m.idx.LabelValues(name) }
func (m *memPostings) Close() error                     { return nil }

// --- filePostings (persisted, seek) ---

type filePostings struct {
	mu       sync.Mutex
	f        *os.File
	closed   bool
	offsets  map[string]map[string]int64 // name -> value -> list offset
	allRefs  int64                       // offset of the "","" sentinel list
	otOffset int64                       // byte offset of the offset table (used for list-body bounds check)
	names    []string                    // sorted, excluding the sentinel
	values   map[string][]string         // name -> sorted values, excluding sentinel
}

func newFilePostings(f *os.File, expected *index.MemPostings) (*filePostings, error) {
	st, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("block: stat postings: %w", err)
	}
	size := st.Size()
	if size < int64(postingsHeaderSz+postingsFooterSz) {
		return nil, fmt.Errorf("block: postings too short (%d bytes)", size)
	}

	hdr := make([]byte, postingsHeaderSz)
	if _, err := f.ReadAt(hdr, 0); err != nil {
		return nil, fmt.Errorf("block: read postings header: %w", err)
	}
	if !bytes.Equal(hdr[:4], postingsMagic[:]) {
		return nil, fmt.Errorf("block: postings bad magic")
	}
	if hdr[4] != postingsVersion {
		return nil, fmt.Errorf("block: postings unsupported version %d", hdr[4])
	}

	footer := make([]byte, postingsFooterSz)
	if _, err := f.ReadAt(footer, size-int64(postingsFooterSz)); err != nil {
		return nil, fmt.Errorf("block: read postings footer: %w", err)
	}
	otOffset := int64(binary.BigEndian.Uint64(footer))
	if otOffset < int64(postingsHeaderSz) || otOffset > size-int64(postingsFooterSz) {
		return nil, fmt.Errorf("block: postings offset-table offset %d out of range", otOffset)
	}

	otLen := size - int64(postingsFooterSz) - otOffset
	ot := make([]byte, otLen)
	if _, err := f.ReadAt(ot, otOffset); err != nil {
		return nil, fmt.Errorf("block: read postings offset table: %w", err)
	}

	fp := &filePostings{
		f:        f,
		offsets:  make(map[string]map[string]int64),
		allRefs:  -1,
		otOffset: otOffset,
		values:   make(map[string][]string),
	}
	pos := 0
	if pos+4 > len(ot) {
		return nil, fmt.Errorf("block: postings offset table truncated at count")
	}
	numEntries := int(binary.BigEndian.Uint32(ot[pos:]))
	pos += 4
	nameSet := make(map[string]struct{})
	readStr := func() (string, error) {
		if pos+4 > len(ot) {
			return "", fmt.Errorf("block: postings offset table truncated at string length")
		}
		n := int(binary.BigEndian.Uint32(ot[pos:]))
		pos += 4
		if pos+n > len(ot) {
			return "", fmt.Errorf("block: postings offset table truncated at string data")
		}
		s := string(ot[pos : pos+n])
		pos += n
		return s, nil
	}
	for i := 0; i < numEntries; i++ {
		name, err := readStr()
		if err != nil {
			return nil, err
		}
		val, err := readStr()
		if err != nil {
			return nil, err
		}
		if pos+8 > len(ot) {
			return nil, fmt.Errorf("block: postings offset table truncated at offset")
		}
		off := int64(binary.BigEndian.Uint64(ot[pos:]))
		pos += 8
		if off < int64(postingsHeaderSz) || off+4 > otOffset {
			return nil, fmt.Errorf("block: postings list offset %d out of range", off)
		}
		if name == "" && val == "" {
			if fp.allRefs >= 0 {
				return nil, fmt.Errorf("block: postings offset table has duplicate allRefs sentinel")
			}
			fp.allRefs = off
			continue
		}
		vals, ok := fp.offsets[name]
		if !ok {
			vals = make(map[string]int64)
			fp.offsets[name] = vals
		}
		if _, dup := vals[val]; dup {
			return nil, fmt.Errorf("block: postings offset table has duplicate entry %q=%q", name, val)
		}
		vals[val] = off
		nameSet[name] = struct{}{}
		fp.values[name] = append(fp.values[name], val)
	}
	if fp.allRefs < 0 {
		return nil, fmt.Errorf("block: postings missing allRefs sentinel")
	}
	for name := range nameSet {
		fp.names = append(fp.names, name)
	}
	sort.Strings(fp.names)
	for _, vs := range fp.values {
		sort.Strings(vs)
	}
	if err := fp.validate(expected); err != nil {
		return nil, err
	}
	return fp, nil
}

// validate verifies the persisted postings exactly reproduce the inverted index
// implied by the forward index (expected), so corruption fails fast at open
// instead of being lazily skipped during a query and silently converted into
// missing or incorrect results. Lists are written contiguously between the
// header and the offset table, so each body must end exactly where the next list
// begins. validate checks:
//
//   - the file lists the same set of label pairs as the forward index (no
//     missing or extra pairs),
//   - each list body (4-byte count + cnt*8 IDs) exactly fills the gap to the
//     next list offset, rejecting reduced or overlapping counts,
//   - no list claims more entries than the block has series (bounding the read
//     allocation), and
//   - each list's IDs equal the forward index's postings for that exact pair —
//     which subsumes ordering, ID existence, and correct label-pair association.
func (fp *filePostings) validate(expected *index.MemPostings) error {
	total := expected.SeriesCount()

	// Pair each list offset with its (name, value); allRefs is the "","" sentinel.
	type listRef struct {
		off         int64
		name, value string
	}
	refs := []listRef{{fp.allRefs, "", ""}}
	for name, vals := range fp.offsets {
		for value, off := range vals {
			refs = append(refs, listRef{off, name, value})
		}
	}
	// Prove the file's (name,value) key set is exactly the forward index's before
	// validating list contents. A count comparison alone is insufficient: a
	// missing expected pair masked by an unexpected (e.g. empty) pair can keep the
	// counts equal, so compare the key sets directly in both directions.
	if err := validatePairKeySet(expected, fp.offsets); err != nil {
		return err
	}

	sort.Slice(refs, func(i, j int) bool { return refs[i].off < refs[j].off })
	for i, ref := range refs {
		end := fp.otOffset
		if i+1 < len(refs) {
			end = refs[i+1].off
		}
		ids, err := fp.readListBounded(ref.off, end, total)
		if err != nil {
			return err
		}
		if !equalU64(ids, expected.Postings(ref.name, ref.value)) {
			return fmt.Errorf("block: postings list for %q=%q does not match forward index", ref.name, ref.value)
		}
	}
	return nil
}

// validatePairKeySet checks that the (name,value) keys present in the persisted
// offset table (offsets) are exactly those of the forward index (expected):
// every expected pair is present, and no extra pair exists. The allRefs sentinel
// is not part of either set.
func validatePairKeySet(expected *index.MemPostings, offsets map[string]map[string]int64) error {
	expectedPairs := 0
	for _, name := range expected.LabelNames() {
		for _, value := range expected.LabelValues(name) {
			expectedPairs++
			vals, ok := offsets[name]
			if !ok {
				return fmt.Errorf("block: postings missing label pair %q=%q", name, value)
			}
			if _, ok := vals[value]; !ok {
				return fmt.Errorf("block: postings missing label pair %q=%q", name, value)
			}
		}
	}
	filePairs := 0
	for _, vals := range offsets {
		filePairs += len(vals)
	}
	if filePairs != expectedPairs {
		return fmt.Errorf("block: postings has %d label pairs, forward index has %d", filePairs, expectedPairs)
	}
	return nil
}

// readListBounded reads the list at off whose body must end exactly at end. cnt
// is rejected if it exceeds maxCount (the block's series count), bounding the
// allocation against a corrupt count.
func (fp *filePostings) readListBounded(off, end int64, maxCount int) ([]uint64, error) {
	cntBuf := make([]byte, 4)
	if _, err := fp.f.ReadAt(cntBuf, off); err != nil {
		return nil, fmt.Errorf("block: validate postings list count at offset %d: %w", off, err)
	}
	cnt := int64(binary.BigEndian.Uint32(cntBuf))
	if off+4+cnt*8 != end {
		return nil, fmt.Errorf("block: postings list at offset %d claims %d entries but body does not end at next list boundary %d", off, cnt, end)
	}
	if cnt > int64(maxCount) {
		return nil, fmt.Errorf("block: postings list at offset %d claims %d entries, exceeds block series count %d", off, cnt, maxCount)
	}
	if cnt == 0 {
		return nil, nil
	}
	idBuf := make([]byte, cnt*8)
	if _, err := fp.f.ReadAt(idBuf, off+4); err != nil {
		return nil, fmt.Errorf("block: validate postings list ids at offset %d: %w", off, err)
	}
	ids := make([]uint64, cnt)
	for i := int64(0); i < cnt; i++ {
		ids[i] = binary.BigEndian.Uint64(idBuf[i*8:])
	}
	return ids, nil
}

// equalU64 reports whether two uint64 slices have identical contents in order.
func equalU64(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// readList reads the postings list whose count+ids start at off.
func (fp *filePostings) readList(off int64) ([]uint64, error) {
	cntBuf := make([]byte, 4)
	if _, err := fp.f.ReadAt(cntBuf, off); err != nil {
		return nil, fmt.Errorf("block: read postings list count: %w", err)
	}
	cnt := int(binary.BigEndian.Uint32(cntBuf))
	if cnt == 0 {
		return nil, nil
	}
	// Guard against a corrupt count that would cause a huge allocation: the
	// entire list body (4-byte count header + cnt*8 bytes of IDs) must fit
	// before the offset table.
	if off+4+int64(cnt)*8 > fp.otOffset {
		return nil, fmt.Errorf("block: postings list at offset %d claims %d entries but body exceeds data region", off, cnt)
	}
	idBuf := make([]byte, cnt*8)
	if _, err := fp.f.ReadAt(idBuf, off+4); err != nil {
		return nil, fmt.Errorf("block: read postings list ids: %w", err)
	}
	ids := make([]uint64, cnt)
	for i := 0; i < cnt; i++ {
		ids[i] = binary.BigEndian.Uint64(idBuf[i*8:])
	}
	return ids, nil
}

func (fp *filePostings) Select(matchers []index.Pair) ([]uint64, error) {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	if fp.closed {
		return nil, fmt.Errorf("block: postings reader is closed")
	}
	if len(matchers) == 0 {
		return fp.readList(fp.allRefs)
	}

	lists := make([][]uint64, 0, len(matchers))
	for _, m := range matchers {
		vals, ok := fp.offsets[m.Name]
		if !ok {
			return nil, nil
		}
		off, ok := vals[m.Value]
		if !ok {
			return nil, nil
		}
		list, err := fp.readList(off)
		if err != nil {
			return nil, err
		}
		if len(list) == 0 {
			return nil, nil
		}
		lists = append(lists, list)
	}
	sort.Slice(lists, func(i, j int) bool { return len(lists[i]) < len(lists[j]) })
	result := append([]uint64(nil), lists[0]...)
	for _, list := range lists[1:] {
		result = intersectSortedU64(result, list)
		if len(result) == 0 {
			return nil, nil
		}
	}
	return result, nil
}

func (fp *filePostings) LabelNames() []string { return fp.names }

func (fp *filePostings) LabelValues(name string) []string { return fp.values[name] }

func (fp *filePostings) Close() error {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	if fp.closed {
		return nil
	}
	fp.closed = true
	return fp.f.Close()
}

// intersectSortedU64 returns the sorted intersection of two ascending slices.
func intersectSortedU64(a, b []uint64) []uint64 {
	var out []uint64
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] == b[j]:
			out = append(out, a[i])
			i++
			j++
		case a[i] < b[j]:
			i++
		default:
			j++
		}
	}
	return out
}
