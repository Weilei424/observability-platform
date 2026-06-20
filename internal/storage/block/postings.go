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
	validIDs := make(map[uint64]struct{}, len(entries))
	for _, se := range entries {
		validIDs[se.ID] = struct{}{}
	}
	fp, err := newFilePostings(f, validIDs)
	if err != nil {
		f.Close()
		return nil, err
	}
	return fp, nil
}

func rebuildPostings(entries []SeriesEntry) blockPostings {
	mp := index.NewMemPostings()
	for _, se := range entries {
		pairs := make([]index.Pair, 0, len(se.Labels))
		for _, lp := range se.Labels {
			pairs = append(pairs, index.Pair{Name: lp.Name, Value: lp.Value})
		}
		mp.Add(se.ID, pairs)
	}
	return &memPostings{idx: mp}
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

func newFilePostings(f *os.File, validIDs map[uint64]struct{}) (*filePostings, error) {
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
			fp.allRefs = off
			continue
		}
		vals, ok := fp.offsets[name]
		if !ok {
			vals = make(map[string]int64)
			fp.offsets[name] = vals
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
	if err := fp.validateLists(validIDs); err != nil {
		return nil, err
	}
	return fp, nil
}

// validateLists eagerly validates every postings list (the allRefs sentinel plus
// each label-pair list) at open time, so corruption fails fast instead of being
// lazily skipped during a query and silently converted into missing/incorrect
// results. Lists are written contiguously between the header and the offset
// table, so each list body must end exactly where the next list begins. For
// each list it verifies:
//
//   - the declared body (4-byte count + cnt*8 IDs) exactly fills the gap to the
//     next list offset (or to the offset table for the final list); this rejects
//     both reduced counts and counts that bleed into the neighbouring list,
//   - the IDs are strictly ascending (the intersection logic in Select depends
//     on sorted lists), and
//   - every ID refers to a series present in the block index.
func (fp *filePostings) validateLists(validIDs map[uint64]struct{}) error {
	// Collect all list offsets and sort them to derive each list's exclusive end
	// bound from the next list's start.
	offs := make([]int64, 0, 1+len(fp.offsets))
	offs = append(offs, fp.allRefs)
	for _, vals := range fp.offsets {
		for _, off := range vals {
			offs = append(offs, off)
		}
	}
	sort.Slice(offs, func(i, j int) bool { return offs[i] < offs[j] })

	for i, off := range offs {
		end := fp.otOffset
		if i+1 < len(offs) {
			end = offs[i+1]
		}
		if err := fp.validateList(off, end, validIDs); err != nil {
			return err
		}
	}
	return nil
}

// validateList checks the single list starting at off whose body must end
// exactly at end (the next list offset, or the offset table for the last list).
func (fp *filePostings) validateList(off, end int64, validIDs map[uint64]struct{}) error {
	cntBuf := make([]byte, 4)
	if _, err := fp.f.ReadAt(cntBuf, off); err != nil {
		return fmt.Errorf("block: validate postings list count at offset %d: %w", off, err)
	}
	cnt := int64(binary.BigEndian.Uint32(cntBuf))
	if cnt < 0 || off+4+cnt*8 != end {
		return fmt.Errorf("block: postings list at offset %d claims %d entries but body does not end at next list boundary %d", off, cnt, end)
	}
	if cnt == 0 {
		return nil
	}
	idBuf := make([]byte, cnt*8)
	if _, err := fp.f.ReadAt(idBuf, off+4); err != nil {
		return fmt.Errorf("block: validate postings list ids at offset %d: %w", off, err)
	}
	var prev uint64
	for i := int64(0); i < cnt; i++ {
		id := binary.BigEndian.Uint64(idBuf[i*8:])
		if i > 0 && id <= prev {
			return fmt.Errorf("block: postings list at offset %d is not strictly ascending (%d after %d)", off, id, prev)
		}
		if _, ok := validIDs[id]; !ok {
			return fmt.Errorf("block: postings list at offset %d references unknown series ID %d", off, id)
		}
		prev = id
	}
	return nil
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
