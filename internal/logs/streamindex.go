package logs

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/masonwheeler/observability-platform/internal/storage/fsutil"
	"github.com/masonwheeler/observability-platform/internal/storage/index"
)

var manifestMagic = [4]byte{0x9C, 'S', 'I', 0x01} // Stream Index v1

const manifestVersion byte = 1

// streamIndex is the in-memory log index: label pair -> stream IDs (via the shared
// MemPostings), stream ID -> chunk refs, and stream ID -> labels (for manifest
// rewrite and label discovery). It is not safe for concurrent use on its own; the
// owning Store serializes access under its mutex.
type streamIndex struct {
	postings *index.MemPostings
	refs     map[StreamID][]ChunkRef
	labels   map[StreamID]StreamLabels
}

func newStreamIndex() *streamIndex {
	return &streamIndex{
		postings: index.NewMemPostings(),
		refs:     make(map[StreamID][]ChunkRef),
		labels:   make(map[StreamID]StreamLabels),
	}
}

// add registers a chunk ref for a stream, indexing the stream's labels once.
func (x *streamIndex) add(id StreamID, labels StreamLabels, ref ChunkRef) {
	if _, ok := x.labels[id]; !ok {
		x.labels[id] = labels
		x.postings.Add(uint64(id), labelPairsOf(labels))
	}
	x.refs[id] = append(x.refs[id], ref)
}

// matchingStreamIDs returns the stream IDs whose labels match all matchers.
func (x *streamIndex) matchingStreamIDs(matchers []index.Pair) []StreamID {
	ids := x.postings.Select(matchers)
	out := make([]StreamID, len(ids))
	for i, id := range ids {
		out[i] = StreamID(id)
	}
	return out
}

// chunkRefs returns the stream's chunk refs overlapping [minTs, maxTs].
func (x *streamIndex) chunkRefs(id StreamID, minTs, maxTs int64) []ChunkRef {
	var out []ChunkRef
	for _, r := range x.refs[id] {
		if r.MaxTs >= minTs && r.MinTs <= maxTs {
			out = append(out, r)
		}
	}
	return out
}

func labelPairsOf(l StreamLabels) []index.Pair {
	m := l.Map()
	pairs := make([]index.Pair, 0, len(m))
	for k, v := range m {
		pairs = append(pairs, index.Pair{Name: k, Value: v})
	}
	return pairs
}

// writeManifest serializes the index as a flat list of chunk records and writes it
// durably (tmp -> fsync -> rename -> dir fsync). Record layout:
//
//	magic(4)|version(1)|count(4)| repeated {
//	  nameLen(2)|name | streamID(8) | minTs(8) | maxTs(8) |
//	  labelCount(1) | {nameLen(1)|name|valLen(2)|val}...
//	}
func (x *streamIndex) writeManifest(path string) error {
	// Deterministic order: streams by ID, refs in insertion order.
	ids := make([]StreamID, 0, len(x.refs))
	for id := range x.refs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	var count uint32
	for _, id := range ids {
		count += uint32(len(x.refs[id]))
	}

	buf := make([]byte, 0, 9)
	buf = append(buf, manifestMagic[:]...)
	buf = append(buf, manifestVersion)
	var u32 [4]byte
	binary.BigEndian.PutUint32(u32[:], count)
	buf = append(buf, u32[:]...)

	var u16 [2]byte
	var u64 [8]byte
	for _, id := range ids {
		labels := x.labels[id]
		lm := labels.Map()
		for _, ref := range x.refs[id] {
			binary.BigEndian.PutUint16(u16[:], uint16(len(ref.Name)))
			buf = append(buf, u16[:]...)
			buf = append(buf, ref.Name...)
			binary.BigEndian.PutUint64(u64[:], uint64(id))
			buf = append(buf, u64[:]...)
			binary.BigEndian.PutUint64(u64[:], uint64(ref.MinTs))
			buf = append(buf, u64[:]...)
			binary.BigEndian.PutUint64(u64[:], uint64(ref.MaxTs))
			buf = append(buf, u64[:]...)
			buf = append(buf, byte(len(lm)))
			for name, val := range lm {
				buf = append(buf, byte(len(name)))
				buf = append(buf, name...)
				binary.BigEndian.PutUint16(u16[:], uint16(len(val)))
				buf = append(buf, u16[:]...)
				buf = append(buf, val...)
			}
		}
	}

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, buf, 0o644); err != nil {
		return fmt.Errorf("logs: write manifest tmp: %w", err)
	}
	f, err := os.Open(tmp)
	if err != nil {
		return fmt.Errorf("logs: open manifest tmp: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("logs: fsync manifest tmp: %w", err)
	}
	f.Close()
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("logs: rename manifest: %w", err)
	}
	if err := fsutil.SyncDir(filepath.Dir(path)); err != nil {
		return fmt.Errorf("logs: fsync manifest dir: %w", err)
	}
	return nil
}

// loadManifest reconstructs a streamIndex from a manifest file. A missing file
// surfaces as an os.ErrNotExist-wrapped error; a malformed body returns a parse
// error (the caller rebuilds from a chunk scan).
func loadManifest(path string) (*streamIndex, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err // includes os.ErrNotExist for the missing-file case
	}
	if len(data) < 9 || data[0] != manifestMagic[0] || data[1] != manifestMagic[1] ||
		data[2] != manifestMagic[2] || data[3] != manifestMagic[3] {
		return nil, fmt.Errorf("logs: unrecognized manifest header")
	}
	if data[4] != manifestVersion {
		return nil, fmt.Errorf("logs: unsupported manifest version %d", data[4])
	}
	count := binary.BigEndian.Uint32(data[5:9])
	pos := 9
	x := newStreamIndex()
	for i := uint32(0); i < count; i++ {
		if pos+2 > len(data) {
			return nil, fmt.Errorf("logs: truncated manifest at record %d", i)
		}
		nl := int(binary.BigEndian.Uint16(data[pos : pos+2]))
		pos += 2
		if pos+nl+24+1 > len(data) {
			return nil, fmt.Errorf("logs: truncated manifest record %d", i)
		}
		name := string(data[pos : pos+nl])
		pos += nl
		id := StreamID(binary.BigEndian.Uint64(data[pos : pos+8]))
		pos += 8
		minTs := int64(binary.BigEndian.Uint64(data[pos : pos+8]))
		pos += 8
		maxTs := int64(binary.BigEndian.Uint64(data[pos : pos+8]))
		pos += 8
		labelCount := int(data[pos])
		pos++
		m := make(map[string]string, labelCount)
		for j := 0; j < labelCount; j++ {
			if pos+1 > len(data) {
				return nil, fmt.Errorf("logs: truncated manifest label at record %d", i)
			}
			knl := int(data[pos])
			pos++
			if pos+knl+2 > len(data) {
				return nil, fmt.Errorf("logs: truncated manifest label at record %d", i)
			}
			kname := string(data[pos : pos+knl])
			pos += knl
			kvl := int(binary.BigEndian.Uint16(data[pos : pos+2]))
			pos += 2
			if pos+kvl > len(data) {
				return nil, fmt.Errorf("logs: truncated manifest label value at record %d", i)
			}
			m[kname] = string(data[pos : pos+kvl])
			pos += kvl
		}
		labels, err := NewStreamLabels(m)
		if err != nil {
			return nil, fmt.Errorf("logs: invalid labels in manifest record %d: %w", i, err)
		}
		x.add(id, labels, ChunkRef{Name: name, MinTs: minTs, MaxTs: maxTs})
	}
	if pos != len(data) {
		return nil, fmt.Errorf("logs: %d trailing bytes in manifest", len(data)-pos)
	}
	return x, nil
}

// rebuildFromScan reconstructs the index by reading every chunk file's header.
// Chunks are the source of truth, so this recovers a missing or corrupt manifest.
func rebuildFromScan(chunksDir string) (*streamIndex, error) {
	entries, err := os.ReadDir(chunksDir)
	if err != nil {
		if os.IsNotExist(err) {
			return newStreamIndex(), nil
		}
		return nil, fmt.Errorf("logs: scan chunks dir: %w", err)
	}
	// Sort names so rebuilt ref order is deterministic.
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".chunk") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)

	x := newStreamIndex()
	for _, name := range names {
		id, labels, c, err := readChunkFile(filepath.Join(chunksDir, name))
		if err != nil {
			return nil, fmt.Errorf("logs: rebuild from %s: %w", name, err)
		}
		x.add(id, labels, ChunkRef{Name: name, MinTs: c.MinTs(), MaxTs: c.MaxTs()})
	}
	return x, nil
}
