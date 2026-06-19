package block

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/masonwheeler/observability-platform/internal/storage/chunk"
	"github.com/masonwheeler/observability-platform/internal/storage/index"
)

// LabelPair is a name/value label stored in the block index.
type LabelPair struct {
	Name  string
	Value string
}

// ChunkRef is the location of a chunk payload within the chunks file.
type ChunkRef struct {
	Offset int64
	Length uint32
}

// SeriesEntry is one series record loaded from the block index.
type SeriesEntry struct {
	ID     uint64
	Labels []LabelPair
	Chunks []ChunkRef
}

// Reader reads a completed block directory.
// The chunks file is opened lazily on the first ReadChunk call.
type Reader struct {
	dir        string
	meta       Meta
	entries    []SeriesEntry
	postings   blockPostings
	mu         sync.Mutex
	chunksFile *os.File
	closed     bool
}

// OpenReader loads meta.json and the index from blockDir.
// Returns an error if meta.json is missing, unparseable, or if the
// block_id field does not match the directory name.
func OpenReader(blockDir string) (*Reader, error) {
	meta, err := readMeta(blockDir)
	if err != nil {
		return nil, err
	}
	if meta.BlockID != filepath.Base(blockDir) {
		return nil, fmt.Errorf("block: meta.json block_id %q does not match directory name %q",
			meta.BlockID, filepath.Base(blockDir))
	}
	entries, err := readIndex(blockDir)
	if err != nil {
		return nil, err
	}
	postings, err := loadPostings(blockDir, entries)
	if err != nil {
		return nil, err
	}
	return &Reader{dir: blockDir, meta: meta, entries: entries, postings: postings}, nil
}

// Meta returns the block metadata.
func (r *Reader) Meta() Meta { return r.meta }

// Series returns all series entries loaded from the index.
func (r *Reader) Series() []SeriesEntry { return r.entries }

// Postings returns the sorted series IDs in this block matching all matchers
// (AND). Empty matchers return every series in the block.
func (r *Reader) Postings(matchers []index.Pair) ([]uint64, error) {
	return r.postings.Select(matchers)
}

// LabelNames returns the sorted label names present in this block.
func (r *Reader) LabelNames() []string { return r.postings.LabelNames() }

// LabelValues returns the sorted values for name present in this block.
func (r *Reader) LabelValues(name string) []string { return r.postings.LabelValues(name) }

// ReadChunk reads and validates the chunk at ref. Opens the chunks file on
// first call; the file remains open until Close. The mutex is held for the
// full call so that Close cannot close the descriptor while ReadAt is in
// progress and no new open can happen after Close.
func (r *Reader) ReadChunk(ref ChunkRef) (*chunk.Chunk, error) {
	if ref.Length == 0 {
		return nil, fmt.Errorf("block: chunk ref at offset %d has zero length", ref.Offset)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, fmt.Errorf("block: reader is closed")
	}
	if r.chunksFile == nil {
		f, err := os.Open(filepath.Join(r.dir, "chunks"))
		if err != nil {
			return nil, fmt.Errorf("block: open chunks file: %w", err)
		}
		r.chunksFile = f
	}
	buf := make([]byte, ref.Length)
	if _, err := r.chunksFile.ReadAt(buf, ref.Offset); err != nil {
		return nil, fmt.Errorf("block: read chunk at offset %d: %w", ref.Offset, err)
	}
	return chunk.FromBytes(buf)
}

// Close marks the reader as closed and closes the chunks file if it was opened.
func (r *Reader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	var firstErr error
	if r.chunksFile != nil {
		if err := r.chunksFile.Close(); err != nil {
			firstErr = err
		}
	}
	if r.postings != nil {
		if err := r.postings.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// readIndex parses the binary index file and returns all series entries.
func readIndex(blockDir string) ([]SeriesEntry, error) {
	data, err := os.ReadFile(filepath.Join(blockDir, "index"))
	if err != nil {
		return nil, fmt.Errorf("block: read index: %w", err)
	}
	if len(data) < 4 {
		return nil, errors.New("block: index too short")
	}
	numEntries := int(binary.BigEndian.Uint32(data[:4]))
	if numEntries > 1_000_000 {
		return nil, fmt.Errorf("block: index declares %d entries, exceeds maximum 1000000", numEntries)
	}
	pos := 4
	entries := make([]SeriesEntry, 0, numEntries)
	for i := 0; i < numEntries; i++ {
		if pos+8 > len(data) {
			return nil, errors.New("block: index truncated at series ID")
		}
		id := binary.BigEndian.Uint64(data[pos:])
		pos += 8

		if pos+4 > len(data) {
			return nil, errors.New("block: index truncated at label set length")
		}
		labelLen := int(binary.BigEndian.Uint32(data[pos:]))
		pos += 4
		if pos+labelLen > len(data) {
			return nil, errors.New("block: index truncated at label set data")
		}
		labels, err := decodeLabelSet(data[pos : pos+labelLen])
		if err != nil {
			return nil, err
		}
		pos += labelLen

		if pos+4 > len(data) {
			return nil, errors.New("block: index truncated at chunk count")
		}
		numChunks := int(binary.BigEndian.Uint32(data[pos:]))
		if numChunks > 10_000 {
			return nil, fmt.Errorf("block: series %d declares %d chunks, exceeds maximum 10000", id, numChunks)
		}
		pos += 4

		refs := make([]ChunkRef, numChunks)
		for j := 0; j < numChunks; j++ {
			if pos+12 > len(data) {
				return nil, errors.New("block: index truncated at chunk ref")
			}
			refs[j] = ChunkRef{
				Offset: int64(binary.BigEndian.Uint64(data[pos:])),
				Length: binary.BigEndian.Uint32(data[pos+8:]),
			}
			pos += 12
		}
		entries = append(entries, SeriesEntry{ID: id, Labels: labels, Chunks: refs})
	}
	return entries, nil
}

func decodeLabelSet(data []byte) ([]LabelPair, error) {
	var pairs []LabelPair
	pos := 0
	for pos < len(data) {
		if pos+4 > len(data) {
			return nil, errors.New("block: label set truncated at name length")
		}
		nameLen := int(binary.BigEndian.Uint32(data[pos:]))
		pos += 4
		if pos+nameLen > len(data) {
			return nil, errors.New("block: label set truncated at name data")
		}
		name := string(data[pos : pos+nameLen])
		pos += nameLen

		if pos+4 > len(data) {
			return nil, errors.New("block: label set truncated at value length")
		}
		valLen := int(binary.BigEndian.Uint32(data[pos:]))
		pos += 4
		if pos+valLen > len(data) {
			return nil, errors.New("block: label set truncated at value data")
		}
		value := string(data[pos : pos+valLen])
		pos += valLen

		pairs = append(pairs, LabelPair{Name: name, Value: value})
	}
	return pairs, nil
}
