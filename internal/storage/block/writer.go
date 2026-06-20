package block

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/masonwheeler/observability-platform/internal/storage/chunk"
)

type seriesData struct {
	id     uint64
	labels []LabelPair
	chunks []*chunk.Chunk
}

// Writer accumulates series and writes them atomically to a new block directory.
// Call AddSeries for each series, then Commit to finalize. Call Abort to clean
// up if Commit is never called.
type Writer struct {
	blocksDir  string
	blockID    string
	workDir    string // temp directory; cleared after Commit
	series     []seriesData
	seriesIDs  map[uint64]struct{} // guards against duplicate series IDs
	minTime    int64
	maxTime    int64
	numSamples int
}

// NewWriter creates a Writer that writes to a temp directory and renames to
// blocksDir/<block-id> on Commit.
func NewWriter(blocksDir, tmpDir string) (*Writer, error) {
	id, err := generateBlockID()
	if err != nil {
		return nil, err
	}
	workDir := filepath.Join(tmpDir, id)
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return nil, fmt.Errorf("block: mkdir temp dir %s: %w", workDir, err)
	}
	return &Writer{
		blocksDir: blocksDir,
		blockID:   id,
		workDir:   workDir,
		seriesIDs: make(map[uint64]struct{}),
		minTime:   math.MaxInt64,
		maxTime:   math.MinInt64,
	}, nil
}

// AddSeries enqueues one series with its sealed chunks for writing.
// chunks must all be sealed (Bytes() must return a valid payload).
func (w *Writer) AddSeries(id uint64, labels []LabelPair, chunks []*chunk.Chunk) error {
	if _, dup := w.seriesIDs[id]; dup {
		return fmt.Errorf("block: duplicate series ID %d", id)
	}
	w.seriesIDs[id] = struct{}{}
	for _, c := range chunks {
		if c.NumSamples() == 0 {
			continue
		}
		w.numSamples += c.NumSamples()
		if c.MinTs() < w.minTime {
			w.minTime = c.MinTs()
		}
		if c.MaxTs() > w.maxTime {
			w.maxTime = c.MaxTs()
		}
	}
	w.series = append(w.series, seriesData{id: id, labels: labels, chunks: chunks})
	return nil
}

// Commit writes chunks, index, and meta.json to the temp directory, then
// renames it atomically to blocksDir/<block-id>. Returns the populated Meta.
func (w *Writer) Commit() (Meta, error) {
	if w.workDir == "" {
		return Meta{}, fmt.Errorf("block: writer already committed or aborted")
	}
	if err := w.writeChunksAndIndex(); err != nil {
		return Meta{}, err
	}
	if w.numSamples == 0 {
		w.minTime = 0
		w.maxTime = 0
	}
	meta := Meta{
		BlockID:    w.blockID,
		MinTime:    w.minTime,
		MaxTime:    w.maxTime,
		NumSeries:  len(w.series),
		NumSamples: w.numSamples,
		CreatedAt:  time.Now().UTC(),
	}
	if err := writeMeta(w.workDir, meta); err != nil {
		return Meta{}, err
	}
	if err := syncPath(filepath.Join(w.workDir, "meta.json")); err != nil {
		return Meta{}, err
	}
	// Flush the temp directory's entries so all file data is durable before rename.
	if err := syncPath(w.workDir); err != nil {
		return Meta{}, err
	}
	dest := filepath.Join(w.blocksDir, w.blockID)
	if err := os.MkdirAll(w.blocksDir, 0o755); err != nil {
		return Meta{}, fmt.Errorf("block: mkdir blocks dir: %w", err)
	}
	if err := os.Rename(w.workDir, dest); err != nil {
		return Meta{}, fmt.Errorf("block: rename block to final location: %w", err)
	}
	// Flush the parent directory so the rename is durable.
	if err := syncPath(w.blocksDir); err != nil {
		return Meta{}, err
	}
	w.workDir = ""
	return meta, nil
}

// Abort removes the temp directory. Safe to call even if Commit already succeeded.
func (w *Writer) Abort() error {
	if w.workDir == "" {
		return nil
	}
	err := os.RemoveAll(w.workDir)
	w.workDir = ""
	return err
}

func (w *Writer) writeChunksAndIndex() error {
	cf, err := os.Create(filepath.Join(w.workDir, "chunks"))
	if err != nil {
		return fmt.Errorf("block: create chunks file: %w", err)
	}
	defer cf.Close()

	type chunkLoc struct {
		offset int64
		length uint32
	}
	allLocs := make([][]chunkLoc, len(w.series))

	var fileOffset int64
	for i, sd := range w.series {
		locs := make([]chunkLoc, 0, len(sd.chunks))
		for _, c := range sd.chunks {
			payload := c.Bytes()
			var hdr [12]byte
			binary.BigEndian.PutUint64(hdr[:8], sd.id)
			binary.BigEndian.PutUint32(hdr[8:12], uint32(len(payload)))
			if _, err := cf.Write(hdr[:]); err != nil {
				return fmt.Errorf("block: write chunk header: %w", err)
			}
			payloadOffset := fileOffset + 12
			if _, err := cf.Write(payload); err != nil {
				return fmt.Errorf("block: write chunk payload: %w", err)
			}
			locs = append(locs, chunkLoc{offset: payloadOffset, length: uint32(len(payload))})
			fileOffset += int64(12 + len(payload))
		}
		allLocs[i] = locs
	}

	// Build index in memory then write atomically.
	var idx bytes.Buffer
	var tmp8 [8]byte
	var tmp4 [4]byte

	binary.BigEndian.PutUint32(tmp4[:], uint32(len(w.series)))
	idx.Write(tmp4[:])

	for i, sd := range w.series {
		binary.BigEndian.PutUint64(tmp8[:], sd.id)
		idx.Write(tmp8[:])

		labelData := encodeLabelSet(sd.labels)
		binary.BigEndian.PutUint32(tmp4[:], uint32(len(labelData)))
		idx.Write(tmp4[:])
		idx.Write(labelData)

		locs := allLocs[i]
		binary.BigEndian.PutUint32(tmp4[:], uint32(len(locs)))
		idx.Write(tmp4[:])
		for _, loc := range locs {
			binary.BigEndian.PutUint64(tmp8[:], uint64(loc.offset))
			idx.Write(tmp8[:])
			binary.BigEndian.PutUint32(tmp4[:], loc.length)
			idx.Write(tmp4[:])
		}
	}

	// Sync the chunks file to disk before closing so all payload bytes are
	// durable before the WAL checkpoint advances past the segments they cover.
	if err := cf.Sync(); err != nil {
		return fmt.Errorf("block: sync chunks file: %w", err)
	}

	indexPath := filepath.Join(w.workDir, "index")
	if err := os.WriteFile(indexPath, idx.Bytes(), 0o644); err != nil {
		return fmt.Errorf("block: write index: %w", err)
	}
	if err := syncPath(indexPath); err != nil {
		return err
	}
	return writePostings(w.workDir, w.series)
}

func encodeLabelSet(pairs []LabelPair) []byte {
	var buf bytes.Buffer
	var tmp [4]byte
	for _, p := range pairs {
		binary.BigEndian.PutUint32(tmp[:], uint32(len(p.Name)))
		buf.Write(tmp[:])
		buf.WriteString(p.Name)
		binary.BigEndian.PutUint32(tmp[:], uint32(len(p.Value)))
		buf.Write(tmp[:])
		buf.WriteString(p.Value)
	}
	return buf.Bytes()
}

// syncPath opens path and calls Sync, making its contents durable on Linux for
// both regular files and directories. Used to guarantee block data survives a
// crash before the WAL checkpoint is advanced.
func syncPath(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("block: open for fsync %q: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("block: fsync %q: %w", path, err)
	}
	return f.Close()
}

func generateBlockID() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("block: generate block ID: %w", err)
	}
	return hex.EncodeToString(b), nil
}
