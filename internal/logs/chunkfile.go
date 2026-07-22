package logs

import (
	"bufio"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/masonwheeler/observability-platform/internal/storage/fsutil"
	"github.com/masonwheeler/observability-platform/internal/storage/logchunk"
)

// chunkFileMagic prefixes a persisted log chunk file: 0x9C sentinel, "LF" (log
// file), version 0x01.
var chunkFileMagic = [4]byte{0x9C, 'L', 'F', 0x01}

const chunkFileVersion byte = 1

// ChunkRef locates a persisted chunk file within the chunks directory, with the
// time bounds needed to filter it during a query without reading the file.
type ChunkRef struct {
	Name  string
	MinTs int64
	MaxTs int64
}

// readChunkFileHeader reads a chunk file's stream identity, labels, and timestamp
// bounds WITHOUT decompressing the payload — the cheap path rebuildFromScan uses to
// reconstruct the index from chunk headers, so recovery scales with the number of
// chunks rather than the whole log corpus. It reads only the file header plus the
// logchunk fixed header, streaming just those bytes.
func readChunkFileHeader(path string) (StreamID, StreamLabels, int64, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, StreamLabels{}, 0, 0, fmt.Errorf("logs: open chunk file: %w", err)
	}
	defer f.Close()
	r := bufio.NewReader(f)

	fixed := make([]byte, 14) // magic(4)|version(1)|streamID(8)|labelCount(1)
	if _, err := io.ReadFull(r, fixed); err != nil {
		return 0, StreamLabels{}, 0, 0, fmt.Errorf("logs: read chunk file header: %w", err)
	}
	if fixed[0] != chunkFileMagic[0] || fixed[1] != chunkFileMagic[1] ||
		fixed[2] != chunkFileMagic[2] || fixed[3] != chunkFileMagic[3] {
		return 0, StreamLabels{}, 0, 0, fmt.Errorf("logs: unrecognized chunk file header")
	}
	if fixed[4] != chunkFileVersion {
		return 0, StreamLabels{}, 0, 0, fmt.Errorf("logs: unsupported chunk file version %d", fixed[4])
	}
	id := StreamID(binary.BigEndian.Uint64(fixed[5:13]))
	labelCount := int(fixed[13])

	m := make(map[string]string, labelCount)
	var vlen [2]byte
	for i := 0; i < labelCount; i++ {
		nl, err := r.ReadByte()
		if err != nil {
			return 0, StreamLabels{}, 0, 0, fmt.Errorf("logs: read chunk file label name len: %w", err)
		}
		name := make([]byte, int(nl))
		if _, err := io.ReadFull(r, name); err != nil {
			return 0, StreamLabels{}, 0, 0, fmt.Errorf("logs: read chunk file label name: %w", err)
		}
		if _, err := io.ReadFull(r, vlen[:]); err != nil {
			return 0, StreamLabels{}, 0, 0, fmt.Errorf("logs: read chunk file label value len: %w", err)
		}
		val := make([]byte, int(binary.BigEndian.Uint16(vlen[:])))
		if _, err := io.ReadFull(r, val); err != nil {
			return 0, StreamLabels{}, 0, 0, fmt.Errorf("logs: read chunk file label value: %w", err)
		}
		m[string(name)] = string(val)
	}
	labels, err := NewStreamLabels(m)
	if err != nil {
		return 0, StreamLabels{}, 0, 0, fmt.Errorf("logs: invalid labels in chunk file: %w", err)
	}

	lch := make([]byte, logchunk.HeaderLen)
	if _, err := io.ReadFull(r, lch); err != nil {
		return 0, StreamLabels{}, 0, 0, fmt.Errorf("logs: read chunk header in %s: %w", path, err)
	}
	minTs, maxTs, _, err := logchunk.PeekBounds(lch)
	if err != nil {
		return 0, StreamLabels{}, 0, 0, fmt.Errorf("logs: peek chunk bounds in %s: %w", path, err)
	}
	return id, labels, minTs, maxTs, nil
}

// encodeChunkFileHeader serializes stream identity + labels:
// magic(4)|version(1)|streamID(8)|labelCount(1)|{nameLen(1)|name|valLen(2)|val}...
func encodeChunkFileHeader(id StreamID, labels StreamLabels) []byte {
	m := labels.Map()
	buf := make([]byte, 0, 14)
	buf = append(buf, chunkFileMagic[:]...)
	buf = append(buf, chunkFileVersion)
	var u64 [8]byte
	binary.BigEndian.PutUint64(u64[:], uint64(id))
	buf = append(buf, u64[:]...)
	buf = append(buf, byte(len(m)))
	for name, val := range m {
		buf = append(buf, byte(len(name)))
		buf = append(buf, name...)
		var u16 [2]byte
		binary.BigEndian.PutUint16(u16[:], uint16(len(val)))
		buf = append(buf, u16[:]...)
		buf = append(buf, val...)
	}
	return buf
}

func chunkFileName(id StreamID, minTs int64) (string, error) {
	var r [4]byte
	if _, err := rand.Read(r[:]); err != nil {
		return "", fmt.Errorf("logs: chunk name randomness: %w", err)
	}
	return fmt.Sprintf("%016x-%020d-%s.chunk", uint64(id), minTs, hex.EncodeToString(r[:])), nil
}

// writeChunkFile durably writes a chunk: tmp file -> fsync -> atomic rename ->
// fsync dir. Returns a ChunkRef for the index.
func writeChunkFile(dir string, id StreamID, labels StreamLabels, c *logchunk.Chunk) (ChunkRef, error) {
	name, err := chunkFileName(id, c.MinTs())
	if err != nil {
		return ChunkRef{}, err
	}
	payload := append(encodeChunkFileHeader(id, labels), c.Bytes()...)

	tmp := filepath.Join(dir, name+".tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return ChunkRef{}, fmt.Errorf("logs: create chunk tmp: %w", err)
	}
	if _, err := f.Write(payload); err != nil {
		f.Close()
		return ChunkRef{}, fmt.Errorf("logs: write chunk: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return ChunkRef{}, fmt.Errorf("logs: fsync chunk: %w", err)
	}
	if err := f.Close(); err != nil {
		return ChunkRef{}, fmt.Errorf("logs: close chunk: %w", err)
	}
	final := filepath.Join(dir, name)
	if err := os.Rename(tmp, final); err != nil {
		return ChunkRef{}, fmt.Errorf("logs: rename chunk: %w", err)
	}
	if err := fsutil.SyncDir(dir); err != nil {
		return ChunkRef{}, fmt.Errorf("logs: fsync chunks dir: %w", err)
	}
	return ChunkRef{Name: name, MinTs: c.MinTs(), MaxTs: c.MaxTs()}, nil
}

// readChunkFile parses a chunk file back into stream identity, labels, and chunk.
func readChunkFile(path string) (StreamID, StreamLabels, *logchunk.Chunk, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, StreamLabels{}, nil, fmt.Errorf("logs: read chunk file: %w", err)
	}
	id, labels, rest, err := decodeChunkFileHeader(data)
	if err != nil {
		return 0, StreamLabels{}, nil, err
	}
	c, err := logchunk.FromBytes(rest)
	if err != nil {
		return 0, StreamLabels{}, nil, fmt.Errorf("logs: decode chunk in %s: %w", path, err)
	}
	return id, labels, c, nil
}

// decodeChunkFileHeader parses the header and returns the remaining chunk bytes.
func decodeChunkFileHeader(data []byte) (StreamID, StreamLabels, []byte, error) {
	if len(data) < 14 || data[0] != chunkFileMagic[0] || data[1] != chunkFileMagic[1] ||
		data[2] != chunkFileMagic[2] || data[3] != chunkFileMagic[3] {
		return 0, StreamLabels{}, nil, fmt.Errorf("logs: unrecognized chunk file header")
	}
	if data[4] != chunkFileVersion {
		return 0, StreamLabels{}, nil, fmt.Errorf("logs: unsupported chunk file version %d", data[4])
	}
	id := StreamID(binary.BigEndian.Uint64(data[5:13]))
	labelCount := int(data[13])
	pos := 14
	m := make(map[string]string, labelCount)
	for i := 0; i < labelCount; i++ {
		if pos+1 > len(data) {
			return 0, StreamLabels{}, nil, fmt.Errorf("logs: truncated chunk file label name len")
		}
		nl := int(data[pos])
		pos++
		if pos+nl > len(data) {
			return 0, StreamLabels{}, nil, fmt.Errorf("logs: truncated chunk file label name")
		}
		name := string(data[pos : pos+nl])
		pos += nl
		if pos+2 > len(data) {
			return 0, StreamLabels{}, nil, fmt.Errorf("logs: truncated chunk file label value len")
		}
		vl := int(binary.BigEndian.Uint16(data[pos : pos+2]))
		pos += 2
		if pos+vl > len(data) {
			return 0, StreamLabels{}, nil, fmt.Errorf("logs: truncated chunk file label value")
		}
		m[name] = string(data[pos : pos+vl])
		pos += vl
	}
	labels, err := NewStreamLabels(m)
	if err != nil {
		return 0, StreamLabels{}, nil, fmt.Errorf("logs: invalid labels in chunk file: %w", err)
	}
	return id, labels, data[pos:], nil
}
