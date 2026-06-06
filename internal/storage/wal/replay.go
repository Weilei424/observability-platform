package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Replay reads all WAL segments in dir in ascending numeric order, invoking fn
// for each successfully decoded sample record. A partial trailing record on the
// last segment is silently discarded — this is the normal result of an unclean
// shutdown. Records in non-final segments must be complete; a corrupt body
// there returns an error.
func Replay(dir string, fn func(labels []LabelPair, tsMs int64, value float64)) error {
	return ReplayFrom(dir, 0, fn)
}

// ReplayFrom replays only WAL segments with a numeric index strictly greater
// than afterSegment. Use afterSegment=0 to replay all segments.
func ReplayFrom(dir string, afterSegment int, fn func(labels []LabelPair, tsMs int64, value float64)) error {
	paths, err := segmentPaths(dir)
	if err != nil {
		return fmt.Errorf("wal replay: list segments: %w", err)
	}
	var filtered []string
	for _, p := range paths {
		base := strings.TrimSuffix(filepath.Base(p), ".wal")
		idx, e := strconv.Atoi(base)
		if e != nil {
			continue
		}
		if idx > afterSegment {
			filtered = append(filtered, p)
		}
	}
	for i, path := range filtered {
		if err := replaySegment(path, i == len(filtered)-1, fn); err != nil {
			return err
		}
	}
	return nil
}

func replaySegment(path string, isLast bool, fn func([]LabelPair, int64, float64)) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("wal replay: open %s: %w", path, err)
	}
	defer f.Close()

	for {
		var lenBuf [4]byte
		if _, err := io.ReadFull(f, lenBuf[:]); err != nil {
			if err == io.EOF {
				return nil
			}
			if err == io.ErrUnexpectedEOF {
				if isLast {
					slog.Warn("wal: partial length prefix discarded", "segment", path)
					return nil
				}
			}
			return fmt.Errorf("wal replay: read length in %s: %w", path, err)
		}

		bodyLen := binary.BigEndian.Uint32(lenBuf[:])
		// Guard against a corrupted length prefix that would cause a multi-GB
		// allocation. No valid record can exceed maxRecordBodyBytes; a larger
		// declared length on the final segment is treated as a partial trailing
		// record rather than allocated.
		if bodyLen > maxRecordBodyBytes {
			if isLast {
				slog.Warn("wal: oversized declared length discarded", "segment", path, "declared", bodyLen)
				return nil
			}
			return fmt.Errorf("wal replay: declared body length %d exceeds maximum in %s", bodyLen, path)
		}
		body := make([]byte, bodyLen)
		if _, err := io.ReadFull(f, body); err != nil {
			if err == io.ErrUnexpectedEOF && isLast {
				slog.Warn("wal: partial record body discarded", "segment", path)
				return nil
			}
			return fmt.Errorf("wal replay: read body in %s: %w", path, err)
		}

		labels, tsMs, value, ok := decodeRecord(body)
		if !ok {
			return fmt.Errorf("wal replay: corrupt record in %s", path)
		}
		fn(labels, tsMs, value)
	}
}
