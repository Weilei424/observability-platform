package logwal

import (
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
)

// Replay reads all WAL segments in dir in ascending numeric order, invoking fn
// for each successfully decoded log record. A partial trailing record on the last
// segment is silently discarded — the normal result of an unclean shutdown.
// Records in non-final segments must be complete; a corrupt body there returns an error.
func Replay(dir string, fn func(labels []LabelPair, tsNs int64, line string)) error {
	paths, err := segmentPaths(dir)
	if err != nil {
		return fmt.Errorf("logwal replay: list segments: %w", err)
	}
	for i, path := range paths {
		if err := replaySegment(path, i == len(paths)-1, fn); err != nil {
			return err
		}
	}
	return nil
}

func replaySegment(path string, isLast bool, fn func([]LabelPair, int64, string)) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("logwal replay: open %s: %w", path, err)
	}
	defer f.Close()

	for {
		var lenBuf [4]byte
		if _, err := io.ReadFull(f, lenBuf[:]); err != nil {
			if err == io.EOF {
				return nil
			}
			if err == io.ErrUnexpectedEOF && isLast {
				slog.Warn("logwal: partial length prefix discarded", "segment", path)
				return nil
			}
			return fmt.Errorf("logwal replay: read length in %s: %w", path, err)
		}

		bodyLen := binary.BigEndian.Uint32(lenBuf[:])
		if bodyLen > maxRecordBodyBytes {
			if isLast {
				slog.Warn("logwal: oversized declared length discarded", "segment", path, "declared", bodyLen)
				return nil
			}
			return fmt.Errorf("logwal replay: declared body length %d exceeds maximum in %s", bodyLen, path)
		}
		body := make([]byte, bodyLen)
		if _, err := io.ReadFull(f, body); err != nil {
			if err == io.ErrUnexpectedEOF && isLast {
				slog.Warn("logwal: partial record body discarded", "segment", path)
				return nil
			}
			return fmt.Errorf("logwal replay: read body in %s: %w", path, err)
		}

		labels, tsNs, line, ok := decodeRecord(body)
		if !ok {
			return fmt.Errorf("logwal replay: corrupt record in %s", path)
		}
		fn(labels, tsNs, line)
	}
}
