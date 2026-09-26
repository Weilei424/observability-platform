package logs

import (
	"context"

	"github.com/masonwheeler/observability-platform/internal/storage/index"
)

// Source is the read surface the logs query engine needs: one call per selector
// per query, coarse enough to cross a process boundary. internal/rpc implements
// it over HTTP and Merge combines two; a local Reader is adapted by
// readerSource.
//
// SelectStreams returns every stream whose labels match all matchers and that
// holds at least one entry with minTs <= ts <= maxTs. Streams ascend by
// StreamID. Each stream's entries ascend by timestamp, are deduplicated by
// (ts, line), and at an equal timestamp keep persisted entries ahead of head
// entries — the order Store.StreamEntries produces, which the engine's
// tie-breaking depends on. Slices in a result belong to the caller.
type Source interface {
	SelectStreams(ctx context.Context, matchers []index.Pair, minTs, maxTs int64) ([]StreamData, error)
	SelectLabelNames(ctx context.Context) ([]string, error)
	SelectLabelValues(ctx context.Context, name string) ([]string, error)
}

// StreamData is one stream's answer to SelectStreams.
type StreamData struct {
	Labels  StreamLabels
	Entries []LogEntry
}

// AsSource returns r as a Source: r itself when it already is one, otherwise
// an exact adapter over its per-stream reads. The ingester and the store serve
// their Head and ChunkStore through it.
func AsSource(r Reader) Source {
	if src, ok := r.(Source); ok {
		return src
	}
	return readerSource{r: r}
}

// readerSource adapts a Reader to Source. For a *Store it is exact: it is the
// same per-stream reads the engine made before bulk reads.
type readerSource struct{ r Reader }

func (s readerSource) SelectStreams(ctx context.Context, matchers []index.Pair, minTs, maxTs int64) ([]StreamData, error) {
	ids := s.r.MatchingStreamIDs(matchers)
	out := make([]StreamData, 0, len(ids))
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		labels, ok := s.r.StreamLabelSet(id)
		if !ok {
			continue
		}
		entries, err := s.r.StreamEntries(ctx, id, minTs, maxTs)
		if err != nil {
			return nil, err
		}
		if len(entries) == 0 {
			continue
		}
		out = append(out, StreamData{Labels: labels, Entries: entries})
	}
	return out, nil
}

func (s readerSource) SelectLabelNames(context.Context) ([]string, error) {
	return s.r.LabelNames(), nil
}

func (s readerSource) SelectLabelValues(_ context.Context, name string) ([]string, error) {
	return s.r.LabelValues(name), nil
}
