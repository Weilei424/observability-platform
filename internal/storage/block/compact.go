package block

import (
	"fmt"
	"sort"

	"github.com/masonwheeler/observability-platform/internal/storage/chunk"
)

// Compact merges sources into one new block written under blocksDir (temp dir in
// tmpDir + atomic rename, identical to Writer.Commit), and returns the new
// block's Meta. Sources are not modified or deleted. For each series present in
// any source, all samples across all sources are merged, sorted by timestamp,
// deduplicated (later source wins on equal timestamps), and re-encoded into
// fresh chunks. The merged block's Level is one above the highest source level;
// Sources lists the source block IDs.
func Compact(blocksDir, tmpDir string, sources []*Reader) (Meta, error) {
	if len(sources) < 2 {
		return Meta{}, fmt.Errorf("block: Compact requires at least 2 sources, got %d", len(sources))
	}

	ordered := make([]*Reader, len(sources))
	copy(ordered, sources)
	sort.SliceStable(ordered, func(i, j int) bool {
		return ordered[i].Meta().MinTime < ordered[j].Meta().MinTime
	})

	type seriesAgg struct {
		labels  []LabelPair
		samples []sample
	}
	aggByID := make(map[uint64]*seriesAgg)
	var order []uint64 // first-seen order, for deterministic block output
	maxLevel := 0
	sourceIDs := make([]string, 0, len(ordered))

	for _, r := range ordered {
		m := r.Meta()
		sourceIDs = append(sourceIDs, m.BlockID)
		if lvl := m.EffectiveLevel(); lvl > maxLevel {
			maxLevel = lvl
		}
		for _, se := range r.Series() {
			agg, ok := aggByID[se.ID]
			if !ok {
				agg = &seriesAgg{labels: se.Labels}
				aggByID[se.ID] = agg
				order = append(order, se.ID)
			} else if !labelPairsEqual(agg.labels, se.Labels) {
				return Meta{}, fmt.Errorf("block: series %d has conflicting label sets across sources", se.ID)
			}
			for _, ref := range se.Chunks {
				c, err := r.ReadChunk(ref)
				if err != nil {
					return Meta{}, fmt.Errorf("block: compact read chunk for series %d: %w", se.ID, err)
				}
				it := c.Iterator()
				for it.Next() {
					ts, v := it.At()
					agg.samples = append(agg.samples, sample{ts: ts, val: v})
				}
				if err := it.Err(); err != nil {
					return Meta{}, fmt.Errorf("block: compact decode chunk for series %d: %w", se.ID, err)
				}
			}
		}
	}

	w, err := NewWriter(blocksDir, tmpDir)
	if err != nil {
		return Meta{}, err
	}
	w.SetCompaction(maxLevel+1, sourceIDs)

	for _, id := range order {
		chunks := rechunk(aggByID[id].samples)
		if len(chunks) == 0 {
			continue // series with no samples is dropped (carries no data)
		}
		if err := w.AddSeries(id, aggByID[id].labels, chunks); err != nil {
			_ = w.Abort()
			return Meta{}, fmt.Errorf("block: compact add series %d: %w", id, err)
		}
	}

	meta, err := w.Commit()
	if err != nil {
		_ = w.Abort()
		return Meta{}, fmt.Errorf("block: compact commit: %w", err)
	}
	return meta, nil
}

type sample struct {
	ts  int64
	val float64
}

// rechunk sorts samples by timestamp, deduplicates equal timestamps (last
// occurrence wins — later source wins because sources were appended MinTime
// ascending), and encodes them into chunks using the same 120-sample / 2h
// sealing rule as the head. Returns nil for no samples.
func rechunk(samples []sample) []*chunk.Chunk {
	if len(samples) == 0 {
		return nil
	}
	sort.SliceStable(samples, func(i, j int) bool { return samples[i].ts < samples[j].ts })
	deduped := samples[:1]
	for i := 1; i < len(samples); i++ {
		if samples[i].ts == deduped[len(deduped)-1].ts {
			deduped[len(deduped)-1] = samples[i]
		} else {
			deduped = append(deduped, samples[i])
		}
	}
	var chunks []*chunk.Chunk
	cur := chunk.NewChunk()
	chunks = append(chunks, cur)
	for _, s := range deduped {
		if cur.Sealed() {
			cur = chunk.NewChunk()
			chunks = append(chunks, cur)
		}
		// Append only fails on a sealed chunk, which the guard above prevents.
		_ = cur.Append(s.ts, s.val)
	}
	return chunks
}

func labelPairsEqual(a, b []LabelPair) bool {
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
