package metrics

import (
	"context"
	"fmt"
	"math/rand/v2"
	"reflect"
	"testing"
)

// Every native Select must return exactly what the per-series reference
// returns for the same store — same series, same order, same samples, same
// anchors — because all-in-one's responses must not change.
func TestNativeSelectMatchesTheReference(t *testing.T) {
	for seed := uint64(1); seed <= 4; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			bs := oracleFixture(t, seed)
			r := rand.New(rand.NewPCG(seed, 11))
			sels := []Selector{
				{MetricName: "oracle_metric"},
				{MetricName: "oracle_metric", Matchers: []Matcher{{Name: "job", Value: "b"}}},
				{Matchers: []Matcher{{Name: "job", Value: "a"}}},
				{},
			}
			stores := map[string]interface {
				Source
				queryStore
			}{
				"BlockStore":  bs,
				"MemoryStore": bs.MemStore(),
			}
			ctx := context.Background()
			for name, store := range stores {
				ref := perSeriesSource{s: store}
				for q := range 60 {
					minT := int64(r.IntN(5*3600)-1800) * 1000
					p := SelectParams{
						Selector: sels[r.IntN(len(sels))],
						MinT:     minT,
						MaxT:     minT + int64(r.IntN(3*3600)-600)*1000, // sometimes empty
					}
					switch r.IntN(4) {
					case 0:
						p.Anchor = true
					case 1:
						p.SeriesOnly = true
					case 2:
						p.SeriesOnly, p.AnyTime = true, true
					}
					got, err := store.Select(ctx, p)
					if err != nil {
						t.Fatalf("%s Select(%+v): %v", name, p, err)
					}
					want, err := ref.Select(ctx, p)
					if err != nil {
						t.Fatalf("reference Select(%+v): %v", p, err)
					}
					if !reflect.DeepEqual(normalize(got), normalize(want)) {
						t.Fatalf("%s query %d %+v:\n native    %v\n reference %v", name, q, p, normalize(got), normalize(want))
					}
				}
			}
		})
	}
}

// normalize renders SeriesData comparably: DeepEqual on Labels compares cached
// internals, and a nil and an empty Samples slice mean the same thing.
func normalize(sds []SeriesData) []string {
	out := make([]string, len(sds))
	for i, sd := range sds {
		anchor := "none"
		if sd.Anchor != nil {
			anchor = fmt.Sprintf("%d/%v/%d", sd.Anchor.TimestampMs, sd.Anchor.Value, sd.Anchor.Gen)
		}
		samples := ""
		for _, s := range sd.Samples {
			samples += fmt.Sprintf("%d/%v/%d ", s.TimestampMs, s.Value, s.Gen)
		}
		out[i] = labelKey(sd.Labels) + " anchor=" + anchor + " samples=" + samples
	}
	return out
}

func TestStoresImplementSource(t *testing.T) {
	var _ Source = (*MemoryStore)(nil)
	var _ Source = (*BlockStore)(nil)
	var _ Source = (*WALStore)(nil)
}
