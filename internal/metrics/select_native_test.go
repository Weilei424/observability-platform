package metrics

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
)

// oracleFixtureTwoBlocks builds on oracleFixture without changing it (the
// per-tick oracle test depends on oracleFixture's exact shape): it appends a
// second, equally sized round of samples over the same series, flushes again,
// then closes the store and reopens it from the same directory. The reopened
// store has two on-disk blocks and a completely empty head, so every series it
// holds is block-only — coverage the plain oracleFixture store never
// exercises, because there every series lands in the head too (round(1000)
// picks uniformly among 7 series, so the odds any series is absent from it
// are astronomically small).
func oracleFixtureTwoBlocks(t *testing.T, seed uint64) *BlockStore {
	t.Helper()
	bs := oracleFixture(t, seed)

	var series []Labels
	for _, job := range []string{"a", "b"} {
		for inst := 1; inst <= 3; inst++ {
			series = append(series, mustLabelsInternal(t, map[string]string{
				"__name__": "oracle_metric", "job": job, "instance": strconv.Itoa(inst),
			}))
		}
	}
	series = append(series, mustLabelsInternal(t, map[string]string{"__name__": "oracle_other", "job": "a"}))

	// A distinct RNG stream from oracleFixture's internal one (which it does not
	// expose), seeded off the same seed for reproducibility.
	r := rand.New(rand.NewPCG(seed, seed^0xa5a5a5a5a5a5a5a5))
	value := func() float64 {
		switch r.IntN(50) {
		case 0:
			return math.NaN()
		case 1:
			return math.Inf(1)
		case 2:
			return math.Inf(-1)
		default:
			return float64(r.IntN(1000))
		}
	}
	for range 2000 {
		l := series[r.IntN(len(series))]
		ts := int64(r.IntN(4*3600)) * 1000
		if err := bs.Append(l, ts, value()); err != nil {
			t.Fatalf("oracleFixtureTwoBlocks: Append: %v", err)
		}
	}
	if wrote, err := bs.FlushBlock(); err != nil || !wrote {
		t.Fatalf("oracleFixtureTwoBlocks: FlushBlock = %v, %v; need a second persisted block", wrote, err)
	}

	// bs.blockDir is dataDir/metrics/blocks; recover dataDir to reopen fresh.
	dataDir := filepath.Dir(filepath.Dir(bs.blockDir))
	if err := bs.Close(); err != nil {
		t.Fatalf("oracleFixtureTwoBlocks: Close: %v", err)
	}
	reopened, err := NewBlockStore(dataDir)
	if err != nil {
		t.Fatalf("oracleFixtureTwoBlocks: NewBlockStore (reopen): %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	if n := len(reopened.blocks); n < 2 {
		t.Fatalf("oracleFixtureTwoBlocks: reopened store has %d blocks, want >= 2", n)
	}
	if n, _, _ := reopened.MemStore().Cardinality(); n != 0 {
		t.Fatalf("oracleFixtureTwoBlocks: reopened head has %d series, want an empty head", n)
	}
	return reopened
}

// Every native Select must return exactly what the per-series reference
// returns for the same store — same series, same order, same samples, same
// anchors — because all-in-one's responses must not change.
func TestNativeSelectMatchesTheReference(t *testing.T) {
	for seed := uint64(1); seed <= 4; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			bs := oracleFixture(t, seed)
			twoBlocks := oracleFixtureTwoBlocks(t, seed)
			r := rand.New(rand.NewPCG(seed, 11))
			sels := []Selector{
				{MetricName: "oracle_metric"},
				{MetricName: "oracle_metric", Matchers: []Matcher{{Name: "job", Value: "b"}}},
				{Matchers: []Matcher{{Name: "job", Value: "a"}}},
				{},
			}
			// A slice, not a map: both stores draw from the one shared RNG below,
			// and a map's randomized iteration order would change which store
			// consumes which 60 draws from run to run, making a failure
			// unreproducible even at a fixed seed.
			stores := []struct {
				name  string
				store interface {
					Source
					queryStore
				}
			}{
				{"BlockStore", bs},
				{"MemoryStore", bs.MemStore()},
				{"BlockStoreTwoBlocksEmptyHead", twoBlocks},
			}
			ctx := context.Background()
			for _, sc := range stores {
				name, store := sc.name, sc.store
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
