package metrics

import (
	"encoding/binary"
	"hash/fnv"
	"sort"
)

// NewLabels validates, normalizes, and fingerprints a label set.
// The map must include __name__.
func NewLabels(m map[string]string) (Labels, error) {
	if err := validateLabelMap(m); err != nil {
		return Labels{}, err
	}
	pairs := make([]Label, 0, len(m))
	for k, v := range m {
		pairs = append(pairs, Label{Name: k, Value: v})
	}
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].Name < pairs[j].Name
	})
	return Labels{pairs: pairs, fp: fingerprint(pairs)}, nil
}

// Fingerprint returns the cached SeriesID derived from the normalized label set.
func (l Labels) Fingerprint() SeriesID {
	return l.fp
}

// Get returns the value for the given label name, or ("", false) if not present.
func (l Labels) Get(name string) (string, bool) {
	lo, hi := 0, len(l.pairs)
	for lo < hi {
		mid := (lo + hi) / 2
		switch {
		case l.pairs[mid].Name == name:
			return l.pairs[mid].Value, true
		case l.pairs[mid].Name < name:
			lo = mid + 1
		default:
			hi = mid
		}
	}
	return "", false
}

// Map returns a copy of the label set as a plain map.
func (l Labels) Map() map[string]string {
	m := make(map[string]string, len(l.pairs))
	for _, p := range l.pairs {
		m[p.Name] = p.Value
	}
	return m
}

// fingerprint computes a FNV-1a 64-bit hash over sorted label pairs.
// Each field is length-prefixed (8-byte big-endian uint64) then written as bytes.
// This is unambiguous regardless of what characters appear in label values.
func fingerprint(pairs []Label) SeriesID {
	h := fnv.New64a()
	var buf [8]byte
	for _, p := range pairs {
		binary.BigEndian.PutUint64(buf[:], uint64(len(p.Name)))
		h.Write(buf[:])
		h.Write([]byte(p.Name))
		binary.BigEndian.PutUint64(buf[:], uint64(len(p.Value)))
		h.Write(buf[:])
		h.Write([]byte(p.Value))
	}
	return SeriesID(h.Sum64())
}

// newOutputLabels builds a Labels for aggregation output. Unlike NewLabels,
// __name__ is not required — aggregated results do not carry a metric name.
func newOutputLabels(m map[string]string) Labels {
	pairs := make([]Label, 0, len(m))
	for k, v := range m {
		pairs = append(pairs, Label{Name: k, Value: v})
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].Name < pairs[j].Name })
	return Labels{pairs: pairs, fp: fingerprint(pairs)}
}
