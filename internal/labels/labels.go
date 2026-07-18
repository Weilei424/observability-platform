package labels

import (
	"encoding/binary"
	"hash/fnv"
	"sort"
)

// Label is a single name/value pair.
type Label struct {
	Name  string
	Value string
}

// Labels is an immutable, normalized set of labels with a cached hash.
// Always construct via New or NewUnvalidated — never the zero value directly.
type Labels struct {
	pairs []Label // sorted by Name
	hash  uint64  // computed once at construction
}

// New validates, normalizes, and hashes a label set. __name__ is treated as an
// ordinary label and is not required.
func New(m map[string]string) (Labels, error) {
	if err := validateLabelMap(m); err != nil {
		return Labels{}, err
	}
	return build(m), nil
}

// NewUnvalidated builds a Labels without validation (e.g. aggregation output).
func NewUnvalidated(m map[string]string) Labels {
	return build(m)
}

func build(m map[string]string) Labels {
	pairs := make([]Label, 0, len(m))
	for k, v := range m {
		pairs = append(pairs, Label{Name: k, Value: v})
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].Name < pairs[j].Name })
	return Labels{pairs: pairs, hash: fingerprint(pairs)}
}

// Hash returns the cached fingerprint of the normalized label set.
func (l Labels) Hash() uint64 { return l.hash }

// Get returns the value for name, or ("", false) if absent.
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

// Len returns the number of labels.
func (l Labels) Len() int { return len(l.pairs) }

// fingerprint computes an FNV-1a 64-bit hash over sorted, length-prefixed pairs.
// Moved verbatim from the metrics package — persisted SeriesIDs depend on this
// exact encoding.
func fingerprint(pairs []Label) uint64 {
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
	return h.Sum64()
}
