// Package index provides an in-memory inverted label index (postings) used by
// the metrics store for query planning. It is dependency-free and must not
// import internal/metrics (the metrics package depends on this one).
package index

import (
	"sort"
	"sync"
)

// Pair is a single label name/value.
type Pair struct {
	Name  string
	Value string
}

// MemPostings maps labelName -> labelValue -> sorted series IDs, plus a sorted
// list of all series IDs under the empty matcher. Safe for concurrent use.
type MemPostings struct {
	mu      sync.RWMutex
	m       map[string]map[string][]uint64
	allRefs []uint64
}

// NewMemPostings returns an empty MemPostings.
func NewMemPostings() *MemPostings {
	return &MemPostings{m: make(map[string]map[string][]uint64)}
}

// Add inserts id into the postings list of each label pair and into allRefs,
// keeping every list sorted ascending. Adding an id already present is a no-op.
func (p *MemPostings) Add(id uint64, labels []Pair) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.allRefs = insertSorted(p.allRefs, id)
	for _, l := range labels {
		vals, ok := p.m[l.Name]
		if !ok {
			vals = make(map[string][]uint64)
			p.m[l.Name] = vals
		}
		vals[l.Value] = insertSorted(vals[l.Value], id)
	}
}

// Postings returns the sorted series IDs for name=value. The empty pair
// ("","") returns allRefs. The returned slice is a copy and safe to mutate.
func (p *MemPostings) Postings(name, value string) []uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if name == "" && value == "" {
		return append([]uint64(nil), p.allRefs...)
	}
	if vals, ok := p.m[name]; ok {
		return append([]uint64(nil), vals[value]...)
	}
	return nil
}

// Select returns the sorted series IDs matching all matchers (AND). An empty
// matcher slice returns allRefs. Returns a nil/empty slice when nothing matches.
func (p *MemPostings) Select(matchers []Pair) []uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(matchers) == 0 {
		return append([]uint64(nil), p.allRefs...)
	}

	lists := make([][]uint64, 0, len(matchers))
	for _, m := range matchers {
		vals, ok := p.m[m.Name]
		if !ok {
			return nil
		}
		list := vals[m.Value]
		if len(list) == 0 {
			return nil
		}
		lists = append(lists, list)
	}
	// Intersect smallest list first.
	sort.Slice(lists, func(i, j int) bool { return len(lists[i]) < len(lists[j]) })
	result := append([]uint64(nil), lists[0]...)
	for _, list := range lists[1:] {
		result = intersectSorted(result, list)
		if len(result) == 0 {
			return nil
		}
	}
	return result
}

// insertSorted inserts id into the ascending slice s, skipping duplicates.
func insertSorted(s []uint64, id uint64) []uint64 {
	i := sort.Search(len(s), func(i int) bool { return s[i] >= id })
	if i < len(s) && s[i] == id {
		return s
	}
	s = append(s, 0)
	copy(s[i+1:], s[i:])
	s[i] = id
	return s
}

// intersectSorted returns the sorted intersection of two ascending slices.
func intersectSorted(a, b []uint64) []uint64 {
	var out []uint64
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] == b[j]:
			out = append(out, a[i])
			i++
			j++
		case a[i] < b[j]:
			i++
		default:
			j++
		}
	}
	return out
}

// Delete removes id from the postings list of each label pair and from allRefs.
// A label value whose list becomes empty is removed, and a label name whose
// values are all gone is dropped, so LabelNames/LabelNameCount never report
// names that no live series carries (which would otherwise leave metadata and
// cardinality stale once retention deletes series).
func (p *MemPostings) Delete(id uint64, labels []Pair) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.allRefs = removeSorted(p.allRefs, id)
	for _, l := range labels {
		vals, ok := p.m[l.Name]
		if !ok {
			continue
		}
		list := removeSorted(vals[l.Value], id)
		if len(list) == 0 {
			delete(vals, l.Value)
		} else {
			vals[l.Value] = list
		}
		if len(vals) == 0 {
			delete(p.m, l.Name)
		}
	}
}

// removeSorted shifts in-place; safe because Postings/Select return copies,
// so no caller holds a live reference to an internal slice.

// LabelNames returns all label names, sorted ascending.
func (p *MemPostings) LabelNames() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	names := make([]string, 0, len(p.m))
	for name := range p.m {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// LabelValues returns all values for name, sorted ascending. Values whose
// postings list is currently empty are omitted.
func (p *MemPostings) LabelValues(name string) []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	vals, ok := p.m[name]
	if !ok {
		return nil
	}
	out := make([]string, 0, len(vals))
	for v, list := range vals {
		if len(list) > 0 {
			out = append(out, v)
		}
	}
	sort.Strings(out)
	return out
}

// SeriesCount returns the number of distinct series IDs.
func (p *MemPostings) SeriesCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.allRefs)
}

// LabelNameCount returns the number of distinct label names currently carried by
// at least one series. Delete prunes names once their last value is removed, so
// this never counts stale names.
func (p *MemPostings) LabelNameCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.m)
}

// LabelPairCount returns the number of distinct name=value pairs with a
// non-empty postings list.
func (p *MemPostings) LabelPairCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	n := 0
	for _, vals := range p.m {
		for _, list := range vals {
			if len(list) > 0 {
				n++
			}
		}
	}
	return n
}

// removeSorted removes id from the ascending slice s if present.
func removeSorted(s []uint64, id uint64) []uint64 {
	i := sort.Search(len(s), func(i int) bool { return s[i] >= id })
	if i < len(s) && s[i] == id {
		return append(s[:i], s[i+1:]...)
	}
	return s
}
