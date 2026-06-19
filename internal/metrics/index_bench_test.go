// internal/metrics/index_bench_test.go
package metrics

import (
	"fmt"
	"reflect"
	"sort"
	"testing"
)

// buildStore creates nSeries across the "http" metric with job/instance labels.
func buildStore(b *testing.B, nSeries int) *MemoryStore {
	s := NewMemoryStore()
	for i := 0; i < nSeries; i++ {
		l, err := NewLabels(map[string]string{
			"__name__": "http_requests_total",
			"job":      fmt.Sprintf("job-%d", i%20),
			"instance": fmt.Sprintf("inst-%d", i),
		})
		if err != nil {
			b.Fatalf("NewLabels: %v", err)
		}
		if err := s.Append(l, 1000, 1); err != nil {
			b.Fatalf("append: %v", err)
		}
	}
	return s
}

// fullScanSelect replicates the pre-index full-scan behavior for comparison.
func fullScanSelect(s *MemoryStore, sel Selector) []MatchedSeries {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []MatchedSeries
	for id, ms := range s.series {
		if sel.MetricName != "" {
			if name, _ := ms.labels.Get("__name__"); name != sel.MetricName {
				continue
			}
		}
		match := true
		for _, m := range sel.Matchers {
			if v, ok := ms.labels.Get(m.Name); !ok || v != m.Value {
				match = false
				break
			}
		}
		if match {
			result = append(result, MatchedSeries{id: id, Labels: ms.labels})
		}
	}
	return result
}

func BenchmarkSelectSeries_Indexed(b *testing.B) {
	s := buildStore(b, 10000)
	sel := Selector{MetricName: "http_requests_total", Matchers: []Matcher{{Name: "job", Value: "job-7"}}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.SelectSeries(sel)
	}
}

func BenchmarkSelectSeries_FullScan(b *testing.B) {
	s := buildStore(b, 10000)
	sel := Selector{MetricName: "http_requests_total", Matchers: []Matcher{{Name: "job", Value: "job-7"}}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = fullScanSelect(s, sel)
	}
}

// TestIndexAndScanAgree guards correctness: indexed and full-scan results match.
func TestIndexAndScanAgree(t *testing.T) {
	s := NewMemoryStore()
	for i := 0; i < 500; i++ {
		l, err := NewLabels(map[string]string{
			"__name__": "http_requests_total",
			"job":      fmt.Sprintf("job-%d", i%20),
			"instance": fmt.Sprintf("inst-%d", i),
		})
		if err != nil {
			t.Fatalf("NewLabels: %v", err)
		}
		if err := s.Append(l, 1000, 1); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	sel := Selector{MetricName: "http_requests_total", Matchers: []Matcher{{Name: "job", Value: "job-3"}}}
	indexedResults := s.SelectSeries(sel)
	fullScanResults := fullScanSelect(s, sel)

	// Extract and sort IDs from indexed results
	var indexedIDs []SeriesID
	for _, ms := range indexedResults {
		indexedIDs = append(indexedIDs, ms.id)
	}
	sort.Slice(indexedIDs, func(i, j int) bool { return indexedIDs[i] < indexedIDs[j] })

	// Extract and sort IDs from full-scan results
	var fullScanIDs []SeriesID
	for _, ms := range fullScanResults {
		fullScanIDs = append(fullScanIDs, ms.id)
	}
	sort.Slice(fullScanIDs, func(i, j int) bool { return fullScanIDs[i] < fullScanIDs[j] })

	// Compare sorted IDs
	if !reflect.DeepEqual(indexedIDs, fullScanIDs) {
		t.Fatalf("indexed and scan disagree on series IDs: indexed=%v, fullScan=%v", indexedIDs, fullScanIDs)
	}
}
