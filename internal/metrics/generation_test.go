package metrics_test

import (
	"context"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/storage/chunk"
)

func genOf(t *testing.T, s *metrics.MemoryStore, name string, ts int64) int64 {
	t.Helper()
	sds, err := s.Select(context.Background(), metrics.SelectParams{
		Selector: metrics.Selector{MetricName: name}, MinT: ts, MaxT: ts,
	})
	if err != nil || len(sds) != 1 || len(sds[0].Samples) != 1 {
		t.Fatalf("select %s@%d = %+v, %v", name, ts, sds, err)
	}
	return sds[0].Samples[0].Gen
}

func lbl(t *testing.T, name string) metrics.Labels {
	t.Helper()
	l, err := metrics.NewLabels(map[string]string{"__name__": name})
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func TestGenerationFollowsTheClock(t *testing.T) {
	s := metrics.NewMemoryStore()
	now := int64(5_000_000)
	s.SetGenerationClock(func() int64 { return now })
	_ = s.Append(lbl(t, "a"), 1, 1)
	if g := genOf(t, s, "a", 1); g != 5_000_000 {
		t.Errorf("gen = %d, want the clock's 5000000", g)
	}
	_ = s.Append(lbl(t, "a"), 2, 1) // same microsecond: strictly after the last
	if g := genOf(t, s, "a", 2); g != 5_000_001 {
		t.Errorf("gen = %d, want 5000001", g)
	}
	now = 9_000_000
	_ = s.Append(lbl(t, "a"), 3, 1)
	if g := genOf(t, s, "a", 3); g != 9_000_000 {
		t.Errorf("gen = %d, want 9000000", g)
	}
}

func TestGenerationClockStepBack(t *testing.T) {
	s := metrics.NewMemoryStore()
	now := int64(9_000_000)
	s.SetGenerationClock(func() int64 { return now })
	_ = s.Append(lbl(t, "a"), 1, 1)
	now = 1_000 // the clock stepped back
	_ = s.Append(lbl(t, "a"), 2, 1)
	if g := genOf(t, s, "a", 2); g != 9_000_001 {
		t.Errorf("gen after a step back = %d, want 9000001 (never below the last)", g)
	}
}

func TestZeroClockIsTheOldCounter(t *testing.T) {
	s := metrics.NewMemoryStore()
	s.SetGenerationClock(func() int64 { return 0 })
	for ts := int64(1); ts <= 3; ts++ {
		_ = s.Append(lbl(t, "a"), ts, 1)
		if g := genOf(t, s, "a", ts); g != ts {
			t.Errorf("gen = %d, want %d", g, ts)
		}
	}
}

func TestAppendGenUsesTheGivenGenerationAndRaisesTheFloor(t *testing.T) {
	s := metrics.NewMemoryStore()
	s.SetGenerationClock(func() int64 { return 0 })
	if err := s.AppendGen(lbl(t, "a"), 1, 1, 777); err != nil {
		t.Fatal(err)
	}
	if g := genOf(t, s, "a", 1); g != 777 {
		t.Errorf("restored gen = %d, want 777", g)
	}
	if next := s.NextGeneration(); next != 778 {
		t.Errorf("NextGeneration = %d, want 778", next)
	}
	if err := s.AppendGen(lbl(t, "a"), 2, 1, 0); err != nil { // 0 assigns
		t.Fatal(err)
	}
	if g := genOf(t, s, "a", 2); g != 778 {
		t.Errorf("assigned gen = %d, want 778", g)
	}
}

func TestReserveGeneration(t *testing.T) {
	s := metrics.NewMemoryStore()
	s.SetGenerationClock(func() int64 { return 50 })
	g, err := s.ReserveGeneration()
	if err != nil || g != 50 {
		t.Fatalf("Reserve = %d, %v; want 50", g, err)
	}
	if g2, _ := s.ReserveGeneration(); g2 != 51 {
		t.Errorf("second Reserve = %d, want 51", g2)
	}
	s.EnsureGenFloor(chunk.MaxGeneration + 1)
	if _, err := s.ReserveGeneration(); err != metrics.ErrGenerationExhausted {
		t.Errorf("Reserve past MaxGeneration = %v, want ErrGenerationExhausted", err)
	}
	if err := s.AppendGen(lbl(t, "b"), 1, 1, chunk.MaxGeneration+1); err != metrics.ErrGenerationExhausted {
		t.Errorf("AppendGen past MaxGeneration = %v, want ErrGenerationExhausted", err)
	}
}

func TestWallClockGenerationsAreMicroseconds(t *testing.T) {
	s := metrics.NewMemoryStore()
	_ = s.Append(lbl(t, "a"), 1, 1)
	g := genOf(t, s, "a", 1)
	// 2020-01-01 and 2100-01-01 in µs: a nanosecond or millisecond clock falls outside.
	if g < 1_577_836_800_000_000 || g > 4_102_444_800_000_000 {
		t.Errorf("wall-clock gen = %d, want Unix microseconds", g)
	}
}
