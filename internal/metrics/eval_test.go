package metrics_test

import (
	"testing"

	"github.com/masonwheeler/observability-platform/internal/metrics"
)

func TestEvalInstant_SelectorExpr_DelegatesToInstantQuery(t *testing.T) {
	engine, store := newEngineWithSamples(t)

	labels := mustNewLabels(t, map[string]string{"__name__": "cpu_usage", "host": "a"})
	_ = store.Append(labels, 1000, 42.0)

	expr := metrics.SelectorExpr{Selector: metrics.Selector{MetricName: "cpu_usage"}}
	result, err := engine.EvalInstant(expr, 1000)
	if err != nil {
		t.Fatalf("EvalInstant: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("len = %d, want 1", len(result))
	}
	if result[0].Value != 42.0 {
		t.Errorf("Value = %v, want 42.0", result[0].Value)
	}
}

func TestEvalRange_SelectorExpr_DelegatesToRangeQuery(t *testing.T) {
	engine, store := newEngineWithSamples(t)

	labels := mustNewLabels(t, map[string]string{"__name__": "cpu_usage"})
	_ = store.Append(labels, 1000, 1.0)
	_ = store.Append(labels, 3000, 3.0)

	expr := metrics.SelectorExpr{Selector: metrics.Selector{MetricName: "cpu_usage"}}
	result, err := engine.EvalRange(expr, 1000, 3000, 1000)
	if err != nil {
		t.Fatalf("EvalRange: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("len = %d, want 1", len(result))
	}
	if len(result[0].Points) != 3 {
		t.Errorf("points = %d, want 3", len(result[0].Points))
	}
}

func TestEvalRange_ZeroStep_ReturnsError(t *testing.T) {
	engine, _ := newEngineWithSamples(t)
	expr := metrics.SelectorExpr{Selector: metrics.Selector{MetricName: "cpu_usage"}}
	_, err := engine.EvalRange(expr, 1000, 3000, 0)
	if err == nil {
		t.Error("expected error for step=0, got nil")
	}
}

func TestEvalRange_EndBeforeStart_ReturnsError(t *testing.T) {
	engine, _ := newEngineWithSamples(t)
	expr := metrics.SelectorExpr{Selector: metrics.Selector{MetricName: "cpu_usage"}}
	_, err := engine.EvalRange(expr, 3000, 1000, 1000)
	if err == nil {
		t.Error("expected error for end < start, got nil")
	}
}

func TestEvalInstant_Rate_TwoSamplesInWindow_ReturnsPerSecondRate(t *testing.T) {
	engine, store := newEngineWithSamples(t)

	labels := mustNewLabels(t, map[string]string{"__name__": "requests_total"})
	// value goes from 100 to 160 over 60s → rate = 1.0/sec
	_ = store.Append(labels, 0, 100.0)
	_ = store.Append(labels, 60000, 160.0)

	expr := metrics.RateExpr{
		Selector: metrics.Selector{MetricName: "requests_total"},
		WindowMs: 60000,
	}
	result, err := engine.EvalInstant(expr, 60000)
	if err != nil {
		t.Fatalf("EvalInstant: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("len = %d, want 1", len(result))
	}
	if result[0].Value != 1.0 {
		t.Errorf("rate = %v, want 1.0", result[0].Value)
	}
	if result[0].TimestampMs != 60000 {
		t.Errorf("TimestampMs = %d, want 60000", result[0].TimestampMs)
	}
}

func TestEvalInstant_Rate_FewerThanTwoSamples_OmitsSeries(t *testing.T) {
	engine, store := newEngineWithSamples(t)

	labels := mustNewLabels(t, map[string]string{"__name__": "requests_total"})
	_ = store.Append(labels, 60000, 100.0) // only one sample

	expr := metrics.RateExpr{
		Selector: metrics.Selector{MetricName: "requests_total"},
		WindowMs: 60000,
	}
	result, err := engine.EvalInstant(expr, 60000)
	if err != nil {
		t.Fatalf("EvalInstant: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("len = %d, want 0 (fewer than 2 samples in window)", len(result))
	}
}

func TestEvalInstant_Rate_OutputLabelsMatchSeries(t *testing.T) {
	engine, store := newEngineWithSamples(t)

	labels := mustNewLabels(t, map[string]string{"__name__": "requests_total", "service": "api"})
	_ = store.Append(labels, 0, 0.0)
	_ = store.Append(labels, 30000, 30.0)

	expr := metrics.RateExpr{
		Selector: metrics.Selector{MetricName: "requests_total"},
		WindowMs: 30000,
	}
	result, err := engine.EvalInstant(expr, 30000)
	if err != nil {
		t.Fatalf("EvalInstant: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("len = %d, want 1", len(result))
	}
	svc, ok := result[0].Labels.Get("service")
	if !ok || svc != "api" {
		t.Errorf("service label = %q, want api", svc)
	}
}

func TestEvalRange_Rate_ReEvaluatesAtEachTick(t *testing.T) {
	engine, store := newEngineWithSamples(t)

	labels := mustNewLabels(t, map[string]string{"__name__": "requests_total"})
	// Samples every 30s; value increments by 30 each interval → rate = 1.0/sec always
	_ = store.Append(labels, 0, 0.0)
	_ = store.Append(labels, 30000, 30.0)
	_ = store.Append(labels, 60000, 60.0)
	_ = store.Append(labels, 90000, 90.0)

	// rate[60s] over range [60s, 90s] step 30s
	// tick 60000: window [0, 60000] → first=0 last=60 → rate=60/60=1.0
	// tick 90000: window [30000, 90000] → first=30 last=90 → rate=60/60=1.0
	expr := metrics.RateExpr{
		Selector: metrics.Selector{MetricName: "requests_total"},
		WindowMs: 60000,
	}
	result, err := engine.EvalRange(expr, 60000, 90000, 30000)
	if err != nil {
		t.Fatalf("EvalRange: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("len = %d, want 1", len(result))
	}
	pts := result[0].Points
	if len(pts) != 2 {
		t.Fatalf("points = %d, want 2", len(pts))
	}
	for i, pt := range pts {
		if pt.Value != 1.0 {
			t.Errorf("pts[%d].Value = %v, want 1.0", i, pt.Value)
		}
	}
}

func TestEvalRange_Rate_TickWithFewerThanTwoSamples_Omitted(t *testing.T) {
	engine, store := newEngineWithSamples(t)

	labels := mustNewLabels(t, map[string]string{"__name__": "requests_total"})
	_ = store.Append(labels, 60000, 100.0) // only one sample total

	expr := metrics.RateExpr{
		Selector: metrics.Selector{MetricName: "requests_total"},
		WindowMs: 60000,
	}
	// tick 60000: window [0, 60000] → only one sample → omitted
	result, err := engine.EvalRange(expr, 60000, 60000, 30000)
	if err != nil {
		t.Fatalf("EvalRange: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("len = %d, want 0", len(result))
	}
}

func TestEvalInstant_Sum_CollapseToSingleValue(t *testing.T) {
	engine, store := newEngineWithSamples(t)

	la := mustNewLabels(t, map[string]string{"__name__": "requests_total", "service": "api"})
	lb := mustNewLabels(t, map[string]string{"__name__": "requests_total", "service": "db"})
	_ = store.Append(la, 1000, 10.0)
	_ = store.Append(lb, 1000, 20.0)

	expr := metrics.SumExpr{
		Inner: metrics.SelectorExpr{Selector: metrics.Selector{MetricName: "requests_total"}},
	}
	result, err := engine.EvalInstant(expr, 1000)
	if err != nil {
		t.Fatalf("EvalInstant: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("len = %d, want 1", len(result))
	}
	if result[0].Value != 30.0 {
		t.Errorf("Value = %v, want 30.0", result[0].Value)
	}
	// Ungrouped sum has empty labels
	if len(result[0].Labels.Map()) != 0 {
		t.Errorf("Labels = %v, want {}", result[0].Labels.Map())
	}
}

func TestEvalInstant_Sum_EmptyInner_ReturnsEmpty(t *testing.T) {
	engine, _ := newEngineWithSamples(t)

	expr := metrics.SumExpr{
		Inner: metrics.SelectorExpr{Selector: metrics.Selector{MetricName: "nonexistent"}},
	}
	result, err := engine.EvalInstant(expr, 1000)
	if err != nil {
		t.Fatalf("EvalInstant: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("len = %d, want 0", len(result))
	}
}

func TestEvalInstant_SumBy_GroupsBySingleLabel(t *testing.T) {
	engine, store := newEngineWithSamples(t)

	la := mustNewLabels(t, map[string]string{"__name__": "requests_total", "service": "api", "region": "us"})
	lb := mustNewLabels(t, map[string]string{"__name__": "requests_total", "service": "api", "region": "eu"})
	lc := mustNewLabels(t, map[string]string{"__name__": "requests_total", "service": "db", "region": "us"})
	_ = store.Append(la, 1000, 10.0)
	_ = store.Append(lb, 1000, 20.0)
	_ = store.Append(lc, 1000, 5.0)

	expr := metrics.SumExpr{
		Inner: metrics.SelectorExpr{Selector: metrics.Selector{MetricName: "requests_total"}},
		By:    []string{"service"},
	}
	result, err := engine.EvalInstant(expr, 1000)
	if err != nil {
		t.Fatalf("EvalInstant: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("len = %d, want 2 (one per service)", len(result))
	}
	sums := make(map[string]float64)
	for _, r := range result {
		svc, _ := r.Labels.Get("service")
		sums[svc] = r.Value
	}
	if sums["api"] != 30.0 {
		t.Errorf("api sum = %v, want 30.0", sums["api"])
	}
	if sums["db"] != 5.0 {
		t.Errorf("db sum = %v, want 5.0", sums["db"])
	}
	// Output labels contain only the grouping label, not __name__ or region
	for _, r := range result {
		m := r.Labels.Map()
		if _, hasName := m["__name__"]; hasName {
			t.Errorf("output should not contain __name__, got %v", m)
		}
		if _, hasRegion := m["region"]; hasRegion {
			t.Errorf("output should not contain region, got %v", m)
		}
	}
}

func TestEvalInstant_SumBy_MultipleLabels_CompositeGroups(t *testing.T) {
	engine, store := newEngineWithSamples(t)

	la := mustNewLabels(t, map[string]string{"__name__": "req", "service": "api", "env": "prod"})
	lb := mustNewLabels(t, map[string]string{"__name__": "req", "service": "api", "env": "staging"})
	lc := mustNewLabels(t, map[string]string{"__name__": "req", "service": "db", "env": "prod"})
	_ = store.Append(la, 1000, 1.0)
	_ = store.Append(lb, 1000, 2.0)
	_ = store.Append(lc, 1000, 4.0)

	expr := metrics.SumExpr{
		Inner: metrics.SelectorExpr{Selector: metrics.Selector{MetricName: "req"}},
		By:    []string{"service", "env"},
	}
	result, err := engine.EvalInstant(expr, 1000)
	if err != nil {
		t.Fatalf("EvalInstant: %v", err)
	}
	// Three distinct (service, env) pairs → three groups
	if len(result) != 3 {
		t.Fatalf("len = %d, want 3", len(result))
	}
}

func TestEvalRange_Sum_SumsPerTick(t *testing.T) {
	engine, store := newEngineWithSamples(t)

	la := mustNewLabels(t, map[string]string{"__name__": "requests_total", "service": "api"})
	lb := mustNewLabels(t, map[string]string{"__name__": "requests_total", "service": "db"})
	_ = store.Append(la, 1000, 10.0)
	_ = store.Append(lb, 1000, 5.0)
	_ = store.Append(la, 2000, 20.0)
	_ = store.Append(lb, 2000, 8.0)

	expr := metrics.SumExpr{
		Inner: metrics.SelectorExpr{Selector: metrics.Selector{MetricName: "requests_total"}},
	}
	result, err := engine.EvalRange(expr, 1000, 2000, 1000)
	if err != nil {
		t.Fatalf("EvalRange: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("len = %d, want 1", len(result))
	}
	pts := result[0].Points
	if len(pts) != 2 {
		t.Fatalf("points = %d, want 2", len(pts))
	}
	// tick 1000: 10+5=15; tick 2000: 20+8=28
	wantVals := map[int64]float64{1000: 15.0, 2000: 28.0}
	for _, pt := range pts {
		if want, ok := wantVals[pt.TimestampMs]; !ok || pt.Value != want {
			t.Errorf("tick %d: Value = %v, want %v", pt.TimestampMs, pt.Value, want)
		}
	}
}

func TestEvalRange_SumBy_GroupsPerTick(t *testing.T) {
	engine, store := newEngineWithSamples(t)

	la := mustNewLabels(t, map[string]string{"__name__": "requests_total", "service": "api"})
	lb := mustNewLabels(t, map[string]string{"__name__": "requests_total", "service": "db"})
	_ = store.Append(la, 1000, 10.0)
	_ = store.Append(lb, 1000, 5.0)

	expr := metrics.SumExpr{
		Inner: metrics.SelectorExpr{Selector: metrics.Selector{MetricName: "requests_total"}},
		By:    []string{"service"},
	}
	result, err := engine.EvalRange(expr, 1000, 1000, 1000)
	if err != nil {
		t.Fatalf("EvalRange: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("len = %d, want 2 (one per service)", len(result))
	}
}

func TestEvalInstant_SumBy_AbsentLabelEmittedAsEmptyString(t *testing.T) {
	// Series missing a by-label must be grouped under "" for that label,
	// and the output label map must include the label with value "".
	engine, store := newEngineWithSamples(t)

	// la has env; lb does not.
	la := mustNewLabels(t, map[string]string{"__name__": "req", "env": "prod"})
	lb := mustNewLabels(t, map[string]string{"__name__": "req"})
	_ = store.Append(la, 1000, 3.0)
	_ = store.Append(lb, 1000, 7.0)

	expr := metrics.SumExpr{
		Inner: metrics.SelectorExpr{Selector: metrics.Selector{MetricName: "req"}},
		By:    []string{"env"},
	}
	result, err := engine.EvalInstant(expr, 1000)
	if err != nil {
		t.Fatalf("EvalInstant: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("len = %d, want 2 (prod group and absent group)", len(result))
	}
	byEnv := make(map[string]float64)
	for _, r := range result {
		env, _ := r.Labels.Get("env")
		byEnv[env] = r.Value
		// All output series must carry the "env" label (possibly "").
		if _, ok := r.Labels.Map()["env"]; !ok {
			t.Errorf("output label map missing 'env' key: %v", r.Labels.Map())
		}
	}
	if byEnv["prod"] != 3.0 {
		t.Errorf("prod sum = %v, want 3.0", byEnv["prod"])
	}
	if byEnv[""] != 7.0 {
		t.Errorf("absent-label sum = %v, want 7.0", byEnv[""])
	}
}

func TestEvalRange_SumBy_AbsentLabelEmittedAsEmptyString(t *testing.T) {
	engine, store := newEngineWithSamples(t)

	la := mustNewLabels(t, map[string]string{"__name__": "req", "env": "prod"})
	lb := mustNewLabels(t, map[string]string{"__name__": "req"})
	_ = store.Append(la, 1000, 3.0)
	_ = store.Append(lb, 1000, 7.0)

	expr := metrics.SumExpr{
		Inner: metrics.SelectorExpr{Selector: metrics.Selector{MetricName: "req"}},
		By:    []string{"env"},
	}
	result, err := engine.EvalRange(expr, 1000, 1000, 1000)
	if err != nil {
		t.Fatalf("EvalRange: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("len = %d, want 2 (prod group and absent group)", len(result))
	}
	byEnv := make(map[string]float64)
	for _, rs := range result {
		env, _ := rs.Labels.Get("env")
		if len(rs.Points) != 1 {
			t.Fatalf("env=%q: points = %d, want 1", env, len(rs.Points))
		}
		byEnv[env] = rs.Points[0].Value
		if _, ok := rs.Labels.Map()["env"]; !ok {
			t.Errorf("output label map missing 'env' key: %v", rs.Labels.Map())
		}
	}
	if byEnv["prod"] != 3.0 {
		t.Errorf("prod sum = %v, want 3.0", byEnv["prod"])
	}
	if byEnv[""] != 7.0 {
		t.Errorf("absent-label sum = %v, want 7.0", byEnv[""])
	}
}

func TestEvalInstant_SumOfRate_ComposesCorrectly(t *testing.T) {
	engine, store := newEngineWithSamples(t)

	la := mustNewLabels(t, map[string]string{"__name__": "requests_total", "service": "api"})
	lb := mustNewLabels(t, map[string]string{"__name__": "requests_total", "service": "db"})
	// Both series: 0 → 60 over 60s → individual rate = 1.0/sec; sum = 2.0/sec
	_ = store.Append(la, 0, 0.0)
	_ = store.Append(la, 60000, 60.0)
	_ = store.Append(lb, 0, 0.0)
	_ = store.Append(lb, 60000, 60.0)

	expr := metrics.SumExpr{
		Inner: metrics.RateExpr{
			Selector: metrics.Selector{MetricName: "requests_total"},
			WindowMs: 60000,
		},
	}
	result, err := engine.EvalInstant(expr, 60000)
	if err != nil {
		t.Fatalf("EvalInstant: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("len = %d, want 1", len(result))
	}
	if result[0].Value != 2.0 {
		t.Errorf("sum(rate) = %v, want 2.0", result[0].Value)
	}
}
