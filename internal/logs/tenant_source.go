package logs

import (
	"context"

	"github.com/masonwheeler/observability-platform/internal/labels"
	"github.com/masonwheeler/observability-platform/internal/storage/index"
	"github.com/masonwheeler/observability-platform/internal/tenant"
)

// TenantSource is metrics.TenantSource for logs, over a local Reader so label
// names and values come from stream label sets without reading any entry.
func TenantSource(r Reader) Source { return tenantSource{r: r, inner: AsSource(r)} }

type tenantSource struct {
	r     Reader
	inner Source
}

func (t tenantSource) matchers(ctx context.Context, m []index.Pair) []index.Pair {
	if id := tenant.From(ctx); id != tenant.Default {
		return append(append([]index.Pair(nil), m...), index.Pair{Name: tenant.Label, Value: id})
	}
	return m
}

func (t tenantSource) SelectStreams(ctx context.Context, matchers []index.Pair, minTs, maxTs int64) ([]StreamData, error) {
	id := tenant.From(ctx)
	got, err := t.inner.SelectStreams(ctx, t.matchers(ctx, matchers), minTs, maxTs)
	if err != nil {
		return nil, err
	}
	out := make([]StreamData, 0, len(got))
	for _, sd := range got {
		if labels.TenantOf(sd.Labels) != id {
			continue
		}
		sd.Labels = labels.Without(sd.Labels, tenant.Label)
		out = append(out, sd)
	}
	return out, nil
}

// tenantLabelSets is every stream label set of ctx's tenant, stripped.
func (t tenantSource) tenantLabelSets(ctx context.Context) []StreamLabels {
	id := tenant.From(ctx)
	var out []StreamLabels
	for _, sid := range t.r.MatchingStreamIDs(t.matchers(ctx, nil)) {
		l, ok := t.r.StreamLabelSet(sid)
		if !ok || labels.TenantOf(l) != id {
			continue
		}
		out = append(out, labels.Without(l, tenant.Label))
	}
	return out
}

func (t tenantSource) SelectLabelNames(ctx context.Context) ([]string, error) {
	set := map[string]struct{}{}
	for _, l := range t.tenantLabelSets(ctx) {
		for name := range l.Map() {
			set[name] = struct{}{}
		}
	}
	return sortedKeys(set), nil
}

func (t tenantSource) SelectLabelValues(ctx context.Context, name string) ([]string, error) {
	set := map[string]struct{}{}
	for _, l := range t.tenantLabelSets(ctx) {
		if v, ok := l.Get(name); ok {
			set[v] = struct{}{}
		}
	}
	return sortedKeys(set), nil
}
