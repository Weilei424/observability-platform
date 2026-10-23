package metrics

import (
	"context"
	"sort"

	"github.com/masonwheeler/observability-platform/internal/labels"
	"github.com/masonwheeler/observability-platform/internal/tenant"
)

// TenantSource restricts src to the tenant each read's context acts for
// (tenant.From) and strips the tenant label from everything it returns. It
// wraps local storage where it is served -- all-in-one's engine, the
// ingester's and the store's internal read routes -- so the engine, RPC
// answers and API responses never see another tenant or the label itself.
//
// A named tenant's read adds the matcher __tenant__=<id>, which the index
// answers from postings; the default tenant's read drops every series that
// carries the label. A selector that names __tenant__ itself matches nothing,
// for every tenant including the one it names: the label is internal, and no
// series a caller can see carries it.
func TenantSource(src Source) Source { return tenantSource{src: src} }

type tenantSource struct{ src Source }

func (t tenantSource) Select(ctx context.Context, p SelectParams) ([]SeriesData, error) {
	for _, m := range p.Selector.Matchers {
		if m.Name == tenant.Label {
			if err := p.Validate(); err != nil {
				return nil, err
			}
			return []SeriesData{}, nil
		}
	}
	id := tenant.From(ctx)
	if id != tenant.Default {
		p.Selector.Matchers = append(append([]Matcher(nil), p.Selector.Matchers...), Matcher{Name: tenant.Label, Value: id})
	}
	got, err := t.src.Select(ctx, p)
	if err != nil {
		return nil, err
	}
	out := make([]SeriesData, 0, len(got))
	for _, sd := range got {
		if labels.TenantOf(sd.Labels) != id {
			continue
		}
		sd.Labels = labels.Without(sd.Labels, tenant.Label)
		out = append(out, sd)
	}
	return out, nil
}

// tenantSeries is every series of ctx's tenant, labels already stripped.
func (t tenantSource) tenantSeries(ctx context.Context) ([]SeriesData, error) {
	return t.Select(ctx, SelectParams{SeriesOnly: true, AnyTime: true})
}

func (t tenantSource) SelectLabelNames(ctx context.Context) ([]string, error) {
	series, err := t.tenantSeries(ctx)
	if err != nil {
		return nil, err
	}
	set := map[string]struct{}{}
	for _, sd := range series {
		for name := range sd.Labels.Map() {
			set[name] = struct{}{}
		}
	}
	return sortedSet(set), nil
}

func (t tenantSource) SelectLabelValues(ctx context.Context, name string) ([]string, error) {
	series, err := t.tenantSeries(ctx)
	if err != nil {
		return nil, err
	}
	set := map[string]struct{}{}
	for _, sd := range series {
		if v, ok := sd.Labels.Get(name); ok {
			set[v] = struct{}{}
		}
	}
	return sortedSet(set), nil
}

func sortedSet(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for s := range set {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}
