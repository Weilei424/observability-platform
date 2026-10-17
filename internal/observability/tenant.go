package observability

import "github.com/prometheus/client_golang/prometheus"

// TenantMetrics describe per-tenant usage and limit enforcement. The gauges are
// pull-model: they read per-tenant counts from their sources at scrape time.
// LimitRejections is push-model: the limit gates increment it.
type TenantMetrics struct {
	LimitRejections *prometheus.CounterVec // obs_tenant_limit_rejections_total{tenant,limit}
}

// NewTenantMetrics builds the instruments without registering them.
func NewTenantMetrics() *TenantMetrics {
	return &TenantMetrics{
		LimitRejections: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "obs_tenant_limit_rejections_total",
			Help: "Writes rejected by a per-tenant limit, by tenant and limit.",
		}, []string{"tenant", "limit"}),
	}
}

// Register registers the limit-rejection counter and, for each non-nil source,
// the matching per-tenant gauge. A nil source leaves its gauge unregistered
// rather than reporting nothing under that name.
func (m *TenantMetrics) Register(reg *prometheus.Registry, series func() map[string]int, streams func() map[string]int) {
	reg.MustRegister(m.LimitRejections)
	if series != nil {
		reg.MustRegister(&tenantGaugeCollector{
			src:  series,
			desc: prometheus.NewDesc("obs_tenant_active_series", "Number of active metric series per tenant.", []string{"tenant"}, nil),
		})
	}
	if streams != nil {
		reg.MustRegister(&tenantGaugeCollector{
			src:  streams,
			desc: prometheus.NewDesc("obs_tenant_active_streams", "Number of active log streams per tenant.", []string{"tenant"}, nil),
		})
	}
}

// tenantGaugeCollector emits one sample per tenant whose count is non-zero. A
// tenant at zero is omitted so an idle tenant does not hold a series open.
type tenantGaugeCollector struct {
	src  func() map[string]int
	desc *prometheus.Desc
}

func (c *tenantGaugeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

func (c *tenantGaugeCollector) Collect(ch chan<- prometheus.Metric) {
	for tenant, n := range c.src() {
		if n == 0 {
			continue
		}
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(n), tenant)
	}
}
