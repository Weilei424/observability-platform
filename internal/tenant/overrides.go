package tenant

import (
	"bytes"
	"fmt"
	"os"
	"time"

	"go.yaml.in/yaml/v3"
)

// MaxPushBytes bounds a per-tenant push cap: a routed batch must still fit an
// internal push request (rpc.FlushBodyLimit is 64 MiB; JSON can grow a body
// several times).
const MaxPushBytes = 16 << 20

type Override struct {
	Retention           *time.Duration
	MaxActiveSeries     *int
	MaxActiveStreams    *int
	MaxMetricsPushBytes *int64
	MaxLogsPushBytes    *int64
}

type fileOverride struct {
	Retention           *string `yaml:"retention"`
	MaxActiveSeries     *int    `yaml:"max_active_series"`
	MaxActiveStreams    *int    `yaml:"max_active_streams"`
	MaxMetricsPushBytes *int64  `yaml:"max_metrics_push_bytes"`
	MaxLogsPushBytes    *int64  `yaml:"max_logs_push_bytes"`
}

// LoadOverrides reads the per-tenant overrides file. Any problem -- an
// unreadable file, an unknown key, an invalid tenant ID or value -- is an
// error: the process refuses to start rather than run with a limit the
// operator did not mean.
func LoadOverrides(path string) (map[string]Override, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("tenant overrides: %w", err)
	}
	var doc struct {
		Overrides map[string]fileOverride `yaml:"overrides"`
	}
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("tenant overrides %s: %w", path, err)
	}
	out := make(map[string]Override, len(doc.Overrides))
	for id, f := range doc.Overrides {
		if err := Validate(id); err != nil {
			return nil, fmt.Errorf("tenant overrides %s: %w", path, err)
		}
		o, err := f.parse()
		if err != nil {
			return nil, fmt.Errorf("tenant overrides %s: tenant %s: %w", path, id, err)
		}
		out[id] = o
	}
	return out, nil
}

func (f fileOverride) parse() (Override, error) {
	var o Override
	if f.Retention != nil {
		d, err := time.ParseDuration(*f.Retention)
		if err != nil {
			return o, fmt.Errorf("retention: %w", err)
		}
		o.Retention = &d
	}
	for name, v := range map[string]*int{"max_active_series": f.MaxActiveSeries, "max_active_streams": f.MaxActiveStreams} {
		if v != nil && *v < 0 {
			return o, fmt.Errorf("%s must be >= 0", name)
		}
	}
	for name, v := range map[string]*int64{"max_metrics_push_bytes": f.MaxMetricsPushBytes, "max_logs_push_bytes": f.MaxLogsPushBytes} {
		if v != nil && (*v <= 0 || *v > MaxPushBytes) {
			return o, fmt.Errorf("%s must be > 0 and <= %d", name, MaxPushBytes)
		}
	}
	o.MaxActiveSeries, o.MaxActiveStreams = f.MaxActiveSeries, f.MaxActiveStreams
	o.MaxMetricsPushBytes, o.MaxLogsPushBytes = f.MaxMetricsPushBytes, f.MaxLogsPushBytes
	return o, nil
}

// Defaults are the per-tenant values for a tenant the overrides do not list.
type Defaults struct {
	Retention           time.Duration
	MaxActiveSeries     int
	MaxActiveStreams    int
	MaxMetricsPushBytes int64
	MaxLogsPushBytes    int64
}

// Limits answers a tenant's retention and limits: its override, else the default.
type Limits struct {
	d Defaults
	o map[string]Override
}

func NewLimits(d Defaults, overrides map[string]Override) *Limits {
	return &Limits{d: d, o: overrides}
}

func (l *Limits) Retention(id string) time.Duration {
	if o, ok := l.o[id]; ok && o.Retention != nil {
		return *o.Retention
	}
	return l.d.Retention
}

func (l *Limits) MaxActiveSeries(id string) int {
	if o, ok := l.o[id]; ok && o.MaxActiveSeries != nil {
		return *o.MaxActiveSeries
	}
	return l.d.MaxActiveSeries
}

func (l *Limits) MaxActiveStreams(id string) int {
	if o, ok := l.o[id]; ok && o.MaxActiveStreams != nil {
		return *o.MaxActiveStreams
	}
	return l.d.MaxActiveStreams
}

func (l *Limits) MaxMetricsPushBytes(id string) int64 {
	if o, ok := l.o[id]; ok && o.MaxMetricsPushBytes != nil {
		return *o.MaxMetricsPushBytes
	}
	return l.d.MaxMetricsPushBytes
}

func (l *Limits) MaxLogsPushBytes(id string) int64 {
	if o, ok := l.o[id]; ok && o.MaxLogsPushBytes != nil {
		return *o.MaxLogsPushBytes
	}
	return l.d.MaxLogsPushBytes
}

// RetentionPolicy is every tenant's retention: the default plus overrides.
// A duration <= 0 keeps that tenant's data forever.
type RetentionPolicy struct {
	Default   time.Duration
	Overrides map[string]time.Duration
}

func (p RetentionPolicy) For(id string) time.Duration {
	if d, ok := p.Overrides[id]; ok {
		return d
	}
	return p.Default
}

func (l *Limits) RetentionPolicy() RetentionPolicy {
	p := RetentionPolicy{Default: l.d.Retention, Overrides: map[string]time.Duration{}}
	for id, o := range l.o {
		if o.Retention != nil {
			p.Overrides[id] = *o.Retention
		}
	}
	return p
}
