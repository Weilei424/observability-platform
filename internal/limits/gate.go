// Package limits admits writes against per-tenant active series and stream
// limits (Phase 6.5).
package limits

import (
	"fmt"
	"sort"
	"sync"
)

type Kind string

const (
	Series  Kind = "series"
	Streams Kind = "streams"
)

type LimitError struct {
	Tenant string
	Kind   Kind
	Limit  int
}

func (e *LimitError) Error() string {
	return fmt.Sprintf("tenant %s would exceed its active %s limit (%d)", e.Tenant, e.Kind, e.Limit)
}

func Share(limit, rf, n int) int {
	if limit <= 0 {
		return 0
	}
	if n <= 0 {
		n = 1
	}
	share := (limit*rf + n - 1) / n
	return min(max(share, 1), limit)
}

type Counter interface {
	Count(tenant string) int
	Has(id uint64) bool
}

type Gate struct {
	kind     Kind
	limit    func(string) int
	c        Counter
	onReject func(string)
	mu       sync.Mutex
	tenants  map[string]*sync.Mutex
}

func NewGate(kind Kind, limit func(string) int, c Counter, onReject func(string)) *Gate {
	return &Gate{kind: kind, limit: limit, c: c, onReject: onReject, tenants: map[string]*sync.Mutex{}}
}

func (g *Gate) lockFor(t string) *sync.Mutex {
	g.mu.Lock()
	defer g.mu.Unlock()
	m, ok := g.tenants[t]
	if !ok {
		m = &sync.Mutex{}
		g.tenants[t] = m
	}
	return m
}

// Admit checks every limited tenant in keys, holding each one's mutex (taken
// in sorted order, so two batches never deadlock) through fn: a tenant's
// admission and the append it admits are one step, exact under concurrent
// pushes. A tenant with no limit takes no lock and is not counted.
func (g *Gate) Admit(keys map[string][]uint64, fn func() error) error {
	var limited []string
	for t := range keys {
		if g.limit(t) > 0 {
			limited = append(limited, t)
		}
	}
	sort.Strings(limited)
	for _, t := range limited {
		m := g.lockFor(t)
		m.Lock()
		defer m.Unlock()
	}
	for _, t := range limited {
		limit := g.limit(t)
		seen := map[uint64]struct{}{}
		fresh := 0
		for _, id := range keys[t] {
			if _, dup := seen[id]; dup {
				continue
			}
			seen[id] = struct{}{}
			if !g.c.Has(id) {
				fresh++
			}
		}
		if fresh > 0 && g.c.Count(t)+fresh > limit {
			if g.onReject != nil {
				g.onReject(t)
			}
			return &LimitError{Tenant: t, Kind: g.kind, Limit: limit}
		}
	}
	return fn()
}
