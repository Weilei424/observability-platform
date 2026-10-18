package compactor

import (
	"github.com/masonwheeler/observability-platform/internal/tenant"
	"log/slog"
	"time"

	"github.com/masonwheeler/observability-platform/internal/storage/block"
)

// MultiBlockManager runs each compaction and retention call against every
// store in turn; one store failing is reported and skipped, and the rest run.
// The observe metric counts calls to each store (compaction and retention),
// not whole passes.
type MultiBlockManager struct {
	names    []string
	managers []BlockManager
	outcome  func(error) string
	observe  func(store, outcome string)
	log      *slog.Logger
}

// NewMultiBlockManager pairs each manager with its store name. outcome maps
// an error to the outcome label (the app passes rpc.OutcomeOf, so this
// package does not import rpc); observe is called once per store per call.
func NewMultiBlockManager(names []string, managers []BlockManager, outcome func(error) string, observe func(store, outcome string), log *slog.Logger) *MultiBlockManager {
	return &MultiBlockManager{names: names, managers: managers, outcome: outcome, observe: observe, log: log}
}

// CompactOnce compacts each store to stable in turn (repeating its CompactOnce
// until it returns 0 or an error), so a failing store never stops the others
// from reaching stable. It sums the plans executed and returns the first
// error after trying all stores. Each store is observed once per call.
func (m *MultiBlockManager) CompactOnce(plan func([]block.BlockInfo) [][]string) (int, error) {
	return m.each("compaction", func(b BlockManager) (int, error) {
		total := 0
		for {
			n, err := b.CompactOnce(plan)
			total += n
			if err != nil || n == 0 {
				return total, err
			}
		}
	})
}

// ApplyRetention sums the blocks deleted across stores and returns the first
// error after trying all of them.
func (m *MultiBlockManager) ApplyRetention(now time.Time, policy tenant.RetentionPolicy) (int, error) {
	return m.each("retention", func(b BlockManager) (int, error) { return b.ApplyRetention(now, policy) })
}

func (m *MultiBlockManager) each(op string, call func(BlockManager) (int, error)) (int, error) {
	total := 0
	var first error
	for i, b := range m.managers {
		n, err := call(b)
		total += n
		if m.observe != nil {
			m.observe(m.names[i], m.outcome(err))
		}
		if err != nil {
			if len(m.managers) > 1 {
				m.log.Warn("store "+op+" failed; skipping it", slog.String("store", m.names[i]), slog.String("error", err.Error()))
			}
			if first == nil {
				first = err
			}
		}
	}
	return total, first
}
