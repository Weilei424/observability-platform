package metrics

// Expr is a parsed query expression node.
type Expr interface{ exprNode() }

// SelectorExpr wraps a raw label Selector.
type SelectorExpr struct{ Selector Selector }

// RateExpr computes per-second rate over a lookback window.
// WindowMs is the window size in milliseconds; must be > 0.
type RateExpr struct {
	Selector Selector
	WindowMs int64
}

// SumExpr aggregates an inner expression.
// By == nil means ungrouped sum (collapses all series to one).
// By != nil means group by those label names.
type SumExpr struct {
	Inner Expr
	By    []string
}

// ScalarExpr is a constant numeric value (e.g. "1+1" from the Grafana health check).
type ScalarExpr struct{ Value float64 }

func (SelectorExpr) exprNode() {}
func (RateExpr) exprNode()     {}
func (SumExpr) exprNode()      {}
func (ScalarExpr) exprNode()   {}
