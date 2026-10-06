package drain

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
)

type countingMetrics struct{ n int }

func (c *countingMetrics) Append(metrics.Labels, int64, float64) error { c.n++; return nil }

type countingLogs struct{ n int }

func (c *countingLogs) Append(logs.StreamLabels, int64, string) error { c.n++; return nil }

// blockingMetrics holds an append open until release is closed.
type blockingMetrics struct {
	entered chan struct{}
	release chan struct{}
}

func (b *blockingMetrics) Append(metrics.Labels, int64, float64) error {
	close(b.entered)
	<-b.release
	return nil
}

func TestGateRefusesEveryWriteOnceClosed(t *testing.T) {
	g := NewGate()
	m, l := &countingMetrics{}, &countingLogs{}
	gm, gl := g.Metrics(m), g.Logs(l)
	if err := gm.Append(metrics.Labels{}, 1, 1); err != nil {
		t.Fatalf("open gate refused a metric: %v", err)
	}
	if err := gl.Append(logs.StreamLabels{}, 1, "x"); err != nil {
		t.Fatalf("open gate refused a line: %v", err)
	}
	if err := g.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for i := 0; i < 2; i++ { // and stays closed
		if err := gm.Append(metrics.Labels{}, 2, 2); !errors.Is(err, ErrDraining) {
			t.Fatalf("metric after Close: err = %v, want ErrDraining", err)
		}
		if err := gl.Append(logs.StreamLabels{}, 2, "y"); !errors.Is(err, ErrDraining) {
			t.Fatalf("line after Close: err = %v, want ErrDraining", err)
		}
		if err := g.Close(context.Background()); err != nil {
			t.Fatalf("second Close: %v", err)
		}
	}
	if m.n != 1 || l.n != 1 {
		t.Fatalf("appends reached the heads: metrics %d, logs %d; want 1 and 1", m.n, l.n)
	}
}

func TestGateCloseWaitsForAdmittedAppends(t *testing.T) {
	g := NewGate()
	b := &blockingMetrics{entered: make(chan struct{}), release: make(chan struct{})}
	appendDone := make(chan error, 1)
	go func() { appendDone <- g.Metrics(b).Append(metrics.Labels{}, 1, 1) }()
	<-b.entered

	short, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := g.Close(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close with an append in flight: err = %v, want DeadlineExceeded", err)
	}
	if err := g.Metrics(&countingMetrics{}).Append(metrics.Labels{}, 2, 2); !errors.Is(err, ErrDraining) {
		t.Fatalf("a Close that timed out reopened the gate: err = %v", err)
	}

	closed := make(chan error, 1)
	go func() { closed <- g.Close(context.Background()) }()
	select {
	case err := <-closed:
		t.Fatalf("Close returned (%v) before the admitted append finished", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(b.release)
	if err := <-appendDone; err != nil {
		t.Fatalf("admitted append: %v", err)
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return after the admitted append finished")
	}
}
