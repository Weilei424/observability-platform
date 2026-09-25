package metrics

import (
	"testing"

	"github.com/masonwheeler/observability-platform/internal/storage/wal"
)

// WALStore must accept any head that can flush, not only a BlockStore: the
// ingester fronts a head whose flushes go to another process. SealedChunkCount
// is what the maintenance loop reads when there is no block manager to ask.
func TestWALStoreFrontsAnyHead(t *testing.T) {
	var _ walHead = (*BlockStore)(nil)
	bs, err := NewBlockStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	ws := NewWALStore(nopRecordWriter{}, bs, t.TempDir())
	l, _ := NewLabels(map[string]string{"__name__": "sealed"})
	for i := range 121 {
		if err := ws.Append(l, int64(i)*1000, 1); err != nil {
			t.Fatal(err)
		}
	}
	if got := ws.SealedChunkCount(); got != 1 {
		t.Fatalf("SealedChunkCount = %d, want 1", got)
	}
}

type nopRecordWriter struct{}

func (nopRecordWriter) WriteRecord([]wal.LabelPair, int64, float64) error { return nil }
func (nopRecordWriter) SegmentIndex() int                                 { return 0 }
