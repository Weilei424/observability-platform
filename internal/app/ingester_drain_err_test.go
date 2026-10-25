package app

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/logs"
)

// The shutdown drain reports what the logs flush hook cannot: a metrics
// failure, a logs flush that never got the head lock, or lines left behind.
// A logs flush failure the hook already logged is not reported twice, and
// after any failed flush the head is not asked whether it is empty: that takes
// the lock without a deadline.
func TestIngesterDrainErr(t *testing.T) {
	lockWait := fmt.Errorf("%w: %w", logs.ErrLockWait, errors.New("context deadline exceeded"))
	hooked := errors.New("store unavailable") // a sink failure the hook logged
	for _, tc := range []struct {
		name               string
		metricsErr, logErr error
		logsEmpty          bool
		want               string // substring; "" means nil
	}{
		{"clean", nil, nil, true, ""},
		{"metrics failed", errors.New("boom"), nil, true, "metrics: boom"},
		{"logs never got the lock", nil, lockWait, false, "logs:"},
		{"both failed", errors.New("boom"), lockWait, false, "metrics: boom"},
		{"logs sink failure already logged by the hook", nil, hooked, false, ""},
		{"lines left behind", nil, nil, false, "head not empty"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			empty := func() bool {
				if tc.logErr != nil {
					t.Fatal("asked whether the head is empty after a failed flush; that waits on the lock again")
				}
				return tc.logsEmpty
			}
			err := ingesterDrainErr(tc.metricsErr, tc.logErr, empty)
			switch {
			case tc.want == "" && err != nil:
				t.Fatalf("err = %v, want nil", err)
			case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)):
				t.Fatalf("err = %v, want it to contain %q", err, tc.want)
			}
			if tc.name == "both failed" && !errors.Is(err, logs.ErrLockWait) {
				t.Fatalf("err = %v, want the lock wait kept beside the metrics failure", err)
			}
		})
	}
}
