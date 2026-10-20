package e2e_test

import (
	"os/exec"
	"strings"
	"testing"
)

// TestBackendRejectsTemplateOwnedTopologyKeys pins the component target, the
// peer URLs, the store replication factor, and each ingester's and store's own URL as the chart's own. Set by hand they would contradict the
// topology the chart renders — the failure TestBackendRejectsHTTPAddrOverride
// guards against for the listen port.
func TestBackendRejectsTemplateOwnedTopologyKeys(t *testing.T) {
	helmAvailable(t)
	for _, key := range []string{"OBS_TARGET", "OBS_INGESTER_URL", "OBS_STORE_URL", "OBS_QUERIER_URL", "OBS_INGESTER_SELF_URL", "OBS_STORE_REPLICATION_FACTOR", "OBS_STORE_SELF_URL",
		"OBS_TENANT_REQUIRED", "OBS_TENANT_OVERRIDES_FILE", "OBS_MAX_ACTIVE_SERIES_PER_TENANT", "OBS_MAX_ACTIVE_STREAMS_PER_TENANT", "OBS_MAX_METRICS_PUSH_BYTES", "OBS_MAX_LOGS_PUSH_BYTES"} {
		out, err := exec.Command("helm", "template", "backend", backendChart,
			"--set-string", "config."+key+"=x").CombinedOutput()
		if err == nil {
			t.Errorf("helm template accepted config.%s; it must fail instead.\nRendered:\n%s", key, out)
			continue
		}
		if !strings.Contains(string(out), key) {
			t.Errorf("the rejection does not name %s, so the operator cannot see which key: %s", key, out)
		}
	}
}
