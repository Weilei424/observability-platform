package e2e_test

import (
	"os/exec"
	"strings"
	"testing"
)

// TestBackendRejectsTemplateOwnedTopologyKeys pins the component target and
// the peer URLs as the chart's own. Set by hand they would contradict the
// topology the chart renders — the failure TestBackendRejectsHTTPAddrOverride
// guards against for the listen port.
func TestBackendRejectsTemplateOwnedTopologyKeys(t *testing.T) {
	helmAvailable(t)
	for _, key := range []string{"OBS_TARGET", "OBS_INGESTER_URL", "OBS_STORE_URL", "OBS_QUERIER_URL"} {
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
