package etcdexperiment

import "testing"

func TestControlledIntegrationIsExplicitlyUnsupported(t *testing.T) {
	supported, missing := ControlledIntegrationStatus()
	if supported || len(missing) == 0 {
		t.Fatal("baseline must not silently claim controlled integration")
	}
	t.Logf("controlled integration pending: %v", missing)
}
