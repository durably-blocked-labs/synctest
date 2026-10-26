package experiments

import (
	"runtime"
	"testing"
)

// fixtureProcessors changes topology only outside a bubble, restoring it after
// the test's joined runs. One bubble needs one owned P and a spare host P.
func fixtureProcessors(t *testing.T, processors int) {
	t.Helper()
	if processors < 2 {
		t.Fatal("bubble fixture requires at least two processors")
	}
	previous := runtime.GOMAXPROCS(processors)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
}
