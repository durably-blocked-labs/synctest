//go:build bubbles_gc_liveness_qualification

package resourcegc

import (
	"os"
	"runtime"
	"runtime/debug"
	"testing"
	"testing/synctest"
)

// This worker is deliberately process-terminal. The external runner requires
// the spare-host configuration failure, rather than allowing a silent hang.
func TestGCWithoutSpareHostFailsClosed(t *testing.T) {
	if os.Getenv("RESOURCEGC_NO_HOST") != "1" {
		t.Skip("explicit terminal qualification only")
	}
	runtime.GOMAXPROCS(1)
	debug.SetGCPercent(-1)
	synctest.ExploreResult(t, func(*testing.T) { runtime.GC() }, nil)
	t.Fatal("GC with no eligible host processor unexpectedly returned")
}
