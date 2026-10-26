package resourcegc

import (
	"os"
	"runtime"
	"runtime/debug"
	"testing"
	"testing/synctest"
)

// A distinct process-isolated qualification of direct in-bubble GC. The main
// resource-lifecycle experiment requests collections from a spare host P.
func TestGCInsideBubbleOwnershipQualification(t *testing.T) {
	if os.Getenv("RESOURCEGC_OWNERSHIP_PROBE") != "direct" {
		t.Skip("explicit one-shot ownership probe required")
	}
	runtime.GOMAXPROCS(2)
	debug.SetGCPercent(-1)
	completed := false
	r := synctest.ExploreResult(t, func(*testing.T) {
		runtime.GC()
		completed = true
	}, nil)
	if !r.OK || r.WorkerUnsafe || !completed {
		t.Fatalf("direct GC ownership failed: OK=%v unsafe=%v completed=%v cause=%v", r.OK, r.WorkerUnsafe, completed, r.Cause)
	}
}
