package k8s1321

import (
	"testing"

	"github.com/shubhaankar/synctest/explorer"
)

// TestKubernetes1321 explores the k8s#1321 deadlock.
// NOT DETECTED: requires preemption between Watch() and Stop() —
// both use uncontested locks (no yield), so the loop goroutine
// never interleaves. Needs instruction-level scheduling granularity.
func TestKubernetes1321(t *testing.T) {
	explorer.Test(t, Workload)
}
