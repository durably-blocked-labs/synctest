package serving2137

import (
	"testing"

	"github.com/shubhaankar/synctest/explorer"
)

// TestServing2137 explores the serving#2137 deadlock.
// NOT DETECTED: G_A always enters Maybe() before G_B (created first, continues
// after WaitGroup.Done() without yielding). The deadlock requires G_B to acquire
// the activeRequests slot first. Needs instruction-level scheduling granularity.
func TestServing2137(t *testing.T) {
	explorer.Test(t, Workload)
}
