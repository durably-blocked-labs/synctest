package quorumreadrepair

import (
	"runtime"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

func TestQuorumReadRepair_FIFOPasses(t *testing.T) {
	runtime.GOMAXPROCS(8)
	resetObservedOutcome()

	orch := orchestrator.New()
	addQuorumReadRepairScenario(orch, false)

	if _, ok := orch.Run(t); !ok {
		t.Fatal("FIFO run failed")
	}
	if observedBugFound() {
		t.Fatalf("FIFO should preserve siblings, got %v", lastObservedValues())
	}
}
