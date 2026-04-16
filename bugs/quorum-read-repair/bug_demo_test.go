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

func TestQuorumReadRepair_ExploreGlobalOnly(t *testing.T) {
	runtime.GOMAXPROCS(8)
	resetObservedOutcome()

	orch := orchestrator.New()
	ok := orch.Explore(t, func(o *orchestrator.Orchestrator) {
		addQuorumReadRepairScenario(o, false)
	}, orchestrator.GlobalBound(4), orchestrator.GlobalMaxRuns(500))

	t.Logf("G-only Explore: ok=%v bug=%v values=%v", ok, observedBugFound(), lastObservedValues())
}

func TestQuorumReadRepair_ExploreAll(t *testing.T) {
	runtime.GOMAXPROCS(8)
	resetObservedOutcome()

	orch := orchestrator.New()
	ok := orch.ExploreAll(t, func(o *orchestrator.Orchestrator) {
		addQuorumReadRepairScenario(o, false)
	}, orchestrator.GlobalBound(6), orchestrator.GlobalMaxRuns(2000))

	if !observedBugFound() {
		t.Fatalf("ExploreAll did not observe quorum read-repair bug within max runs; ok=%v values=%v", ok, lastObservedValues())
	}
	t.Logf("ExploreAll: FOUND bug values=%v", lastObservedValues())
}

func TestQuorumReadRepair_FindBug(t *testing.T) {
	runtime.GOMAXPROCS(8)
	resetObservedOutcome()

	orch := orchestrator.New()
	addQuorumReadRepairScenario(orch, false)

	rr := orch.RunWith(t, func(dp orchestrator.DecisionPoint) int {
		if dp.Kind == orchestrator.Global && dp.N() > 1 {
			if idx := chooseGlobal(dp, "C1", "R1", "Put"); idx >= 0 {
				return idx
			}
			if idx := chooseGlobal(dp, "C1", "R2", "Put"); idx >= 0 {
				return idx
			}
			if idx := chooseGlobal(dp, "C2", "R2", "Put"); idx >= 0 {
				return idx
			}
			if idx := chooseGlobal(dp, "C2", "R3", "Put"); idx >= 0 {
				return idx
			}
			if idx := chooseGlobal(dp, "R1", "C1", "PutAck"); idx >= 0 {
				return idx
			}
			if idx := chooseGlobal(dp, "R2", "C1", "PutAck"); idx >= 0 {
				return idx
			}
			if idx := chooseGlobal(dp, "R2", "C2", "PutAck"); idx >= 0 {
				return idx
			}
			if idx := chooseGlobal(dp, "R3", "C2", "PutAck"); idx >= 0 {
				return idx
			}
			if idx := chooseGlobal(dp, "C1", "Reader", "Control"); idx >= 0 {
				return idx
			}
			if idx := chooseGlobal(dp, "C2", "Reader", "Control"); idx >= 0 {
				return idx
			}
			if idx := chooseGlobal(dp, "Reader", "R1", "Get"); idx >= 0 {
				return idx
			}
			if idx := chooseGlobal(dp, "Reader", "R2", "Get"); idx >= 0 {
				return idx
			}
			if idx := chooseGlobal(dp, "R1", "Reader", "GetResp"); idx >= 0 {
				return idx
			}
			if idx := chooseGlobal(dp, "R2", "Reader", "GetResp"); idx >= 0 {
				return idx
			}
			if idx := chooseGlobal(dp, "Reader", "R1", "Repair"); idx >= 0 {
				return idx
			}
			if idx := chooseGlobal(dp, "C2", "R1", "Put"); idx >= 0 {
				return idx
			}
		}
		if dp.Kind == orchestrator.Local && dp.N() > 1 && dp.Node == "R1" {
			return dp.N() - 1
		}
		return 0
	})
	if !observedBugFound() {
		t.Fatalf("expected targeted scheduler to expose bug, passed=%v values=%v", rr.Passed, lastObservedValues())
	}
	t.Logf("FindBug: passed=%v values=%v", rr.Passed, lastObservedValues())
}

func chooseGlobal(dp orchestrator.DecisionPoint, from, to, msgType string) int {
	for i := range dp.Alts {
		alt := dp.Alts[i]
		if alt.From == from && alt.To == to && alt.MsgType == msgType {
			return i
		}
	}
	return -1
}
