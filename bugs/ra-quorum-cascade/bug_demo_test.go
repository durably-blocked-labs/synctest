package raquorumcascade

import (
	"runtime"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

func TestQuorumCascade_FIFOPasses(t *testing.T) {
	runtime.GOMAXPROCS(8)
	resetObservedOutcome()

	orch := orchestrator.New()
	addrs := scenarioAddrs
	transports := setupClusterWithKV(addrs)
	addKVNode(orch, transports)
	addQuorumCascadeNodes(orch, addrs, transports)

	if _, ok := orch.Run(t); !ok {
		t.Fatal("FIFO run failed")
	}
	t.Logf("FIFO counter=%d bug=%v", lastObservedCounter(), observedBugFound())
}

func TestQuorumCascade_BenchFIFOPasses(t *testing.T) {
	runtime.GOMAXPROCS(8)
	resetObservedOutcome()

	orch := orchestrator.New()
	benchSetup(orch)

	if _, ok := orch.Run(t); !ok {
		t.Fatal("benchmark setup should not fail on the canonical FIFO run")
	}
}

func TestQuorumCascade_ExploreGlobalOnly(t *testing.T) {
	runtime.GOMAXPROCS(8)
	resetObservedOutcome()

	orch := orchestrator.New()
	ok := orch.Explore(t, func(o *orchestrator.Orchestrator) {
		addrs := scenarioAddrs
		transports := setupClusterWithKV(addrs)
		addKVNode(o, transports)
		addQuorumCascadeNodesChecked(o, addrs, transports)
	}, orchestrator.GlobalBound(3), orchestrator.GlobalMaxRuns(1000))

	t.Logf("G-only Explore: ok=%v last_counter=%d bug=%v", ok, lastObservedCounter(), observedBugFound())
}

func TestQuorumCascade_ExploreAll(t *testing.T) {
	runtime.GOMAXPROCS(8)
	resetObservedOutcome()

	orch := orchestrator.New()
	ok := orch.ExploreAll(t, func(o *orchestrator.Orchestrator) {
		addrs := scenarioAddrs
		transports := setupClusterWithKV(addrs)
		addKVNode(o, transports)
		addQuorumCascadeNodesChecked(o, addrs, transports)
	}, orchestrator.GlobalBound(4), orchestrator.GlobalMaxRuns(2000))

	t.Logf("ExploreAll: ok=%v last_counter=%d bug=%v", ok, lastObservedCounter(), observedBugFound())
}

func TestQuorumCascade_FindBug(t *testing.T) {
	runtime.GOMAXPROCS(8)
	resetObservedOutcome()

	addrs := scenarioAddrs
	transports := setupClusterWithKV(addrs)
	orch := orchestrator.New()
	addKVNode(orch, transports)
	addQuorumCascadeNodes(orch, addrs, transports)

	rr := orch.RunWith(t, func(dp orchestrator.DecisionPoint) int {
		if dp.Kind == orchestrator.Global && dp.N() > 1 {
			for i := range dp.Alts {
				alt := dp.Alts[i]
				if alt.MsgType == "Control" && alt.From == "A" && alt.To == "C" {
					return i
				}
			}
			for i := range dp.Alts {
				alt := dp.Alts[i]
				if alt.MsgType == "Request" && alt.From == "C" && alt.To == "B" {
					return i
				}
			}
			for i := range dp.Alts {
				alt := dp.Alts[i]
				if alt.MsgType == "Reply" && alt.From == "A" && alt.To == "B" {
					return i
				}
			}
		}
		return 0
	})
	if !observedBugFound() {
		t.Fatalf("expected targeted schedule to expose quorum-cascade bug, got passed=%v counter=%d", rr.Passed, lastObservedCounter())
	}
	t.Logf("FindBug: passed=%v last_counter=%d bug=%v", rr.Passed, lastObservedCounter(), observedBugFound())
}
