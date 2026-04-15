package raghostgrant

import (
	"runtime"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

func TestGhostGrant_FIFOPasses(t *testing.T) {
	runtime.GOMAXPROCS(8)
	addrs := scenarioAddrs
	transports := setupClusterWithKV(addrs)

	orch := orchestrator.New()
	addKVNode(orch, transports)
	addGhostGrantNodes(orch, addrs, transports)

	_, ok := orch.Run(t)
	if !ok {
		t.Fatal("FIFO run failed")
	}
}

func TestGhostGrant_ExploreGlobalOnly(t *testing.T) {
	runtime.GOMAXPROCS(8)
	addrs := scenarioAddrs

	orch := orchestrator.New()
	ok := orch.Explore(t, func(o *orchestrator.Orchestrator) {
		transports := setupClusterWithKV(addrs)
		addKVNode(o, transports)
		addGhostGrantNodes(o, addrs, transports)
	}, orchestrator.GlobalBound(3), orchestrator.GlobalMaxRuns(80))

	if ok {
		t.Log("G-only Explore: did not find bug within 80 runs")
	} else {
		t.Log("G-only Explore: FOUND ghost-grant violation")
	}
}

func TestGhostGrant_ExploreAll(t *testing.T) {
	runtime.GOMAXPROCS(8)
	addrs := scenarioAddrs

	orch := orchestrator.New()
	ok := orch.ExploreAll(t, func(o *orchestrator.Orchestrator) {
		transports := setupClusterWithKV(addrs)
		addKVNode(o, transports)
		addGhostGrantNodes(o, addrs, transports)
	}, orchestrator.GlobalBound(3), orchestrator.GlobalMaxRuns(80))

	if ok {
		t.Log("ExploreAll: did not find bug within 80 runs")
	} else {
		t.Log("ExploreAll: FOUND ghost-grant violation")
	}
}

func TestGhostGrant_FindBug(t *testing.T) {
	runtime.GOMAXPROCS(8)
	addrs := scenarioAddrs
	transports := setupClusterWithKV(addrs)

	orch := orchestrator.New()
	addKVNode(orch, transports)
	addGhostGrantNodes(orch, addrs, transports)

	rr := orch.RunWith(t, func(dp orchestrator.DecisionPoint) int {
		if dp.Kind != orchestrator.Global || dp.N() <= 1 {
			return 0
		}

		for i, alt := range dp.Alts {
			if alt.From == "C" && alt.MsgType == "Request" {
				return i
			}
		}
		for i, alt := range dp.Alts {
			if alt.To == "C" && alt.MsgType == "Reply" {
				return i
			}
		}

		joinToA := -1
		for i, alt := range dp.Alts {
			if alt.From == "C" && alt.To == "A" && alt.MsgType == "Join" {
				joinToA = i
				break
			}
		}
		if joinToA >= 0 {
			for i := 0; i < dp.N(); i++ {
				if i != joinToA {
					return i
				}
			}
		}

		return dp.N() - 1
	})
	if rr.Passed {
		t.Fatal("expected targeted global scheduler to expose the ghost-grant bug")
	}
}
