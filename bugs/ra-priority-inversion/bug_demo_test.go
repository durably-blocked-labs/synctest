package rapriorityinversion

import (
	"runtime"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

func TestPriorityInversion_FIFOPasses(t *testing.T) {
	runtime.GOMAXPROCS(8)
	addrs := scenarioAddrs
	transports := setupClusterWithKV(addrs)

	orch := orchestrator.New()
	addKVNode(orch, transports)
	addPriorityInversionNodes(orch, addrs, transports)

	_, ok := orch.Run(t)
	if !ok {
		t.Fatal("FIFO run failed")
	}
}

func TestPriorityInversion_ExploreGlobalOnly(t *testing.T) {
	runtime.GOMAXPROCS(8)
	addrs := scenarioAddrs

	orch := orchestrator.New()
	ok := orch.Explore(t, func(o *orchestrator.Orchestrator) {
		transports := setupClusterWithKV(addrs)
		addKVNode(o, transports)
		addPriorityInversionNodesChecked(o, addrs, transports)
	}, orchestrator.GlobalBound(3), orchestrator.GlobalMaxRuns(500))

	if ok {
		t.Log("G-only Explore: did not find bug within max runs")
	} else {
		t.Log("G-only Explore: FOUND priority-inversion violation")
	}
}

func TestPriorityInversion_FindBug(t *testing.T) {
	runtime.GOMAXPROCS(8)
	addrs := scenarioAddrs

	transports := setupClusterWithKV(addrs)
	orch := orchestrator.New()
	addKVNode(orch, transports)
	addPriorityInversionNodesChecked(orch, addrs, transports)

	rr := orch.RunWith(t, func(dp orchestrator.DecisionPoint) int {
		if dp.Kind == orchestrator.Global && dp.N() > 1 {
			want := []struct {
				from    string
				to      string
				msgType string
			}{
				{"A", "B", "Control"},
				{"B", "A", "Control"},
				{"A", "E", "Control"},
				{"E", "A", "Request"},
				{"B", "A", "Request"},
				{"A", "E", "Reply"},
			}
			for _, step := range want {
				for i, alt := range dp.Alts {
					if alt.From == step.from && alt.To == step.to && alt.MsgType == step.msgType {
						return i
					}
				}
			}
		}
		return 0
	})
	if rr.Passed {
		t.Fatal("expected targeted scheduler to expose priority inversion")
	}
}
