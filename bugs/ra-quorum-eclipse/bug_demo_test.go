package raquorumeclipse

import (
	"runtime"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

func TestQuorumEclipse_FIFOPasses(t *testing.T) {
	runtime.GOMAXPROCS(8)
	addrs := scenarioAddrs

	transports := setupClusterWithKV(addrs)
	orch := orchestrator.New()
	addKVNode(orch, transports)
	addQuorumEclipseNodes(orch, addrs, transports)

	_, ok := orch.Run(t)
	if !ok {
		t.Fatal("FIFO run failed")
	}
	t.Log("FIFO: PASSED (bug is latent)")
}

func TestQuorumEclipse_ExploreGlobalOnly(t *testing.T) {
	runtime.GOMAXPROCS(8)
	addrs := scenarioAddrs

	orch := orchestrator.New()
	ok := orch.Explore(t, func(o *orchestrator.Orchestrator) {
		transports := setupClusterWithKV(addrs)
		addKVNode(o, transports)
		addQuorumEclipseNodes(o, addrs, transports)
	}, orchestrator.GlobalBound(5), orchestrator.GlobalMaxRuns(50))

	if ok {
		t.Log("G-only Explore: did not find bug within max runs")
	} else {
		t.Log("G-only Explore: FOUND lost-update bug")
	}
}

func TestQuorumEclipse_ExploreAll(t *testing.T) {
	runtime.GOMAXPROCS(8)
	addrs := scenarioAddrs

	orch := orchestrator.New()
	ok := orch.ExploreAll(t, func(o *orchestrator.Orchestrator) {
		transports := setupClusterWithKV(addrs)
		addKVNode(o, transports)
		addQuorumEclipseNodes(o, addrs, transports)
	}, orchestrator.GlobalBound(6), orchestrator.GlobalMaxRuns(400))

	if ok {
		t.Log("ExploreAll: did not find bug within max runs")
	} else {
		t.Log("ExploreAll: FOUND lost-update bug")
	}
}

func TestQuorumEclipse_FindBug(t *testing.T) {
	runtime.GOMAXPROCS(8)
	addrs := scenarioAddrs

	transports := setupClusterWithKV(addrs)
	orch := orchestrator.New()
	addKVNode(orch, transports)
	addQuorumEclipseNodes(orch, addrs, transports)

	pick := func(dp orchestrator.DecisionPoint, from, to, msgType string) int {
		for i, alt := range dp.Alts {
			if alt.From == from && alt.To == to && alt.MsgType == msgType {
				return i
			}
		}
		return -1
	}

	rr := orch.RunWith(t, func(dp orchestrator.DecisionPoint) int {
		if dp.Kind == orchestrator.Global && dp.N() > 1 {
			want := []struct {
				from    string
				to      string
				msgType string
			}{
				{"A", "E", "Request"},
				{"E", "A", "Failed"},
				{"A", "D", "Request"},
				{"D", "A", "Grant"},
				{"A", "C", "Request"},
				{"A", "B", "Control"},
				{"B", "D", "Request"},
				{"B", "C", "Request"},
				{"B", "E", "Request"},
				{"D", "A", "Inquire"},
				{"C", "B", "Failed"},
				{"C", "A", "Grant"},
				{"E", "B", "Grant"},
				{"A", "D", "Relinquish"},
				{"A", "KV", "KVGet"},
				{"D", "B", "Grant"},
			}
			for _, step := range want {
				if i := pick(dp, step.from, step.to, step.msgType); i >= 0 {
					return i
				}
			}
			for i := len(dp.Alts) - 1; i >= 0; i-- {
				alt := dp.Alts[i]
				if alt.From == "D" && alt.To == "A" && alt.MsgType == "Failed" {
					return i
				}
			}
		}
		return 0
	})

	if rr.Passed {
		t.Fatal("expected targeted schedule to expose the quorum eclipse bug")
	}

	for i, s := range rr.Trace {
		if s.Kind == orchestrator.Global && s.Index > 0 {
			t.Logf("  Global step %d: %s->%s(%s) [index %d of %d]",
				i, s.From, s.To, s.MsgType, s.Index, s.Alternatives)
		}
	}
}
