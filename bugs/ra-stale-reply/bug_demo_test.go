package rastalereply

import (
	"runtime"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

func TestStaleReply_FIFOPasses(t *testing.T) {
	runtime.GOMAXPROCS(8)
	addrs := scenarioAddrs
	transports := setupClusterWithKV(addrs)

	orch := orchestrator.New()
	addKVNode(orch, transports)
	addStaleReplyNodes(orch, addrs, transports)

	_, ok := orch.Run(t)
	if !ok {
		t.Fatal("FIFO run failed")
	}
}

func TestStaleReply_ExploreGlobalOnly(t *testing.T) {
	runtime.GOMAXPROCS(8)
	addrs := scenarioAddrs

	orch := orchestrator.New()
	ok := orch.Explore(t, func(o *orchestrator.Orchestrator) {
		transports := setupClusterWithKV(addrs)
		addKVNode(o, transports)
		addStaleReplyNodes(o, addrs, transports)
	}, orchestrator.GlobalBound(4), orchestrator.GlobalMaxRuns(500))

	if ok {
		t.Log("G-only Explore: did not find bug within max runs")
	} else {
		t.Log("G-only Explore: FOUND stale-reply violation")
	}
}

func TestStaleReply_ExploreAll(t *testing.T) {
	runtime.GOMAXPROCS(8)
	addrs := scenarioAddrs

	orch := orchestrator.New()
	ok := orch.ExploreAll(t, func(o *orchestrator.Orchestrator) {
		transports := setupClusterWithKV(addrs)
		addKVNode(o, transports)
		addStaleReplyNodes(o, addrs, transports)
	}, orchestrator.GlobalBound(6), orchestrator.GlobalMaxRuns(2000))

	if ok {
		t.Log("ExploreAll: did not find bug within max runs")
	} else {
		t.Log("ExploreAll: FOUND stale-reply violation")
	}
}

func TestStaleReply_FindBug(t *testing.T) {
	runtime.GOMAXPROCS(8)
	addrs := scenarioAddrs

	transports := setupClusterWithKV(addrs)
	orch := orchestrator.New()
	addKVNode(orch, transports)
	addStaleReplyNodes(orch, addrs, transports)

	rr := orch.RunWith(t, func(dp orchestrator.DecisionPoint) int {
		if dp.Kind == orchestrator.Global && dp.N() > 1 {
			for i := len(dp.Alts) - 1; i >= 0; i-- {
				alt := dp.Alts[i]
				if alt.From == "B" && alt.To == "A" && alt.MsgType == "Reply" {
					return i
				}
			}
		}
		return 0
	})
	if !rr.Passed {
		t.Log("BUG FOUND: stale reply counted in a later round")
	} else {
		t.Log("No bug found with targeted global scheduler")
	}
}
