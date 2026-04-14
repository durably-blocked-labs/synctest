package rastalereply

import (
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

func TestStaleReply_FIFOPasses(t *testing.T) {
	runtime.GOMAXPROCS(4)
	addrs := scenarioAddrs
	transports := setupCluster(addrs)
	store := NewKVStore()

	var inCS atomic.Int32
	orch := orchestrator.New()
	addStaleReplyNodes(orch, addrs, transports, store, &inCS)

	_, ok := orch.Run(t)
	if !ok {
		t.Fatal("FIFO run failed")
	}
	if got := store.Get("counter"); got != activeContenders+1 {
		t.Errorf("lost update: counter=%d, want %d", got, activeContenders+1)
	}
}

func TestStaleReply_ExploreGlobalOnly(t *testing.T) {
	runtime.GOMAXPROCS(4)
	addrs := scenarioAddrs

	var inCS atomic.Int32
	orch := orchestrator.New()
	ok := orch.Explore(t, func(o *orchestrator.Orchestrator) {
		transports := setupCluster(addrs)
		store := NewKVStore()
		addStaleReplyNodes(o, addrs, transports, store, &inCS)
	}, orchestrator.GlobalBound(4), orchestrator.GlobalMaxRuns(500))

	if ok {
		t.Log("G-only Explore: did not find bug within max runs")
	} else {
		t.Log("G-only Explore: FOUND stale-reply violation")
	}
}

func TestStaleReply_ExploreAll(t *testing.T) {
	runtime.GOMAXPROCS(4)
	addrs := scenarioAddrs

	var inCS atomic.Int32
	orch := orchestrator.New()
	ok := orch.ExploreAll(t, func(o *orchestrator.Orchestrator) {
		transports := setupCluster(addrs)
		store := NewKVStore()
		addStaleReplyNodes(o, addrs, transports, store, &inCS)
	}, orchestrator.GlobalBound(4), orchestrator.GlobalMaxRuns(500))

	if ok {
		t.Log("ExploreAll: did not find bug within max runs")
	} else {
		t.Log("ExploreAll: FOUND stale-reply violation")
	}
}

func TestStaleReply_FindBug(t *testing.T) {
	runtime.GOMAXPROCS(4)
	addrs := scenarioAddrs

	var inCS atomic.Int32
	store := NewKVStore()
	orch := orchestrator.New()
	transports := setupCluster(addrs)
	addStaleReplyNodes(orch, addrs, transports, store, &inCS)

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
