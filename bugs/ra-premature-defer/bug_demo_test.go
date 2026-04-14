package raprematuredefer

import (
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

func TestPrematureDefer_FIFOPasses(t *testing.T) {
	runtime.GOMAXPROCS(4)
	addrs := scenarioAddrs
	transports := setupCluster(addrs)
	store := NewKVStore()

	var violated atomic.Bool
	orch := orchestrator.New()
	addPrematureDeferNodes(orch, addrs, transports, store, nil, &violated)

	_, ok := orch.Run(t)
	if !ok {
		t.Fatal("FIFO run failed")
	}
	if got := store.Get("counter"); got != activeContenders+1 {
		t.Errorf("lost update: counter=%d, want %d", got, activeContenders+1)
	}
	t.Log("FIFO: PASSED (bug is latent)")
}

func TestPrematureDefer_ExploreGlobalOnly(t *testing.T) {
	runtime.GOMAXPROCS(4)
	addrs := scenarioAddrs

	var inCS atomic.Int32
	var violated atomic.Bool
	orch := orchestrator.New()
	ok := orch.Explore(t, func(o *orchestrator.Orchestrator) {
		transports := setupCluster(addrs)
		store := NewKVStore()
		addPrematureDeferNodes(o, addrs, transports, store, &inCS, &violated)
	}, orchestrator.GlobalBound(4), orchestrator.GlobalMaxRuns(500))

	if ok {
		t.Log("G-only Explore: did not find bug within max runs")
	} else {
		t.Log("G-only Explore: FOUND violation via delivery reordering")
	}
}

func TestPrematureDefer_ExploreAll(t *testing.T) {
	runtime.GOMAXPROCS(4)
	addrs := scenarioAddrs

	var inCS atomic.Int32
	var violated atomic.Bool
	orch := orchestrator.New()
	ok := orch.ExploreAll(t, func(o *orchestrator.Orchestrator) {
		transports := setupCluster(addrs)
		store := NewKVStore()
		addPrematureDeferNodes(o, addrs, transports, store, &inCS, &violated)
	}, orchestrator.GlobalBound(4), orchestrator.GlobalMaxRuns(500))

	if ok {
		t.Log("ExploreAll: did not find bug within max runs")
	} else {
		t.Log("ExploreAll: FOUND mutual exclusion violation")
	}
}

func TestPrematureDefer_FindBug(t *testing.T) {
	runtime.GOMAXPROCS(4)
	addrs := scenarioAddrs

	var inCS atomic.Int32
	store := NewKVStore()
	orch := orchestrator.New()
	transports := setupCluster(addrs)
	addPrematureDeferNodes(orch, addrs, transports, store, &inCS, nil)

	rr := orch.RunWith(t, func(dp orchestrator.DecisionPoint) int {
		if dp.Kind == orchestrator.Global && dp.N() > 1 {
			for i, alt := range dp.Alts {
				if alt.From == "C" && alt.To == "B" && alt.MsgType == "Start" {
					return i
				}
			}
			for i, alt := range dp.Alts {
				if alt.From == "B" && alt.To == "C" && alt.MsgType == "Arm" {
					return i
				}
			}
			for i, alt := range dp.Alts {
				if alt.From == "A" && alt.To == "B" && alt.MsgType == "Start" {
					return i
				}
			}
			for i, alt := range dp.Alts {
				if alt.To == "A" && alt.From != "A" && alt.MsgType == "Request" {
					return i
				}
			}
			for i := len(dp.Alts) - 1; i >= 0; i-- {
				alt := dp.Alts[i]
				if alt.To == "A" && alt.MsgType == "Reply" {
					return i
				}
			}
		}
		return 0
	})
	if !rr.Passed {
		t.Log("BUG FOUND: mutual exclusion violation with reordered delivery")
	} else {
		t.Log("No bug found with targeted global scheduler")
	}

	for i, s := range rr.Trace {
		if s.Kind == orchestrator.Global && s.Index > 0 {
			t.Logf("  Global step %d: %s→%s(%s) [index %d of %d]",
				i, s.From, s.To, s.MsgType, s.Index, s.Alternatives)
		}
	}
}
