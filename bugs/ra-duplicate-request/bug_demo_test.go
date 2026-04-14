package raduplicaterequest

import (
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

func TestDuplicateRequest_FIFOPasses(t *testing.T) {
	runtime.GOMAXPROCS(4)
	addrs := scenarioAddrs
	transports := setupCluster(addrs)
	store := NewKVStore()

	var inCS atomic.Int32
	var violated atomic.Bool
	orch := orchestrator.New()
	addDuplicateRequestNodes(orch, addrs, transports, store, &inCS, &violated, false)

	_, _ = orch.Run(t)
	if violated.Load() {
		t.Fatal("expected FIFO run to avoid duplicate-request violation")
	}
	if got := store.Get("counter"); got != activeContenders {
		t.Errorf("lost update: counter=%d, want %d", got, activeContenders)
	}
}

func TestDuplicateRequest_ExploreGlobalOnly(t *testing.T) {
	runtime.GOMAXPROCS(4)
	addrs := scenarioAddrs

	var violated atomic.Bool
	var inCS atomic.Int32
	algo := &orchestrator.CHESS{Bound: 3, GlobalOnly: true}
	orch := orchestrator.New()
	orch.ExploreWith(t, func(o *orchestrator.Orchestrator) {
		transports := setupCluster(addrs)
		store := NewKVStore()
		addDuplicateRequestNodes(o, addrs, transports, store, &inCS, &violated, false)
	}, algo, orchestrator.GlobalMaxRuns(500))

	if violated.Load() {
		t.Log("global-only exploration found duplicate-request violation")
	} else {
		t.Log("global-only exploration did not find a violation within the current budget")
	}
}

func TestDuplicateRequest_ExploreAll(t *testing.T) {
	runtime.GOMAXPROCS(4)
	addrs := scenarioAddrs

	var violated atomic.Bool
	var inCS atomic.Int32
	algo := &orchestrator.CHESS{Bound: 3}
	orch := orchestrator.New()
	orch.ExploreWith(t, func(o *orchestrator.Orchestrator) {
		transports := setupCluster(addrs)
		store := NewKVStore()
		addDuplicateRequestNodes(o, addrs, transports, store, &inCS, &violated, false)
	}, algo, orchestrator.GlobalMaxRuns(500))

	if violated.Load() {
		t.Log("combined exploration found duplicate-request violation")
	} else {
		t.Log("combined exploration did not find a violation within the current budget")
	}
}

func TestDuplicateRequest_FindBug(t *testing.T) {
	runtime.GOMAXPROCS(4)
	addrs := scenarioAddrs

	var violated atomic.Bool
	var inCS atomic.Int32
	algo := &orchestrator.Random{Seed: 1}
	orch := orchestrator.New()
	orch.ExploreWith(t, func(o *orchestrator.Orchestrator) {
		transports := setupCluster(addrs)
		store := NewKVStore()
		addDuplicateRequestNodes(o, addrs, transports, store, &inCS, &violated, false)
	}, algo, orchestrator.GlobalMaxRuns(500))

	if violated.Load() {
		t.Log("search found duplicate-request bug")
	} else {
		t.Log("search did not find the bug within the current budget")
	}
}
