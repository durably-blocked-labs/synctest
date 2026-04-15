package raduplicaterequest

import (
	"runtime"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

func TestDuplicateRequest_FIFOPasses(t *testing.T) {
	runtime.GOMAXPROCS(8)
	addrs := scenarioAddrs
	transports := setupClusterWithKV(addrs)

	orch := orchestrator.New()
	addKVNode(orch, transports)
	addDuplicateRequestNodes(orch, addrs, transports)

	_, ok := orch.Run(t)
	if !ok {
		t.Fatal("FIFO run failed")
	}
}

func TestDuplicateRequest_ExploreGlobalOnly(t *testing.T) {
	runtime.GOMAXPROCS(8)
	addrs := scenarioAddrs

	algo := &orchestrator.CHESS{Bound: 3, GlobalOnly: true}
	orch := orchestrator.New()
	orch.ExploreWith(t, func(o *orchestrator.Orchestrator) {
		transports := setupClusterWithKV(addrs)
		addKVNode(o, transports)
		addDuplicateRequestNodes(o, addrs, transports)
	}, algo, orchestrator.GlobalMaxRuns(500))
}

func TestDuplicateRequest_ExploreAll(t *testing.T) {
	runtime.GOMAXPROCS(8)
	addrs := scenarioAddrs

	algo := &orchestrator.CHESS{Bound: 3}
	orch := orchestrator.New()
	orch.ExploreWith(t, func(o *orchestrator.Orchestrator) {
		transports := setupClusterWithKV(addrs)
		addKVNode(o, transports)
		addDuplicateRequestNodes(o, addrs, transports)
	}, algo, orchestrator.GlobalMaxRuns(500))
}

func TestDuplicateRequest_FindBug(t *testing.T) {
	runtime.GOMAXPROCS(8)
	addrs := scenarioAddrs

	algo := &orchestrator.Random{Seed: 1}
	orch := orchestrator.New()
	orch.ExploreWith(t, func(o *orchestrator.Orchestrator) {
		transports := setupClusterWithKV(addrs)
		addKVNode(o, transports)
		addDuplicateRequestNodes(o, addrs, transports)
	}, algo, orchestrator.GlobalMaxRuns(500))
}
