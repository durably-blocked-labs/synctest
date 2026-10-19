package ragate

import (
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

const kvAddr = "KV"

func setupCluster(addrs []string) map[string]*OrchestratorTransport {
	transports := make(map[string]*OrchestratorTransport, len(addrs))
	for _, addr := range addrs {
		transports[addr] = NewOrchestratorTransport(addr)
	}
	for _, tr := range transports {
		for _, peer := range transports {
			if peer != tr {
				tr.Connect(peer)
			}
		}
	}
	return transports
}

func setupClusterWithKV(addrs []string) map[string]*OrchestratorTransport {
	allAddrs := make([]string, 0, len(addrs)+1)
	allAddrs = append(allAddrs, kvAddr)
	allAddrs = append(allAddrs, addrs...)
	return setupCluster(allAddrs)
}

func addKVNode(orch *orchestrator.Orchestrator, transports map[string]*OrchestratorTransport) {
	kvTr := transports[kvAddr]
	orch.AddNode(kvTr, func(t *testing.T) {
		kvTr.StartBridge()
		store := NewNetworkKVStore(kvTr)
		store.Serve()
	})
}

func addGateNodes(orch *orchestrator.Orchestrator, addrs []string, transports map[string]*OrchestratorTransport) {
	expected := len(addrs)
	var done atomic.Int32
	for i, addr := range addrs {
		addr := addr
		tr := transports[addr]

		peers := make([]string, 0, len(addrs)-1)
		for j, a := range addrs {
			if j != i {
				peers = append(peers, a)
			}
		}

		orch.AddNode(tr, func(t *testing.T) {
			tr.StartBridge()
			node := NewGateRANode(addr, tr, peers)
			node.Start()

			kv := NewKVClient(tr, kvAddr)
			node.AcquireLock()
			val := kv.Get("counter")
			kv.Put("counter", val+1)
			node.ReleaseLock()

			if int(done.Add(1)) == expected {
				final := kv.Get("counter")
				if final != expected {
					t.Errorf("lost update: counter=%d, want %d", final, expected)
				}
			}
		})
	}
}

// TestGateBug_FIFOPasses verifies that under FIFO delivery the bug is latent.
func TestGateBug_FIFOPasses(t *testing.T) {
	runtime.GOMAXPROCS(8)
	addrs := []string{"A", "B", "C"}
	transports := setupClusterWithKV(addrs)

	orch := orchestrator.New()
	addKVNode(orch, transports)
	addGateNodes(orch, addrs, transports)

	_, ok := orch.Run(t)
	if !ok {
		t.Fatal("FIFO run failed")
	}
	t.Log("FIFO: PASSED (bug is latent)")
}

// TestGateBug_ExploreGlobalOnly explores global delivery orderings only.
func TestGateBug_ExploreGlobalOnly(t *testing.T) {
	runtime.GOMAXPROCS(8)
	addrs := []string{"A", "B", "C"}

	orch := orchestrator.New()
	ok := orch.Explore(t, func(o *orchestrator.Orchestrator) {
		transports := setupClusterWithKV(addrs)
		addKVNode(o, transports)
		addGateNodes(o, addrs, transports)
	}, orchestrator.GlobalBound(3), orchestrator.GlobalMaxRuns(200))

	if ok {
		t.Log("G-only Explore: PASSED all interleavings")
	} else {
		t.Log("G-only Explore: found violation via delivery reordering")
	}
}

// TestGateBug_ExploreAll uses G+L exploration to find the mutual exclusion bug.
// The bug needs 1 global (message reorder) + 1 local (goroutine schedule) decision,
// plus KV message reordering for the lost update to manifest in the counter.
func TestGateBug_ExploreAll(t *testing.T) {
	runtime.GOMAXPROCS(8)
	addrs := []string{"A", "B", "C"}

	orch := orchestrator.New()
	ok := orch.ExploreAll(t, func(o *orchestrator.Orchestrator) {
		transports := setupClusterWithKV(addrs)
		addKVNode(o, transports)
		addGateNodes(o, addrs, transports)
	}, orchestrator.GlobalBound(3), orchestrator.GlobalMaxRuns(500))

	if ok {
		t.Log("ExploreAll: did not find bug within max runs")
	} else {
		t.Log("ExploreAll: FOUND lost-update bug (black-box detection)")
	}
}

// TestGateBug_FindBug uses RunWith() with a reverse-FIFO decide function.
func TestGateBug_FindBug(t *testing.T) {
	runtime.GOMAXPROCS(8)
	addrs := []string{"A", "B", "C"}

	transports := setupClusterWithKV(addrs)
	orch := orchestrator.New()
	addKVNode(orch, transports)
	addGateNodes(orch, addrs, transports)

	// Reverse-FIFO: always pick the last goroutine/message.
	rr := orch.RunWith(t, func(dp orchestrator.DecisionPoint) int {
		if dp.N() > 1 {
			return dp.N() - 1
		}
		return 0
	})
	if !rr.Passed {
		t.Log("BUG FOUND: lost update with reverse-FIFO scheduler!")
	} else {
		t.Log("No bug found with reverse-FIFO scheduler")
	}

	for i, s := range rr.Trace {
		if s.Kind == orchestrator.Local && s.Node == "A" {
			t.Logf("  A step %d: RunqSize=%d Index=%d ChosenBGID=B%d bgids=%v",
				i, s.Alternatives, s.Index, s.ChosenBGID, s.RunqBGIDs)
		}
	}
}

// TestGateBug_PCT uses combined G+L PCT to find the bug.
func TestGateBug_PCT(t *testing.T) {
	runtime.GOMAXPROCS(8)
	addrs := []string{"A", "B", "C"}

	orch := orchestrator.New()
	ok := orch.ExplorePCT(t, func(o *orchestrator.Orchestrator) {
		transports := setupClusterWithKV(addrs)
		addKVNode(o, transports)
		addGateNodes(o, addrs, transports)
	}, orchestrator.PCTDepth(3), orchestrator.PCTMaxRuns(500), orchestrator.PCTSeed(1))

	if ok {
		t.Log("PCT: did not find bug within 500 runs")
	} else {
		t.Log("PCT: FOUND lost-update bug")
	}
}

// TestGateBug_Random uses combined G+L random scheduling to find the bug.
func TestGateBug_Random(t *testing.T) {
	runtime.GOMAXPROCS(8)
	addrs := []string{"A", "B", "C"}

	orch := orchestrator.New()
	ok := orch.ExploreRandom(t, func(o *orchestrator.Orchestrator) {
		transports := setupClusterWithKV(addrs)
		addKVNode(o, transports)
		addGateNodes(o, addrs, transports)
	}, orchestrator.RandomMaxRuns(500), orchestrator.RandomSeed(1))

	if ok {
		t.Log("Random: did not find bug within 500 runs")
	} else {
		t.Log("Random: FOUND lost-update bug")
	}
}
