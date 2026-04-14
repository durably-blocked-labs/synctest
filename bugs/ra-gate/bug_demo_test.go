package ragate

import (
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

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

func addGateNodes(orch *orchestrator.Orchestrator, addrs []string, transports map[string]*OrchestratorTransport, store *KVStore, inCS *atomic.Int32, violated *atomic.Bool) {
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
			node.OnViolation = func(msg string) {
				if violated != nil {
					violated.Store(true)
				}
				t.Errorf("protocol violation: %s", msg)
			}
			node.Start()

			node.AcquireLock()
			if inCS != nil {
				if v := inCS.Add(1); v > 1 {
					t.Errorf("mutual exclusion violated on %s: %d in CS", addr, v)
				}
			}
			store.Put("counter", store.Get("counter")+1)
			if inCS != nil {
				inCS.Add(-1)
			}
			node.ReleaseLock()

			// Don't Stop() or Close() here. The handler must stay alive to
			// reply to late-arriving requests from peers that haven't acquired
			// the lock yet. The orchestrator's drain logic shuts down transports
			// when all nodes are idle and no messages are pending.
		})
	}
}

// TestGateBug_FIFOPasses verifies that under FIFO delivery the bug is latent.
func TestGateBug_FIFOPasses(t *testing.T) {
	runtime.GOMAXPROCS(4)
	addrs := []string{"A", "B", "C"}
	transports := setupCluster(addrs)
	store := NewKVStore()

	var violated atomic.Bool
	orch := orchestrator.New()
	addGateNodes(orch, addrs, transports, store, nil, &violated)

	_, ok := orch.Run(t)
	if !ok {
		t.Fatal("FIFO run failed")
	}
	if got := store.Get("counter"); got != len(addrs) {
		t.Errorf("lost update: counter=%d, want %d", got, len(addrs))
	}
	t.Log("FIFO: PASSED (bug is latent)")
}

// TestGateBug_ExploreGlobalOnly explores global delivery orderings only.
// Some G-only orderings also trigger protocol violations (not just the gate
// bug which needs an L decision). This test verifies exploration completes
// without hanging — previously it would hang on deadlocked bubbles.
func TestGateBug_ExploreGlobalOnly(t *testing.T) {
	runtime.GOMAXPROCS(4)
	addrs := []string{"A", "B", "C"}

	var inCS atomic.Int32
	var violated atomic.Bool
	orch := orchestrator.New()
	ok := orch.Explore(t, func(o *orchestrator.Orchestrator) {
		transports := setupCluster(addrs)
		store := NewKVStore()
		addGateNodes(o, addrs, transports, store, &inCS, &violated)
	}, orchestrator.GlobalBound(2), orchestrator.GlobalMaxRuns(200))

	if ok {
		t.Log("G-only Explore: PASSED all interleavings")
	} else {
		t.Log("G-only Explore: found violation via delivery reordering")
	}
}

// TestGateBug_ExploreAll uses G+L exploration to find the mutual exclusion bug.
// The bug needs 1 global (message reorder) + 1 local (goroutine schedule) decision.
func TestGateBug_ExploreAll(t *testing.T) {
	runtime.GOMAXPROCS(4)
	addrs := []string{"A", "B", "C"}

	var inCS atomic.Int32
	var violated atomic.Bool
	orch := orchestrator.New()
	ok := orch.ExploreAll(t, func(o *orchestrator.Orchestrator) {
		transports := setupCluster(addrs)
		store := NewKVStore()
		addGateNodes(o, addrs, transports, store, &inCS, &violated)
	}, orchestrator.GlobalBound(2), orchestrator.GlobalMaxRuns(500))

	if ok {
		t.Log("ExploreAll: did not find bug within max runs")
	} else {
		t.Log("ExploreAll: FOUND mutual exclusion violation (G+L bug)")
	}
}

// TestGateBug_FindBug uses RunWith() with a reverse-FIFO decide function
// and logs decision details to understand why the bug does or doesn't trigger.
func TestGateBug_FindBug(t *testing.T) {
	runtime.GOMAXPROCS(4)
	addrs := []string{"A", "B", "C"}

	var inCS atomic.Int32
	store := NewKVStore()
	orch := orchestrator.New()
	transports := setupCluster(addrs)
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

			node.AcquireLock()
			if v := inCS.Add(1); v > 1 {
				t.Errorf("mutual exclusion violated on %s: %d in CS", addr, v)
			}
			store.Put("counter", store.Get("counter")+1)
			inCS.Add(-1)
			node.ReleaseLock()

			node.Stop()
			tr.Close()
		})
	}

	// Reverse-FIFO: always pick the last goroutine/message.
	rr := orch.RunWith(t, func(dp orchestrator.DecisionPoint) int {
		if dp.N() > 1 {
			return dp.N() - 1
		}
		return 0
	})
	if !rr.Passed {
		t.Log("BUG FOUND: mutual exclusion violation with reverse-FIFO scheduler!")
	} else {
		t.Log("No bug found with reverse-FIFO scheduler")
	}

	// Log local decisions from the unified trace.
	for i, s := range rr.Trace {
		if s.Kind == orchestrator.Local && s.Node == "A" {
			t.Logf("  A step %d: RunqSize=%d Index=%d ChosenBGID=B%d bgids=%v",
				i, s.Alternatives, s.Index, s.ChosenBGID, s.RunqBGIDs)
		}
	}
}

// TestGateBug_PCT uses combined G+L PCT to find the mutual exclusion bug.
func TestGateBug_PCT(t *testing.T) {
	runtime.GOMAXPROCS(4)
	addrs := []string{"A", "B", "C"}

	var inCS atomic.Int32
	var violated atomic.Bool
	orch := orchestrator.New()
	ok := orch.ExplorePCT(t, func(o *orchestrator.Orchestrator) {
		transports := setupCluster(addrs)
		store := NewKVStore()
		addGateNodes(o, addrs, transports, store, &inCS, &violated)
	}, orchestrator.PCTDepth(2), orchestrator.PCTMaxRuns(500), orchestrator.PCTSeed(1))

	if ok {
		t.Log("PCT: did not find bug within 500 runs")
	} else {
		t.Log("PCT: FOUND mutual exclusion violation")
	}
}

// TestGateBug_Random uses combined G+L random scheduling to find the bug.
func TestGateBug_Random(t *testing.T) {
	runtime.GOMAXPROCS(4)
	addrs := []string{"A", "B", "C"}

	var inCS atomic.Int32
	var violated atomic.Bool
	orch := orchestrator.New()
	ok := orch.ExploreRandom(t, func(o *orchestrator.Orchestrator) {
		transports := setupCluster(addrs)
		store := NewKVStore()
		addGateNodes(o, addrs, transports, store, &inCS, &violated)
	}, orchestrator.RandomMaxRuns(500), orchestrator.RandomSeed(1))

	if ok {
		t.Log("Random: did not find bug within 500 runs")
	} else {
		t.Log("Random: FOUND mutual exclusion violation")
	}
}
