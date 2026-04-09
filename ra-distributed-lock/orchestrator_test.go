package lock

import (
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

// makeOrchestratorCluster builds transports and wires them together.
// Returns transports in addr order and a setup func for Explore.
func setupOrchestratorCluster(addrs []string) map[string]*OrchestratorTransport {
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

// addNodesToOrch registers each address as a node on the orchestrator.
// Each node's critical section does a read-modify-write on store.
// inCS is optional: when non-nil, it is incremented on CS entry and
// decremented on CS exit so callers can detect simultaneous entry.
func addNodesToOrch(orch *orchestrator.Orchestrator, addrs []string, transports map[string]*OrchestratorTransport, store *KVStore, inCS *atomic.Int32) {
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
			node := NewRANode(addr, tr, peers)
			node.Start()

			node.AcquireLock()
			if inCS != nil {
				if v := inCS.Add(1); v > 1 {
					t.Errorf("mutual exclusion violated on %s: %d nodes in CS simultaneously", addr, v)
				}
			}
			store.Put("counter", store.Get("counter")+1)
			if inCS != nil {
				inCS.Add(-1)
			}
			node.ReleaseLock()

			node.Stop()
			tr.Close()
		})
	}
}

func TestOrchestratorMutualExclusion(t *testing.T) {
	runtime.GOMAXPROCS(4) // bubble P + orchestrator P + spare
	addrs := []string{"A", "B", "C"}
	transports := setupOrchestratorCluster(addrs)
	store := NewKVStore()

	orch := orchestrator.New()
	addNodesToOrch(orch, addrs, transports, store, nil)

	_, ok := orch.Run(t)
	if !ok {
		t.Fatal("orchestrator run failed")
	}
	if got := store.Get("counter"); got != len(addrs) {
		t.Errorf("lost update: expected counter=%d, got %d", len(addrs), got)
	}
}

func TestOrchestratorExplore(t *testing.T) {
	runtime.GOMAXPROCS(4)
	addrs := []string{"A", "B", "C"}

	var inCS atomic.Int32

	orch := orchestrator.New()
	ok := orch.Explore(t, func(o *orchestrator.Orchestrator) {
		transports := setupOrchestratorCluster(addrs)
		store := NewKVStore()
		addNodesToOrch(o, addrs, transports, store, &inCS)
	}, orchestrator.GlobalBound(2), orchestrator.GlobalMaxRuns(50))

	if !ok {
		t.Fatal("found mutual exclusion violation under message reordering")
	}
}
