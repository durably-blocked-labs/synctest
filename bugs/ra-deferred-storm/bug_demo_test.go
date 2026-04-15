package radeferredstorm

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

// addStormNodes wires up 4 nodes: A does 2 rounds, B/C/D do 1 round each.
// Total CS entries: 5. The gate bug can fire independently in each of A's
// rounds, requiring depth 4 (2G + 2L) for both to manifest.
func addStormNodes(orch *orchestrator.Orchestrator, addrs []string, transports map[string]*OrchestratorTransport) {
	// A does 2 CS entries, B/C/D do 1 each = 5 total.
	const expected = 5
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
			node := NewStormRANode(addr, tr, peers)
			node.Start()
			kv := NewKVClient(tr, kvAddr)

			checkCounter := func() {
				if int(done.Add(1)) == expected {
					final := kv.Get("counter")
					if final != expected {
						t.Errorf("lost update: counter=%d, want %d", final, expected)
					}
				}
			}

			switch addr {
			case "A":
				// Round 1
				node.AcquireLock()
				val := kv.Get("counter")
				kv.Put("counter", val+1)
				node.ReleaseLock()
				checkCounter()

				// Round 2 — gate bug can fire again with a fresh
				// deferredFlusher goroutine and a new gate channel.
				node.AcquireLock()
				val = kv.Get("counter")
				kv.Put("counter", val+1)
				node.ReleaseLock()
				checkCounter()

			default:
				// B, C, D: single round
				node.AcquireLock()
				val := kv.Get("counter")
				kv.Put("counter", val+1)
				node.ReleaseLock()
				checkCounter()
			}
		})
	}
}

var stormAddrs = []string{"A", "B", "C", "D"}

// TestStorm_FIFOPasses verifies the bug is latent under FIFO delivery.
func TestStorm_FIFOPasses(t *testing.T) {
	runtime.GOMAXPROCS(8)
	transports := setupClusterWithKV(stormAddrs)

	orch := orchestrator.New()
	addKVNode(orch, transports)
	addStormNodes(orch, stormAddrs, transports)

	_, ok := orch.Run(t)
	if !ok {
		t.Fatal("FIFO run failed")
	}
	t.Log("FIFO: PASSED (bug is latent)")
}

// TestStorm_ExploreGlobalOnly explores global delivery orderings only.
// The gate bug needs a local decision, so G-only should NOT find it.
func TestStorm_ExploreGlobalOnly(t *testing.T) {
	runtime.GOMAXPROCS(8)

	orch := orchestrator.New()
	ok := orch.Explore(t, func(o *orchestrator.Orchestrator) {
		transports := setupClusterWithKV(stormAddrs)
		addKVNode(o, transports)
		addStormNodes(o, stormAddrs, transports)
	}, orchestrator.GlobalBound(4), orchestrator.GlobalMaxRuns(500))

	if ok {
		t.Log("G-only: PASSED (expected — bug needs L decision)")
	} else {
		t.Log("G-only: found violation (unexpected for pure gate bug)")
	}
}

// TestStorm_ExploreAll_Bound2 uses bound=2 — enough for ONE round of the
// gate bug but not both. Should find single-round violations (counter=4).
func TestStorm_ExploreAll_Bound2(t *testing.T) {
	runtime.GOMAXPROCS(8)

	orch := orchestrator.New()
	ok := orch.ExploreAll(t, func(o *orchestrator.Orchestrator) {
		transports := setupClusterWithKV(stormAddrs)
		addKVNode(o, transports)
		addStormNodes(o, stormAddrs, transports)
	}, orchestrator.GlobalBound(2), orchestrator.GlobalMaxRuns(1000))

	if ok {
		t.Log("Bound=2: did not find bug")
	} else {
		t.Log("Bound=2: FOUND single-round gate violation")
	}
}

// TestStorm_ExploreAll_Bound4 uses bound=4 — enough for BOTH rounds.
// Can find the double violation (counter=3, two lost updates).
func TestStorm_ExploreAll_Bound4(t *testing.T) {
	runtime.GOMAXPROCS(8)

	orch := orchestrator.New()
	ok := orch.ExploreAll(t, func(o *orchestrator.Orchestrator) {
		transports := setupClusterWithKV(stormAddrs)
		addKVNode(o, transports)
		addStormNodes(o, stormAddrs, transports)
	}, orchestrator.GlobalBound(4), orchestrator.GlobalMaxRuns(2000))

	if ok {
		t.Log("Bound=4: did not find bug within max runs")
	} else {
		t.Log("Bound=4: FOUND deep gate violation (depth 4)")
	}
}

// TestStorm_PCT uses PCT to find the bug probabilistically.
func TestStorm_PCT(t *testing.T) {
	runtime.GOMAXPROCS(8)

	orch := orchestrator.New()
	ok := orch.ExplorePCT(t, func(o *orchestrator.Orchestrator) {
		transports := setupClusterWithKV(stormAddrs)
		addKVNode(o, transports)
		addStormNodes(o, stormAddrs, transports)
	}, orchestrator.PCTDepth(4), orchestrator.PCTMaxRuns(1000), orchestrator.PCTSeed(1))

	if ok {
		t.Log("PCT(d=4): did not find bug within 1000 runs")
	} else {
		t.Log("PCT(d=4): FOUND gate violation")
	}
}

// TestStorm_Random uses uniform random scheduling.
func TestStorm_Random(t *testing.T) {
	runtime.GOMAXPROCS(8)

	orch := orchestrator.New()
	ok := orch.ExploreRandom(t, func(o *orchestrator.Orchestrator) {
		transports := setupClusterWithKV(stormAddrs)
		addKVNode(o, transports)
		addStormNodes(o, stormAddrs, transports)
	}, orchestrator.RandomMaxRuns(1000), orchestrator.RandomSeed(1))

	if ok {
		t.Log("Random: did not find bug within 1000 runs")
	} else {
		t.Log("Random: FOUND gate violation")
	}
}

// @stance: creative
