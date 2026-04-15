package chainreplication

import (
	"runtime"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

func setupChain() map[string]*OrchestratorTransport {
	addrs := []string{"A", "B", "C", "D"}
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

const numWrites = 5

// addChainNodes wires up HEAD(A) → MID(B) → TAIL(C) with Client(D).
//
// The Client drives the scenario in three phases:
//  1. Send writes 1-2, wait for acks (fully committed via A→B→C chain)
//  2. Trigger failure: stop B, reconfigure A→C direct chain
//  3. Send writes 3-5 to A (which now forwards directly to C)
//
// Under FIFO: writes 1-2 propagate normally (phase 1 acks arrive),
// failure triggers cleanly (A's sentHistory empty), writes 3-5 go
// directly A→C. TAIL has all 5. PASS.
//
// Under reordering: write 3 might reach A and be forwarded to B
// BEFORE the NewSuccessor message arrives. B is stopped, so write 3
// is lost in B's mailbox. When A processes NewSuccessor(lastAck=2),
// the off-by-one bug skips seq=3 in sentHistory. TAIL gets [1,2,4,5].
func addChainNodes(orch *orchestrator.Orchestrator, tr map[string]*OrchestratorTransport) {
	// HEAD (A)
	orch.AddNode(tr["A"], func(t *testing.T) {
		tr["A"].StartBridge()
		node := NewChainNode("A", tr["A"], "", "B")
		node.Start()
		<-node.done
	})

	// MID (B)
	orch.AddNode(tr["B"], func(t *testing.T) {
		tr["B"].StartBridge()
		node := NewChainNode("B", tr["B"], "A", "C")
		node.Start()
		<-node.done
	})

	// TAIL (C)
	orch.AddNode(tr["C"], func(t *testing.T) {
		tr["C"].StartBridge()
		node := NewChainNode("C", tr["C"], "B", "")
		node.Start()
		<-node.done

		// Post-shutdown invariant check: TAIL must have all numWrites entries.
		hist := node.CommittedHistory()
		seqs := make(map[int]bool)
		for _, e := range hist {
			if e.Key == "counter" {
				seqs[e.Seq] = true
			}
		}
		if len(seqs) != numWrites {
			missing := []int{}
			for i := 1; i <= numWrites; i++ {
				if !seqs[i] {
					missing = append(missing, i)
				}
			}
			t.Errorf("chain lost writes: TAIL has %d/%d (missing seq %v)",
				len(seqs), numWrites, missing)
		}
	})

	// Client (D) — drives the three-phase scenario.
	orch.AddNode(tr["D"], func(t *testing.T) {
		tr["D"].StartBridge()

		// Phase 1: writes that propagate through the full chain.
		for i := 1; i <= 2; i++ {
			tr["D"].Send("A", Message{
				Kind: MsgClientWrite, From: "D", Key: "counter", Value: i,
			})
		}
		// Wait for both acks (writes fully committed at TAIL).
		for i := 0; i < 2; i++ {
			<-tr["D"].Mailbox()
		}

		// Phase 2: failure recovery.
		tr["D"].Send("B", Message{Kind: MsgStop, From: "D"})
		tr["D"].Send("A", Message{
			Kind: MsgNewSuccessor, From: "D", NewPeer: "C", LastAckSeq: 2,
		})
		tr["D"].Send("C", Message{
			Kind: MsgNewPredecessor, From: "D", NewPeer: "A",
		})

		// Phase 3: writes that go through the recovered chain (A→C direct).
		for i := 3; i <= numWrites; i++ {
			tr["D"].Send("A", Message{
				Kind: MsgClientWrite, From: "D", Key: "counter", Value: i,
			})
		}
		// Wait for acks for writes 3-5.
		for i := 0; i < numWrites-2; i++ {
			<-tr["D"].Mailbox()
		}
	})
}

func TestChain_FIFOPasses(t *testing.T) {
	runtime.GOMAXPROCS(8)
	tr := setupChain()
	orch := orchestrator.New()
	addChainNodes(orch, tr)

	_, ok := orch.Run(t)
	if !ok {
		t.Fatal("FIFO run failed")
	}
	t.Log("FIFO: PASSED")
}

func TestChain_ExploreGlobalOnly(t *testing.T) {
	runtime.GOMAXPROCS(8)
	orch := orchestrator.New()
	ok := orch.Explore(t, func(o *orchestrator.Orchestrator) {
		tr := setupChain()
		addChainNodes(o, tr)
	}, orchestrator.GlobalBound(5), orchestrator.GlobalMaxRuns(2000))

	if ok {
		t.Log("G-only: did not find bug within max runs")
	} else {
		t.Log("G-only: FOUND chain replication write loss")
	}
}

func TestChain_ExploreAll(t *testing.T) {
	runtime.GOMAXPROCS(8)
	orch := orchestrator.New()
	ok := orch.ExploreAll(t, func(o *orchestrator.Orchestrator) {
		tr := setupChain()
		addChainNodes(o, tr)
	}, orchestrator.GlobalBound(5), orchestrator.GlobalMaxRuns(2000))

	if ok {
		t.Log("ExploreAll: did not find bug within max runs")
	} else {
		t.Log("ExploreAll: FOUND chain replication write loss")
	}
}

func TestChain_PCT(t *testing.T) {
	runtime.GOMAXPROCS(8)
	orch := orchestrator.New()
	ok := orch.ExplorePCT(t, func(o *orchestrator.Orchestrator) {
		tr := setupChain()
		addChainNodes(o, tr)
	}, orchestrator.PCTDepth(5), orchestrator.PCTMaxRuns(2000), orchestrator.PCTSeed(1))

	if ok {
		t.Log("PCT(d=5): did not find bug within 2000 runs")
	} else {
		t.Log("PCT(d=5): FOUND chain replication write loss")
	}
}

func TestChain_Random(t *testing.T) {
	runtime.GOMAXPROCS(8)
	orch := orchestrator.New()
	ok := orch.ExploreRandom(t, func(o *orchestrator.Orchestrator) {
		tr := setupChain()
		addChainNodes(o, tr)
	}, orchestrator.RandomMaxRuns(2000), orchestrator.RandomSeed(1))

	if ok {
		t.Log("Random: did not find bug within 2000 runs")
	} else {
		t.Log("Random: FOUND chain replication write loss")
	}
}

// @stance: creative
