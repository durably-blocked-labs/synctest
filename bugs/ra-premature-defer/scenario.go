package raprematuredefer

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/shubhaankar/synctest/orchestrator"
)

const activeContenders = 2

var scenarioAddrs = []string{"A", "B", "C"}

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

func addPrematureDeferNodes(orch *orchestrator.Orchestrator, addrs []string, transports map[string]*OrchestratorTransport, store *KVStore, inCS *atomic.Int32, violated *atomic.Bool) {
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
			node := NewPrematureDeferNode(addr, tr, peers)
			node.OnViolation = func(msg string) {
				if violated != nil {
					violated.Store(true)
				}
				t.Errorf("protocol violation: %s", msg)
			}
			if addr == "C" {
				node.SetStartPeer("A")
			}

			node.Start()

			switch addr {
			case "A":
				node.AcquireLock()
				store.Put("counter", store.Get("counter")+1)
				node.ReleaseLock()

				node.WaitForStart()
				node.AcquireLock()
				if inCS != nil {
					if v := inCS.Add(1); v > 1 {
						t.Errorf("mutual exclusion violated on %s: %d in CS", addr, v)
					}
				}
				time.Sleep(5 * time.Millisecond)
				store.Put("counter", store.Get("counter")+1)
				if inCS != nil {
					inCS.Add(-1)
				}
				node.ReleaseLock()
				return

			case "B":
				node.WaitForStart()
				node.AcquireLock()
				if inCS != nil {
					if v := inCS.Add(1); v > 1 {
						t.Errorf("mutual exclusion violated on %s: %d in CS", addr, v)
					}
				}
				// Under FIFO the stale reply is delivered before A is started and is ignored.
				// Reordered runs can deliver the Arm/Start chain first, then the stale reply.
				node.SendStaleReply()
				node.SendArm("C")
				time.Sleep(5 * time.Millisecond)
				store.Put("counter", store.Get("counter")+1)
				if inCS != nil {
					inCS.Add(-1)
				}
				node.ReleaseLock()
				return

			case "C":
				time.Sleep(20 * time.Millisecond)
				node.SendStart("B")
				return
			}
		})
	}
}
