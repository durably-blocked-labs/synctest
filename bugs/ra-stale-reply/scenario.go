package rastalereply

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

func addStaleReplyNodes(orch *orchestrator.Orchestrator, addrs []string, transports map[string]*OrchestratorTransport, store *KVStore, inCS *atomic.Int32) {
	transports["B"].DuplicateNext("A", MsgReply)

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
			node := NewStaleReplyNode(addr, tr, peers)
			node.Start()

			switch addr {
			case "A":
				node.AcquireLock()
				if inCS != nil {
					if v := inCS.Add(1); v > 1 {
						t.Errorf("mutual exclusion violated on %s round 1: %d in CS", addr, v)
					}
				}
				store.Put("counter", store.Get("counter")+1)
				if inCS != nil {
					inCS.Add(-1)
				}
				node.ReleaseLock()

				time.Sleep(10 * time.Millisecond)

				node.AcquireLock()
				if inCS != nil {
					if v := inCS.Add(1); v > 1 {
						t.Errorf("mutual exclusion violated on %s round 2: %d in CS", addr, v)
					}
				}
				store.Put("counter", store.Get("counter")+1)
				if inCS != nil {
					inCS.Add(-1)
				}
				node.ReleaseLock()
				return

			case "B":
				time.Sleep(5 * time.Millisecond)
				node.AcquireLock()
				if inCS != nil {
					if v := inCS.Add(1); v > 1 {
						t.Errorf("mutual exclusion violated on %s: %d in CS", addr, v)
					}
				}
				time.Sleep(20 * time.Millisecond)
				store.Put("counter", store.Get("counter")+1)
				if inCS != nil {
					inCS.Add(-1)
				}
				node.ReleaseLock()
				return

			case "C":
				return
			}
		})
	}
}
