package raduplicaterequest

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/shubhaankar/synctest/orchestrator"
)

const activeContenders = 3

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

func addDuplicateRequestNodes(orch *orchestrator.Orchestrator, addrs []string, transports map[string]*OrchestratorTransport, store *KVStore, inCS *atomic.Int32, violated *atomic.Bool, failOnViolation bool) {
	transports["B"].DuplicateNext("A", MsgRequest)

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
			node := NewDuplicateRequestNode(addr, tr, peers)
			node.Start()

			switch addr {
			case "A":
				node.AcquireLock()
				if inCS != nil {
					if v := inCS.Add(1); v > 1 {
						if violated != nil {
							violated.Store(true)
						}
						if failOnViolation {
							t.Errorf("mutual exclusion violated on %s: %d in CS", addr, v)
						}
					}
				}
				store.Put("counter", store.Get("counter")+1)
				time.Sleep(10 * time.Millisecond)
				if inCS != nil {
					inCS.Add(-1)
				}
				node.ReleaseLock()
				return

			case "B":
				time.Sleep(30 * time.Millisecond)
				node.AcquireLock()
				if inCS != nil {
					if v := inCS.Add(1); v > 1 {
						if violated != nil {
							violated.Store(true)
						}
						if failOnViolation {
							t.Errorf("mutual exclusion violated on %s: %d in CS", addr, v)
						}
					}
				}
				store.Put("counter", store.Get("counter")+1)
				if inCS != nil {
					inCS.Add(-1)
				}
				node.ReleaseLock()
				return

			case "C":
				time.Sleep(4 * time.Millisecond)
				node.AcquireLock()
				if inCS != nil {
					if v := inCS.Add(1); v > 1 {
						if violated != nil {
							violated.Store(true)
						}
						if failOnViolation {
							t.Errorf("mutual exclusion violated on %s: %d in CS", addr, v)
						}
					}
				}
				time.Sleep(15 * time.Millisecond)
				store.Put("counter", store.Get("counter")+1)
				if inCS != nil {
					inCS.Add(-1)
				}
				node.ReleaseLock()
				return
			}
		})
	}
}
