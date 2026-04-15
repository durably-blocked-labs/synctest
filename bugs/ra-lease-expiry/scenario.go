package raleaseexpiry

import (
	"sync/atomic"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

const (
	activeContenders = 2
	kvAddr           = "KV"
)

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

func addLeaseExpiryNodes(orch *orchestrator.Orchestrator, addrs []string, transports map[string]*OrchestratorTransport, wantFinal int) {
	expected := activeContenders
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
			node := NewLeaseExpiryNode(addr, tr, peers)
			node.Start()
			kv := NewKVClient(tr, kvAddr)

			checkCounter := func() {
				if int(done.Add(1)) == expected {
					final := kv.Get("counter")
					if final != wantFinal {
						t.Errorf("lost update: counter=%d, want %d", final, wantFinal)
					}
				}
			}

			switch addr {
			case "A":
				node.AcquireLock()
				tr.SendControl("B", "begin")
				val := kv.Get("counter")
				kv.Put("counter", val+1)
				node.ReleaseLock()
				checkCounter()
				return

			case "B":
				tr.WaitControl("begin")
				node.AcquireLock()
				val := kv.Get("counter")
				kv.Put("counter", val+1)
				node.ReleaseLock()
				checkCounter()
				return

			case "C":
				return
			}
		})
	}
}
