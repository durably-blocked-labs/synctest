package raghostgrant

import (
	"sync/atomic"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

const (
	activeContenders = 2
	kvAddr           = "KV"
)

var scenarioAddrs = []string{"A", "B", "C", "D"}

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

func addGhostGrantNodes(orch *orchestrator.Orchestrator, addrs []string, transports map[string]*OrchestratorTransport) {
	expected := activeContenders + 1 // A acquires twice; C acquires once.
	var done atomic.Int32

	initialPeers := map[string][]string{
		"A": []string{"B", "D"},
		"B": []string{"A", "D"},
		"C": []string{"A", "B", "D"},
		"D": []string{"A", "B"},
	}

	for _, addr := range addrs {
		addr := addr
		tr := transports[addr]
		peers := append([]string(nil), initialPeers[addr]...)

		orch.AddNode(tr, func(t *testing.T) {
			tr.StartBridge()
			node := NewGhostGrantNode(addr, tr, peers)
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
				node.AcquireLock()
				val := kv.Get("counter")
				kv.Put("counter", val+1)
				node.ReleaseLock()
				checkCounter()

				node.AcquireLock()
				val = kv.Get("counter")
				_ = kv.Get("a-hold")
				kv.Put("counter", val+1)
				node.ReleaseLock()
				checkCounter()

			case "B":
				return

			case "C":
				node.JoinCluster()
				node.AcquireLock()
				val := kv.Get("counter")
				_ = kv.Get("c-hold")
				kv.Put("counter", val+1)
				node.ReleaseLock()
				checkCounter()

			case "D":
				return
			}
		})
	}
}
