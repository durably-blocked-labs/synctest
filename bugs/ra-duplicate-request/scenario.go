package raduplicaterequest

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/shubhaankar/synctest/orchestrator"
)

const (
	activeContenders = 3
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

func addDuplicateRequestNodes(orch *orchestrator.Orchestrator, addrs []string, transports map[string]*OrchestratorTransport) {
	transports["B"].DuplicateNext("A", MsgRequest)

	expected := activeContenders // All 3 nodes do 1 CS each
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
			node := NewDuplicateRequestNode(addr, tr, peers)
			node.Start()
			kv := NewKVClient(tr, kvAddr)

			switch addr {
			case "A":
				node.AcquireLock()
				val := kv.Get("counter")
				kv.Put("counter", val+1)
				time.Sleep(10 * time.Millisecond)
				node.ReleaseLock()

			case "B":
				time.Sleep(30 * time.Millisecond)
				node.AcquireLock()
				val := kv.Get("counter")
				kv.Put("counter", val+1)
				node.ReleaseLock()

			case "C":
				time.Sleep(4 * time.Millisecond)
				node.AcquireLock()
				val := kv.Get("counter")
				time.Sleep(15 * time.Millisecond)
				kv.Put("counter", val+1)
				node.ReleaseLock()
			}

			if int(done.Add(1)) == expected {
				final := kv.Get("counter")
				if final != expected {
					t.Errorf("lost update: counter=%d, want %d", final, expected)
				}
			}
		})
	}
}
