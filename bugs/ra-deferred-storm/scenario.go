package radeferredstorm

import (
	"sync/atomic"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

const (
	activeContenders = 3
	kvAddr           = "KV"
)

var scenarioAddrs = []string{"A", "C", "D", "Z"}

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

func addDeferredStormNodes(orch *orchestrator.Orchestrator, addrs []string, transports map[string]*OrchestratorTransport) {
	addDeferredStormNodesCore(orch, addrs, transports, true)
}

func addDeferredStormNodesUnchecked(orch *orchestrator.Orchestrator, addrs []string, transports map[string]*OrchestratorTransport) {
	addDeferredStormNodesCore(orch, addrs, transports, false)
}

func addDeferredStormNodesCore(orch *orchestrator.Orchestrator, addrs []string, transports map[string]*OrchestratorTransport, reportBug bool) {
	expected := activeContenders + 1 // A(1) + D(2) + Z(1) = 4 CS entries
	var done atomic.Int32

	for i, addr := range addrs {
		addr := addr
		tr := transports[addr]

		peers := make([]string, 0, len(addrs)-1)
		for j, peer := range addrs {
			if i != j {
				peers = append(peers, peer)
			}
		}

		orch.AddNode(tr, func(t *testing.T) {
			tr.StartBridge()
			node := NewDeferredStormNode(addr, tr, peers)
			if addr == "C" {
				node.SetReleasePeer("Z")
			}
			node.Start()
			kv := NewKVClient(tr, kvAddr)

			checkCounter := func() {
				if int(done.Add(1)) == expected {
					final := kv.Get("counter")
					if reportBug && final != expected {
						t.Errorf("lost update: counter=%d, want %d", final, expected)
					}
				}
			}

			switch addr {
			case "D":
				node.EnterCS()
				node.SendStart("A")
				val := kv.Get("counter")
				kv.Put("counter", val+1)
				node.ExitCS(true)
				checkCounter()

				node.WaitForEagerEntry()
				val = kv.Get("counter")
				kv.Put("counter", val+1)
				node.ExitCS(false)
				checkCounter()
				return

			case "A":
				node.WaitForStart()
				node.EnterCS()
				node.WaitForRequest("Z", 1)
				node.WaitForRequest("D", 2)
				val := kv.Get("counter")
				kv.Put("counter", val+1)
				node.ExitCS(false)
				node.SendArm("C")
				checkCounter()
				return

			case "Z":
				node.WaitForRequest("A", 1)
				node.EnterCS()
				node.WaitForRelease()
				val := kv.Get("counter")
				kv.Put("counter", val+1)
				node.ExitCS(false)
				checkCounter()
				return

			case "C":
				return
			}
		})
	}
}
