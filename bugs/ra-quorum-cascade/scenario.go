package raquorumcascade

import (
	"sync/atomic"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

const (
	activeContenders = 3
	kvAddr           = "KV"
	startBLabel      = "start-b"
	armCLabel        = "arm-c"
	startCLabel      = "start-c"
)

var scenarioAddrs = []string{"A", "B", "C"}
var observedFinalCounter atomic.Int32
var observedBug atomic.Bool

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

func addQuorumCascadeNodes(orch *orchestrator.Orchestrator, addrs []string, transports map[string]*OrchestratorTransport) {
	addQuorumCascadeNodesCore(orch, addrs, transports, false)
}

func addQuorumCascadeNodesChecked(orch *orchestrator.Orchestrator, addrs []string, transports map[string]*OrchestratorTransport) {
	addQuorumCascadeNodesCore(orch, addrs, transports, true)
}

func addQuorumCascadeNodesCore(orch *orchestrator.Orchestrator, addrs []string, transports map[string]*OrchestratorTransport, reportBug bool) {
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
			node := NewQuorumCascadeNode(addr, tr, peers)
			if addr == "B" {
				sent := false
				node.OnReplyReceived = func(from string, count int) {
					if from != "C" || count != 1 || sent {
						return
					}
					sent = true
					tr.SendControl("C", armCLabel)
				}
			}
			node.Start()
			kv := NewKVClient(tr, kvAddr)

			switch addr {
			case "A":
				node.AcquireLock()
				tr.SendControl("B", startBLabel)
				val := kv.Get("counter")
				kv.Put("counter", val+1)
				node.ReleaseLock()
				tr.SendControl("C", startCLabel)
			case "B":
				tr.WaitControl(startBLabel)
				node.AcquireLock()
				val := kv.Get("counter")
				kv.Put("counter", val+1)
				node.ReleaseLock()
			case "C":
				tr.WaitControl(armCLabel)
				tr.WaitControl(startCLabel)
				node.AcquireLock()
				val := kv.Get("counter")
				kv.Put("counter", val+1)
				node.ReleaseLock()
			}

			if int(done.Add(1)) == expected {
				final := kv.Get("counter")
				observedFinalCounter.Store(int32(final))
				if final != expected {
					observedBug.Store(true)
					if reportBug {
						t.Errorf("lost update: counter=%d, want %d", final, expected)
					}
				}
			}
		})
	}
}

func lastObservedCounter() int {
	return int(observedFinalCounter.Load())
}

func observedBugFound() bool {
	return observedBug.Load()
}

func resetObservedOutcome() {
	observedFinalCounter.Store(-1)
	observedBug.Store(false)
}
