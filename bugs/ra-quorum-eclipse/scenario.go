package raquorumeclipse

import (
	"sync/atomic"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

const (
	activeContenders = 2
	kvAddr           = "KV"
	beginBContention = "begin-b-contention"
)

var scenarioAddrs = []string{"A", "B", "C", "D", "E"}

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

func addQuorumEclipseNodes(orch *orchestrator.Orchestrator, addrs []string, transports map[string]*OrchestratorTransport) {
	var done atomic.Int32

	for _, addr := range addrs {
		addr := addr
		tr := transports[addr]

		orch.AddNode(tr, func(t *testing.T) {
			tr.StartBridge()

			switch addr {
			case "A":
				node := newContenderNode("A", tr, []string{"C", "D", "E"}, 2, 2)
				node.SetContentionReadyHook("D", func() {
					tr.SendControl("B", beginBContention)
				})
				node.Start()
				kv := NewKVClient(tr, kvAddr)

				node.AcquireLock()
				val := kv.Get("counter")
				_ = kv.Get("a-hold")
				_ = kv.Get("a-hold-b")
				_ = kv.Get("a-hold-c")
				kv.Put("counter", val+1)
				node.ReleaseLock()
				checkFinalCounter(t, kv, &done)

			case "B":
				node := newContenderNode("B", tr, []string{"C", "D", "E"}, 2, 1)
				node.Start()
				kv := NewKVClient(tr, kvAddr)

				tr.WaitControl(beginBContention)
				node.AcquireLock()
				val := kv.Get("counter")
				_ = kv.Get("b-hold")
				_ = kv.Get("b-hold-b")
				kv.Put("counter", val+1)
				node.ReleaseLock()
				checkFinalCounter(t, kv, &done)

			case "C":
				node := newVoterNode("C", tr, modeDelayedGrant)
				node.Start()
			case "D":
				node := newVoterNode("D", tr, modeRevokerBug)
				node.Start()
			case "E":
				node := newVoterNode("E", tr, modeFailThenGrant)
				node.Start()
			}
		})
	}
}

func checkFinalCounter(t *testing.T, kv *KVClient, done *atomic.Int32) {
	t.Helper()
	if int(done.Add(1)) != activeContenders {
		return
	}
	final := kv.Get("counter")
	if final != activeContenders {
		t.Errorf("lost update: counter=%d, want %d", final, activeContenders)
	}
}
