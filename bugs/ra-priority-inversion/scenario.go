package rapriorityinversion

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

const (
	activeContenders = 3
	kvAddr           = "KV"
	startBLabel      = "start-b"
	startELabel      = "start-e"
	bReadyLabel      = "b-ready"
	eReadyLabel      = "e-ready"
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

func addPriorityInversionNodes(orch *orchestrator.Orchestrator, addrs []string, transports map[string]*OrchestratorTransport) {
	addPriorityInversionNodesCore(orch, addrs, transports, false)
}

func addPriorityInversionNodesChecked(orch *orchestrator.Orchestrator, addrs []string, transports map[string]*OrchestratorTransport) {
	addPriorityInversionNodesCore(orch, addrs, transports, true)
}

func addPriorityInversionNodesCore(orch *orchestrator.Orchestrator, addrs []string, transports map[string]*OrchestratorTransport, checked bool) {
	expected := activeContenders
	var done atomic.Int32
	var priorityBug atomic.Bool
	var replyOrderMu sync.Mutex
	sawHigherReply := false

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
			node := NewPriorityInversionNode(addr, tr, peers)
			node.Start()
			kv := NewKVClient(tr, kvAddr)

			switch addr {
			case "A":
				if checked {
					node.OnReplySent = func(to string) {
						replyOrderMu.Lock()
						defer replyOrderMu.Unlock()
						switch to {
						case "B":
							sawHigherReply = true
						case "E":
							if !sawHigherReply {
								priorityBug.Store(true)
							}
						}
					}
				}
				node.AcquireLock()
				tr.SendControl("B", startBLabel)
				tr.WaitControl(bReadyLabel)
				tr.SendControl("E", startELabel)
				if checked {
					tr.WaitControl(eReadyLabel)
					transports["B"].ReleaseHeld()
				}
				val := kv.Get("counter")
				_ = kv.Get("a-hold")
				kv.Put("counter", val+1)
				node.ReleaseLock()
			case "B":
				if checked {
					tr.HoldNext("A", MsgRequest)
				}
				node.SetAcquireAnnounce("A", bReadyLabel)
				tr.WaitControl(startBLabel)
				node.AcquireLock()
				val := kv.Get("counter")
				_ = kv.Get("b-hold")
				kv.Put("counter", val+1)
				node.ReleaseLock()
			case "E":
				if checked {
					node.SetAcquireAnnounce("A", eReadyLabel)
				}
				tr.WaitControl(startELabel)
				node.AcquireLock()
				val := kv.Get("counter")
				_ = kv.Get("e-hold")
				kv.Put("counter", val+1)
				node.ReleaseLock()
			case "C", "D":
				return
			}

			if int(done.Add(1)) == expected {
				if checked && priorityBug.Load() {
					t.Errorf("priority inversion: replied to lower-priority E before higher-priority B")
				}
				final := kv.Get("counter")
				if final != expected {
					t.Errorf("lost update: counter=%d, want %d", final, expected)
				}
			}
		})
	}
}
