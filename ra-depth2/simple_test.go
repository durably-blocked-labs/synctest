package radepth2

import (
	"runtime"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

// TestSimpleGateRA tests the GateRANode with the orchestrator.
// Each node acquires, holds, and releases the lock.
func TestSimpleGateRA(t *testing.T) {
	runtime.GOMAXPROCS(4)
	addrs := []string{"A", "B", "C"}

	transports := make(map[string]*OrchestratorTransport, len(addrs))
	for _, addr := range addrs {
		transports[addr] = NewOrchestratorTransport(addr)
	}
	for _, tr := range transports {
		for _, peer := range transports {
			if tr != peer {
				tr.Connect(peer)
			}
		}
	}

	orch := orchestrator.New()
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
			node := NewGateRANode(addr, tr, peers)
			node.Start()

			node.AcquireLock()
			t.Logf("%s: acquired lock", addr)
			node.ReleaseLock()
			t.Logf("%s: released lock", addr)

			node.Stop()
			tr.Close()
		})
	}

	_, ok := orch.Run(t)
	if !ok {
		t.Fatal("orchestrator run failed")
	}
}
