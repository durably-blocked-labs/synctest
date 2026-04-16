package quorumreadrepair

import (
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

var replicaAddrs = []string{"R1", "R2", "R3"}

func setupCluster(addrs []string) map[string]*OrchestratorTransport {
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
	return transports
}

func addReplicas(orch *orchestrator.Orchestrator, addrs []string, transports map[string]*OrchestratorTransport) {
	for _, addr := range addrs {
		addr := addr
		tr := transports[addr]
		orch.AddNode(tr, func(t *testing.T) {
			tr.StartBridge()
			replica := NewReplica(addr, tr)
			replica.Start()
		})
	}
}
