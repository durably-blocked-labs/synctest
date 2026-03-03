package rafttest_test

import (
	"runtime"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/shubhaankar/synctest/orchestratorv2"
	"github.com/shubhaankar/synctest/rafttest"
)

func init() {
	// Each synctest bubble requires its own dedicated P.
	// 3 nodes + 1 for the orchestrator goroutine.
	runtime.GOMAXPROCS(4)
}

// TestRaftThreeNodeElection starts a 3-node Raft cluster inside synctest bubbles,
// routed through the orchestrator, and verifies that a leader is elected.
func TestRaftThreeNodeElection(t *testing.T) {
	addrs := []raft.ServerAddress{"node1", "node2", "node3"}

	// Build the Raft configuration (all 3 as Voter).
	configuration := raft.Configuration{}
	for _, addr := range addrs {
		configuration.Servers = append(configuration.Servers, raft.Server{
			Suffrage: raft.Voter,
			ID:       raft.ServerID(addr),
			Address:  addr,
		})
	}

	// Create transports and wire them together.
	transports := make([]*rafttest.RaftTransport, len(addrs))
	for i, addr := range addrs {
		transports[i] = rafttest.NewRaftTransport(addr)
	}
	// Fully connect: each transport knows all peers.
	for i := range transports {
		for j := range transports {
			if i != j {
				transports[i].Connect(transports[j])
			}
		}
	}

	// Build orchestrator and register nodes.
	orch := orchestratorv2.New()
	for i, trans := range transports {
		addr := addrs[i]
		cfg := configuration // capture for closure

		bubbleFunc := func(t *testing.T) {
			// Start the bridge goroutine inside the bubble.
			trans.StartBridge()

			// Set up Raft stores.
			store := raft.NewInmemStore()
			snap := raft.NewDiscardSnapshotStore()

			conf := raft.DefaultConfig()
			conf.HeartbeatTimeout = 50 * time.Millisecond
			conf.ElectionTimeout = 50 * time.Millisecond
			conf.LeaderLeaseTimeout = 50 * time.Millisecond
			conf.CommitTimeout = 5 * time.Millisecond
			conf.LocalID = raft.ServerID(addr)
			conf.Logger = nil // suppress log output in tests

			// Bootstrap this node with the full cluster configuration.
			if err := raft.BootstrapCluster(conf, store, store, snap, trans, cfg); err != nil {
				t.Fatalf("BootstrapCluster: %v", err)
			}

			r, err := raft.NewRaft(conf, &raft.MockFSM{}, store, store, snap, trans)
			if err != nil {
				t.Fatalf("NewRaft: %v", err)
			}

			// Poll until a leader is elected or fake-time deadline expires.
			deadline := time.Now().Add(30 * time.Second)
			for r.Leader() == "" {
				if time.Now().After(deadline) {
					t.Error("timed out waiting for leader election")
					break
				}
				time.Sleep(5 * time.Millisecond)
			}

			if leader := r.Leader(); leader != "" {
				t.Logf("node %s sees leader: %s", addr, leader)
			}

			if err := r.Shutdown().Error(); err != nil {
				t.Errorf("Shutdown: %v", err)
			}
		}

		orch.AddNode(trans, bubbleFunc)
	}

	// Run the orchestrator — this drives all 3 bubbles to completion.
	trace, allPassed := orch.Run(t)

	// Assertions.
	if !allPassed {
		t.Error("one or more nodes failed")
	}

	// Count delivered send ops and done steps.
	var sendOps, doneSteps int
	for _, step := range trace {
		switch step.Type {
		case orchestratorv2.StepDeliver:
			if step.Dir == orchestratorv2.OpSend {
				sendOps++
			}
		case orchestratorv2.StepDone:
			doneSteps++
		}
	}

	if sendOps == 0 {
		t.Error("expected at least one OpSend delivery in trace")
	}
	if doneSteps != 3 {
		t.Errorf("expected 3 StepDone events, got %d", doneSteps)
	}

	t.Logf("trace: %d steps total, %d sends delivered, %d nodes done",
		len(trace), sendOps, doneSteps)
}
