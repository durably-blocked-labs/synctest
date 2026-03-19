package rafttest_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/shubhaankar/synctest/orchestratorv2"
	"github.com/shubhaankar/synctest/rafttest"
)

// TestStaleTermConcept shows bug #1: SkipTermCheck disables the stale-term
// guard. Under FIFO delivery the bug is latent. Under Explore it triggers.
func TestStaleTermConcept(t *testing.T) {
	addrs := []raft.ServerAddress{"node1", "node2", "node3"}
	configuration := raft.Configuration{}
	expectedServers := make(map[raft.ServerID]raft.ServerAddress, len(addrs))
	for _, addr := range addrs {
		expectedServers[raft.ServerID(addr)] = addr
		configuration.Servers = append(configuration.Servers, raft.Server{
			Suffrage: raft.Voter,
			ID:       raft.ServerID(addr),
			Address:  addr,
		})
	}

	transports := make([]*rafttest.RaftTransport, len(addrs))
	for i, addr := range addrs {
		transports[i] = rafttest.NewRaftTransport(addr)
	}
	for i := range transports {
		for j := range transports {
			if i != j {
				transports[i].Connect(transports[j])
			}
		}
	}

	obs := make(map[raft.ServerAddress]nodeObs)
	orch := orchestratorv2.New()
	for i, trans := range transports {
		addr := addrs[i]
		localCfg := configuration
		orch.AddNode(trans, func(t *testing.T) {
			_ = t
			trans.StartBridge()
			store := raft.NewInmemStore()
			snap := raft.NewDiscardSnapshotStore()
			conf := testRaftConfig(raft.ServerID(addr))
			conf.SkipTermCheck = true
			if err := raft.BootstrapCluster(conf, store, store, snap, trans, localCfg); err != nil {
				panic(fmt.Sprintf("BootstrapCluster: %v", err))
			}
			r, err := raft.NewRaft(conf, &raft.MockFSM{}, store, store, snap, trans)
			if err != nil {
				panic(fmt.Sprintf("NewRaft: %v", err))
			}
			defer func() { trans.Close(); r.Shutdown() }()

			waitForLeader(r, 30*time.Second)
			verifyRaftNodeState(r, raft.ServerID(addr), expectedServers)
			obs[addr] = nodeObs{leader: r.Leader(), term: r.Stats()["term"]}
		})
	}

	_, ok := orch.Run(t)
	if !ok {
		t.Fatal("one or more nodes failed")
	}
	checkClusterConsensus(t, obs)
	t.Log("FIFO delivery: PASSED (bug is latent — only triggers under reordered delivery)")
}

// TestRemoveLeaderConcept demonstrates bug #6: the select race between
// commitCh and applyCh after RemoveServer commits. All nodes exit together
// (no staggered shutdown) to avoid transport deadlocks.
func TestRemoveLeaderConcept(t *testing.T) {
	addrs := []raft.ServerAddress{"node1", "node2", "node3"}
	configuration := raft.Configuration{}
	for _, addr := range addrs {
		configuration.Servers = append(configuration.Servers, raft.Server{
			Suffrage: raft.Voter,
			ID:       raft.ServerID(addr),
			Address:  addr,
		})
	}

	transports := make([]*rafttest.RaftTransport, len(addrs))
	for i, addr := range addrs {
		transports[i] = rafttest.NewRaftTransport(addr)
	}
	for i := range transports {
		for j := range transports {
			if i != j {
				transports[i].Connect(transports[j])
			}
		}
	}

	var leaderAddr raft.ServerAddress
	var postSucceeded, postFailed int

	// Shared barrier: leader closes this after the test is done.
	// Created inside the FIRST bubble so it's a bubble channel.
	// Other bubbles will block on it via their own raft Apply mechanism.
	// Actually — we can't share bubble channels across bubbles.
	// Instead: followers just verify and return. The leader does everything
	// in one shot: apply, remove, post-apply. If the leader can't commit
	// because followers exited, RemoveServer fails and we skip.

	orch := orchestratorv2.New()
	for i, trans := range transports {
		addr := addrs[i]
		localCfg := configuration
		orch.AddNode(trans, func(t *testing.T) {
			_ = t
			trans.StartBridge()
			store := raft.NewInmemStore()
			snap := raft.NewDiscardSnapshotStore()
			conf := testRaftConfig(raft.ServerID(addr))
			conf.ShutdownOnRemove = false
			if err := raft.BootstrapCluster(conf, store, store, snap, trans, localCfg); err != nil {
				panic(fmt.Sprintf("BootstrapCluster: %v", err))
			}
			r, err := raft.NewRaft(conf, &raft.MockFSM{}, store, store, snap, trans)
			if err != nil {
				panic(fmt.Sprintf("NewRaft: %v", err))
			}
			defer func() { trans.Close(); r.Shutdown() }()

			waitForLeader(r, 30*time.Second)

			if r.State() == raft.Leader {
				leaderAddr = addr

				// Apply entries.
				for i := byte(0); i < 3; i++ {
					f := r.Apply([]byte{i}, 5*time.Second)
					if err := f.Error(); err != nil {
						return
					}
				}

				// RemoveServer — blocks until committed.
				removeFuture := r.RemoveServer(raft.ServerID(addr), 0, 0)
				if err := removeFuture.Error(); err != nil {
					t.Logf("RemoveServer error: %v", err)
					return
				}

				// Post-remove applies. Should fail with ErrNotLeader.
				for i := byte(3); i < 8; i++ {
					f := r.Apply([]byte{i}, 0)
					if f.Error() == nil {
						postSucceeded++
					} else {
						postFailed++
					}
				}
			} else {
				// Follower: stay alive until config shrinks (leader removed).
				// GetConfiguration is a local read — fast, no RPC needed.
				for {
					time.Sleep(5 * time.Millisecond)
					cf := r.GetConfiguration()
					if cf.Error() != nil {
						break
					}
					if len(cf.Configuration().Servers) < 3 {
						break
					}
				}
			}
		})
	}

	_, _ = orch.Run(t)

	t.Logf("Leader: %s", leaderAddr)
	t.Logf("Post-remove: %d succeeded (bug), %d failed (correct)", postSucceeded, postFailed)
	if postSucceeded > 0 {
		t.Log("BUG TRIGGERED: select picked applyCh over commitCh")
	} else if postFailed > 0 {
		t.Log("Bug not triggered. Different selectCounter value could trigger it.")
	}
}
