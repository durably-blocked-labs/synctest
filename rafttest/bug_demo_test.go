package rafttest_test

import (
	"fmt"
	"math/rand"
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

// TestRemoveLeaderBugTriggered reliably triggers bug #6: the select race
// between commitCh and applyCh in leaderLoop after RemoveServer.
//
// The trick: fire RemoveServer WITHOUT waiting on its future, then immediately
// fire Apply calls. This puts values on applyCh before leaderLoop processes
// the commitCh notification for the RemoveServer commit. When leaderLoop's
// select sees both channels ready, applyCh can win — and since stepDown is
// still false (commitCh hasn't been processed), the applies are dispatched
// and succeed. This is the bug: applies succeed after the leader was removed.
//
// This reproduces deterministically on every run because:
//   - The synctest bubble serializes goroutine execution (single P)
//   - FIFO scheduling ensures the user goroutine fires Apply before
//     leaderLoop gets to its next select iteration
//   - The select counter is deterministic inside the bubble
func TestRemoveLeaderBugTriggered(t *testing.T) {
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

	rand.Seed(42) //nolint:staticcheck

	var leaderAddr raft.ServerAddress
	var postSucceeded, postFailed int

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

				// Apply a few entries and wait for commit.
				for i := byte(0); i < 3; i++ {
					f := r.Apply([]byte{i}, 5*time.Second)
					if err := f.Error(); err != nil {
						return
					}
				}

				// Fire RemoveServer WITHOUT waiting — then immediately
				// fire Apply calls. This creates the race: applyCh has
				// values before leaderLoop processes commitCh for the
				// RemoveServer commit.
				removeFuture := r.RemoveServer(raft.ServerID(addr), 0, 0)

				var applyFutures []raft.ApplyFuture
				for i := byte(3); i < 8; i++ {
					applyFutures = append(applyFutures, r.Apply([]byte{i}, 5*time.Second))
				}

				// Wait for RemoveServer to complete.
				removeFuture.Error()

				// Check which applies succeeded (bug) vs failed (correct).
				for _, f := range applyFutures {
					if f.Error() == nil {
						postSucceeded++
					} else {
						postFailed++
					}
				}
			} else {
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
		t.Logf("BUG TRIGGERED: %d applies succeeded after RemoveServer committed — "+
			"leaderLoop's select picked applyCh over commitCh", postSucceeded)
	} else {
		t.Fatal("expected bug to trigger but all applies failed correctly")
	}
}

// TestRemoveLeaderExploreAll uses ExploreAll to automatically find the
// RemoveLeader bug through combined global + local exploration.
//
// The test uses a realistic two-goroutine pattern: one goroutine calls
// RemoveServer and waits for the future, another goroutine calls Apply
// concurrently. The explorer varies both message delivery order (global)
// and goroutine scheduling (local) to find the interleaving where applyCh
// wins over commitCh in leaderLoop's select.
func TestRemoveLeaderExploreAll(t *testing.T) {
	addrs := []raft.ServerAddress{"node1", "node2", "node3"}
	configuration := raft.Configuration{}
	for _, addr := range addrs {
		configuration.Servers = append(configuration.Servers, raft.Server{
			Suffrage: raft.Voter,
			ID:       raft.ServerID(addr),
			Address:  addr,
		})
	}

	orch := orchestratorv2.New()
	allPassed := orch.ExploreAll(t, func(o *orchestratorv2.Orchestrator) {
		rand.Seed(42) //nolint:staticcheck

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

		for i, trans := range transports {
			addr := addrs[i]
			localCfg := configuration
			o.AddNode(trans, func(t *testing.T) {
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
					// Apply a few entries first.
					for i := byte(0); i < 3; i++ {
						f := r.Apply([]byte{i}, 5*time.Second)
						if err := f.Error(); err != nil {
							return
						}
					}

					// Two concurrent goroutines — realistic production pattern.
					// The explorer varies goroutine scheduling to find the
					// interleaving where Apply runs before leaderLoop reads commitCh.
					done := make(chan error, 1)
					go func() {
						done <- r.RemoveServer(raft.ServerID(addr), 0, 0).Error()
					}()

					// Concurrent applies — these may land on applyCh before
					// or after leaderLoop processes the RemoveServer commit.
					var applyFutures []raft.ApplyFuture
					for i := byte(3); i < 8; i++ {
						applyFutures = append(applyFutures, r.Apply([]byte{i}, 5*time.Second))
					}

					<-done // wait for RemoveServer

					for _, f := range applyFutures {
						if f.Error() == nil {
							t.Errorf("BUG: Apply succeeded after RemoveServer committed")
						}
					}
				} else {
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
	}, orchestratorv2.GlobalBound(2), orchestratorv2.GlobalMaxRuns(50))

	if !allPassed {
		t.Log("ExploreAll found the RemoveLeader bug!")
	} else {
		t.Log("ExploreAll did not find the bug within the run limit")
	}
}
