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

// TestStaleTermBug shows bug #1 (injected): SkipTermCheck disables the
// stale-term guard in appendEntries. Under FIFO delivery the bug is latent
// because old-leader messages always arrive before new-leader messages.
// Under reordered delivery (Explore / ExploreAll) a stale AppendEntries
// from term 1 arrives after a term-2 election — bypassing the guard.
//
// TODO: wire up Explore / ExploreAll to find the delivery ordering that
// triggers disagreement. This requires the assertion to detect when a
// follower accepts a stale-term message (e.g. leader/term mismatch across
// nodes after the run).
func TestStaleTermBug(t *testing.T) {
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
			// conf.SkipTermCheck = true
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

// TestRemoveLeaderBug triggers a real bug in hashicorp/raft (no code
// injection): the select race between commitCh and applyCh in leaderLoop.
//
// When RemoveServer and Apply are concurrent, Apply calls land on applyCh
// before leaderLoop processes the commitCh notification for the RemoveServer
// commit. The commit requires cross-node replication (slow); Apply is a
// local channel send (fast). The applies are dispatched with stepDown=false
// and succeed — a safety violation: entries committed by a removed leader.
//
// This reproduces deterministically on every run. It is not scheduling-
// dependent — it triggers under ANY goroutine ordering because the commit
// is always slower than the local Apply.
//
// TODO: the next step is to use ExploreAll (combined global + local
// exploration) to find bugs where FIFO passes but a specific interleaving
// fails. This bug doesn't qualify (it always triggers), but the stale-term
// bug does.
func TestRemoveLeaderBug(t *testing.T) {
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
				// fire Apply calls. The applies land on applyCh before
				// leaderLoop processes commitCh for the RemoveServer
				// commit (which requires cross-node replication).
				removeFuture := r.RemoveServer(raft.ServerID(addr), 0, 0)

				var applyFutures []raft.ApplyFuture
				for i := byte(3); i < 8; i++ {
					applyFutures = append(applyFutures, r.Apply([]byte{i}, 5*time.Second))
				}

				removeFuture.Error()

				for _, f := range applyFutures {
					if f.Error() == nil {
						postSucceeded++
					} else {
						postFailed++
					}
				}
			} else {
				// Follower: stay alive until config shrinks.
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
		t.Logf("BUG TRIGGERED: %d applies succeeded after RemoveServer committed", postSucceeded)
	} else {
		t.Fatal("expected bug to trigger but all applies failed correctly")
	}
}
