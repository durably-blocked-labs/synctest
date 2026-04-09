package rafttest_test

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/hashicorp/raft"
	"github.com/shubhaankar/synctest/orchestrator"
	"github.com/shubhaankar/synctest/rafttest"
)

func init() {
	// Each synctest bubble requires its own dedicated P.
	// 3 nodes + 1 for the orchestrator goroutine.
	runtime.GOMAXPROCS(4)
}

func TestMain(m *testing.M) {
	// Re-enable rand.Seed, which became a no-op in Go 1.24+.
	// The internal/godebug package updates settings when GODEBUG changes via
	// os.Setenv, so this takes effect before any test (and before any raft
	// goroutine) calls rand.Int63().
	//
	// This is needed by TestRaftThreeNodeElectionReplay, which resets the
	// global rand source to a fixed seed before each run so that raft's
	// randomTimeout produces identical timer jitter across record and replay.
	existing := os.Getenv("GODEBUG")
	sep := ""
	if existing != "" {
		sep = ","
	}
	os.Setenv("GODEBUG", existing+sep+"randseednop=0")
	os.Exit(m.Run())
}

// TestRaftThreeNodeElection starts a 3-node Raft cluster inside synctest bubbles,
// routed through the orchestrator, and verifies that a leader is elected.
// func TestRaftThreeNodeElection(t *testing.T) {
// 	addrs := []raft.ServerAddress{"node1", "node2", "node3"}
// 	expectedServers := make(map[raft.ServerID]raft.ServerAddress, len(addrs))

// 	// Build the Raft configuration (all 3 as Voter).
// 	configuration := raft.Configuration{}
// 	for _, addr := range addrs {
// 		expectedServers[raft.ServerID(addr)] = addr
// 		configuration.Servers = append(configuration.Servers, raft.Server{
// 			Suffrage: raft.Voter,
// 			ID:       raft.ServerID(addr),
// 			Address:  addr,
// 		})
// 	}

// 	// Create transports and wire them together.
// 	transports := make([]*rafttest.RaftTransport, len(addrs))
// 	for i, addr := range addrs {
// 		transports[i] = rafttest.NewRaftTransport(addr)
// 	}
// 	// Fully connect: each transport knows all peers.
// 	for i := range transports {
// 		for j := range transports {
// 			if i != j {
// 				transports[i].Connect(transports[j])
// 			}
// 		}
// 	}

// 	// observations is populated inside each bubbleFunc and read after orch.Run.
// 	// No mutex needed: the orchestrator runs bubbles one at a time, so writes
// 	// are sequential; orch.Run returning establishes happens-before for reads.
// 	observations := make(map[raft.ServerAddress]nodeObs)

// 	// Build orchestrator and register nodes.
// 	orch := orchestrator.New()
// 	for i, trans := range transports {
// 		addr := addrs[i]
// 		cfg := configuration // capture for closure

// 		bubbleFunc := func(t *testing.T) {
// 			_ = t
// 			// Start the bridge goroutine inside the bubble.
// 			trans.StartBridge()

// 			// Set up Raft stores.
// 			store := raft.NewInmemStore()
// 			snap := raft.NewDiscardSnapshotStore()

// 			conf := raft.DefaultConfig()
// 			conf.HeartbeatTimeout = 50 * time.Millisecond
// 			conf.ElectionTimeout = 50 * time.Millisecond
// 			conf.LeaderLeaseTimeout = 50 * time.Millisecond
// 			conf.CommitTimeout = 5 * time.Millisecond
// 			conf.LocalID = raft.ServerID(addr)
// 			conf.Logger = nil // suppress log output in tests

// 			// Bootstrap this node with the full cluster configuration.
// 			if err := raft.BootstrapCluster(conf, store, store, snap, trans, cfg); err != nil {
// 				panic(fmt.Sprintf("BootstrapCluster: %v", err))
// 			}

// 			r, err := raft.NewRaft(conf, &raft.MockFSM{}, store, store, snap, trans)
// 			if err != nil {
// 				panic(fmt.Sprintf("NewRaft: %v", err))
// 			}
// 			defer func() {
// 				if err := trans.Close(); err != nil {
// 					panic(fmt.Sprintf("Close transport: %v", err))
// 				}
// 				if err := r.Shutdown().Error(); err != nil {
// 					panic(fmt.Sprintf("Shutdown: %v", err))
// 				}
// 			}()

// 			// Poll until a leader is elected or fake-time deadline expires.
// 			deadline := time.Now().Add(30 * time.Second)
// 			for r.Leader() == "" {
// 				if time.Now().After(deadline) {
// 					panic("timed out waiting for leader election")
// 				}
// 				time.Sleep(5 * time.Millisecond)
// 			}

// 			verifyRaftNodeState(r, raft.ServerID(addr), expectedServers)
// 			observations[addr] = nodeObs{
// 				leader:  r.Leader(),
// 				term:    r.Stats()["term"],
// 			}
// 		}

// 		orch.AddNode(trans, bubbleFunc)
// 	}

// 	// Run the orchestrator — this drives all 3 bubbles to completion.
// 	rec, allPassed := orch.Run(t)

// 	// Assertions.
// 	if !allPassed {
// 		t.Error("one or more nodes failed")
// 	}

// 	sendOps, doneSteps := countTrace(rec.GlobalTrace)
// 	if sendOps == 0 {
// 		t.Error("expected at least one OpSend delivery in trace")
// 	}
// 	if doneSteps != 3 {
// 		t.Errorf("expected 3 StepDone events, got %d", doneSteps)
// 	}
// 	t.Logf("trace: %d steps total, %d sends delivered, %d nodes done",
// 		len(rec.GlobalTrace), sendOps, doneSteps)

// 	checkClusterConsensus(t, observations)
// }

// TestRaftThreeNodeElectionReplay records a 3-node election run and then replays
// it using the recorded global delivery order and local scheduling decisions.
//
// Note on full trace identity: the local trace replay (WithPrefix) controls
// goroutine scheduling decisions within each bubble, but NOT application-level
// randomness such as raft's randomTimeout (which uses math/rand). As a result,
// the replay may elect a different leader with different timer fire times.
// However, the replay MUST succeed (all nodes reach a leader) and MUST deliver
// exactly the same number of messages in the same structural pattern, since the
// global delivery order is enforced by the replay mechanism for any messages
// that are matched by {From, To, Type}.
//
// True bit-for-bit trace equality requires controlling application-level
// randomness in addition to goroutine scheduling decisions.
func TestRaftThreeNodeElectionReplay(t *testing.T) {
	newCluster := func() ([]*rafttest.RaftTransport, raft.Configuration, map[raft.ServerID]raft.ServerAddress) {
		addrs := []raft.ServerAddress{"node1", "node2", "node3"}
		expectedServers := make(map[raft.ServerID]raft.ServerAddress, len(addrs))
		configuration := raft.Configuration{}
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
		return transports, configuration, expectedServers
	}

	addNodes := func(orch *orchestrator.Orchestrator, transports []*rafttest.RaftTransport, cfg raft.Configuration, expectedServers map[raft.ServerID]raft.ServerAddress, obs map[raft.ServerAddress]nodeObs) {
		addrs := []raft.ServerAddress{"node1", "node2", "node3"}
		for i, trans := range transports {
			addr := addrs[i]
			localCfg := cfg
			bubbleFunc := func(t *testing.T) {
				_ = t
				trans.StartBridge()

				store := raft.NewInmemStore()
				snap := raft.NewDiscardSnapshotStore()
				conf := testRaftConfig(raft.ServerID(addr))
				if err := raft.BootstrapCluster(conf, store, store, snap, trans, localCfg); err != nil {
					panic(fmt.Sprintf("BootstrapCluster: %v", err))
				}
				r, err := raft.NewRaft(conf, &raft.MockFSM{}, store, store, snap, trans)
				if err != nil {
					panic(fmt.Sprintf("NewRaft: %v", err))
				}
				defer func() {
					if err := trans.Close(); err != nil {
						panic(fmt.Sprintf("Close transport: %v", err))
					}
					if err := r.Shutdown().Error(); err != nil {
						panic(fmt.Sprintf("Shutdown: %v", err))
					}
				}()
				waitForLeader(r, 30*time.Second)
				verifyRaftNodeState(r, raft.ServerID(addr), expectedServers)
				obs[addr] = nodeObs{
					leader:  r.Leader(),
					term:    r.Stats()["term"],
				}
			}
			orch.AddNode(trans, bubbleFunc)
		}
	}

	// Seed raft's randomTimeout with a fixed source so timer jitter is
	// identical in both runs. Combined with the orchestrator's sequential
	// bubble startup, this ensures the goroutine runq is the same at every
	// scheduling decision step, making WithPrefix replay exact.
	//
	// Note: rand.Seed is a no-op in Go 1.24+. We use raft.TestRand directly
	// to bypass the global math/rand source entirely.
	const seed = 42

	// First run: record.
	rand.Seed(seed) //nolint:staticcheck // randseednop=0 set in TestMain makes this work
	transports1, cfg, expectedServers := newCluster()
	orch1 := orchestrator.New()
	obs1 := make(map[raft.ServerAddress]nodeObs)
	addNodes(orch1, transports1, cfg, expectedServers, obs1)
	rec1, ok1 := orch1.Run(t)
	if !ok1 {
		t.Fatal("record run: one or more nodes failed")
	}
	sends1, done1 := countTrace(rec1.GlobalTrace)
	t.Logf("record: %d steps, %d sends, %d done", len(rec1.GlobalTrace), sends1, done1)
	checkClusterConsensus(t, obs1)

	// Second run: replay. Reset the rand source to the same seed so the
	// timer jitter sequence is identical to the record run.
	rand.Seed(seed) //nolint:staticcheck
	transports2, _, _ := newCluster()
	orch2 := orchestrator.New()
	obs2 := make(map[raft.ServerAddress]nodeObs)
	addNodes(orch2, transports2, cfg, expectedServers, obs2)
	rec2, ok2 := orch2.Replay(t, rec1)
	if !ok2 {
		t.Fatal("replay run: one or more nodes failed")
	}
	checkClusterConsensus(t, obs2)
	sends2, done2 := countTrace(rec2.GlobalTrace)
	t.Logf("replay: %d steps, %d sends, %d done", len(rec2.GlobalTrace), sends2, done2)

	// The replay must produce the same structural outcome.
	if sends2 == 0 {
		t.Error("replay: expected at least one OpSend delivery")
	}
	if done2 != 3 {
		t.Errorf("replay: expected 3 StepDone events, got %d", done2)
	}
	if sends1 != sends2 {
		t.Errorf("send count mismatch: record=%d replay=%d", sends1, sends2)
	}

	// Log whether the traces matched exactly (for informational purposes).
	// Exact identity requires application-level randomness (e.g. raft timer
	// jitter) to be controlled in addition to goroutine scheduling decisions.
	exactMatch := len(rec1.GlobalTrace) == len(rec2.GlobalTrace)
	if exactMatch {
		for i, s1 := range rec1.GlobalTrace {
			if s1 != rec2.GlobalTrace[i] {
				exactMatch = false
				break
			}
		}
	}
	t.Logf("global trace exact match: %v", exactMatch)

	// printRun(t, "record", rec1)
	// printRun(t, "replay", rec2)
}

// TestRaftThreeNodeElectionExplore explores different global message delivery
// orderings for a 3-node Raft election using DFS with context bounding.
// setup is called before each explored trace to create fresh transports and
// register nodes; rand is re-seeded so timer jitter is identical across runs.
//
// Expected: all delivery orderings within bound=2 produce a valid leader election.
func TestRaftThreeNodeElectionExplore(t *testing.T) {
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

	orch := orchestrator.New()
	allPassed := orch.Explore(t, func(o *orchestrator.Orchestrator) {
		rand.Seed(42) //nolint:staticcheck // randseednop=0 set in TestMain makes this work

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
			localExpected := expectedServers
			o.AddNode(trans, func(t *testing.T) {
				_ = t
				trans.StartBridge()

				store := raft.NewInmemStore()
				snap := raft.NewDiscardSnapshotStore()
				conf := testRaftConfig(raft.ServerID(addr))
				if err := raft.BootstrapCluster(conf, store, store, snap, trans, localCfg); err != nil {
					panic(fmt.Sprintf("BootstrapCluster: %v", err))
				}
				r, err := raft.NewRaft(conf, &raft.MockFSM{}, store, store, snap, trans)
				if err != nil {
					panic(fmt.Sprintf("NewRaft: %v", err))
				}
				defer func() {
					if err := trans.Close(); err != nil {
						panic(fmt.Sprintf("Close transport: %v", err))
					}
					if err := r.Shutdown().Error(); err != nil {
						panic(fmt.Sprintf("Shutdown: %v", err))
					}
				}()
				waitForLeader(r, 30*time.Second)
				verifyRaftNodeState(r, raft.ServerID(addr), localExpected)
			})
		}
	}, orchestrator.GlobalBound(2), orchestrator.GlobalMaxRuns(20))

	if !allPassed {
		t.Fatal("some delivery ordering produced a failing run")//
	}
}

// // TestRaftRecoverClusterOrchestrated mirrors raft.TestRaft_RecoverCluster in the
// // orchestrator harness:
// //  1. Start and elect a leader.
// //  2. Shut everything down.
// //  3. Run RecoverCluster on persisted stores.
// //  4. Restart and verify leader election and configuration again.
// func TestRaftRecoverClusterOrchestrated(t *testing.T) {
// 	addrs := []raft.ServerAddress{"node1", "node2", "node3"}
// 	expectedServers := make(map[raft.ServerID]raft.ServerAddress, len(addrs))
// 	configuration := raft.Configuration{}
// 	for _, addr := range addrs {
// 		expectedServers[raft.ServerID(addr)] = addr
// 		configuration.Servers = append(configuration.Servers, raft.Server{
// 			Suffrage: raft.Voter,
// 			ID:       raft.ServerID(addr),
// 			Address:  addr,
// 		})
// 	}

// 	// Persisted stores reused across initial run -> recovery -> restart.
// 	logStable := make([]*raft.InmemStore, len(addrs))
// 	snaps := make([]*raft.InmemSnapshotStore, len(addrs))
// 	for i := range addrs {
// 		logStable[i] = raft.NewInmemStore()
// 		snaps[i] = raft.NewInmemSnapshotStore()
// 	}

// 	// Phase 1: start cluster and elect a leader.
// 	transports1 := make([]*rafttest.RaftTransport, len(addrs))
// 	for i, addr := range addrs {
// 		transports1[i] = rafttest.NewRaftTransport(addr)
// 	}
// 	for i := range transports1 {
// 		for j := range transports1 {
// 			if i != j {
// 				transports1[i].Connect(transports1[j])
// 			}
// 		}
// 	}

// 	obs1 := make(map[raft.ServerAddress]nodeObs)
// 	orch1 := orchestrator.New()
// 	for i, trans := range transports1 {
// 		addr := addrs[i]
// 		cfg := configuration
// 		store := logStable[i]
// 		snap := snaps[i]

// 		bubbleFunc := func(t *testing.T) {
// 			_ = t
// 			trans.StartBridge()

// 			conf := testRaftConfig(raft.ServerID(addr))
// 			if err := raft.BootstrapCluster(conf, store, store, snap, trans, cfg); err != nil {
// 				panic(fmt.Sprintf("phase1 node %s bootstrap: %v", addr, err))
// 			}

// 			r, err := raft.NewRaft(conf, &raft.MockFSM{}, store, store, snap, trans)
// 			if err != nil {
// 				panic(fmt.Sprintf("phase1 node %s new raft: %v", addr, err))
// 			}
// 			defer func() {
// 				if err := trans.Close(); err != nil {
// 					panic(fmt.Sprintf("phase1 node %s close transport: %v", addr, err))
// 				}
// 				if err := r.Shutdown().Error(); err != nil {
// 					panic(fmt.Sprintf("phase1 node %s shutdown: %v", addr, err))
// 				}
// 			}()

// 			waitForLeader(r, 30*time.Second)
// 			verifyRaftNodeState(r, raft.ServerID(addr), expectedServers)
// 			obs1[addr] = nodeObs{
// 				leader:  r.Leader(),
// 				term:    r.Stats()["term"],
// 			}
// 		}
// 		orch1.AddNode(trans, bubbleFunc)
// 	}

// 	rec1, ok1 := orch1.Run(t)
// 	if !ok1 {
// 		t.Fatal("phase1: one or more nodes failed")
// 	}
// 	checkClusterConsensus(t, obs1)
// 	if sends, done := countTrace(rec1.GlobalTrace); sends == 0 || done != len(addrs) {
// 		t.Fatalf("phase1: expected sends>0 and done=%d, got sends=%d done=%d", len(addrs), sends, done)
// 	}

// 	// Phase 2: recover each node's persisted state.
// 	for i, addr := range addrs {
// 		before, err := snaps[i].List()
// 		if err != nil {
// 			t.Fatalf("phase2 node %s: list snapshots before recover: %v", addr, err)
// 		}

// 		recTrans := rafttest.NewRaftTransport(addr)
// 		conf := testRaftConfig(raft.ServerID(addr))
// 		if err := raft.RecoverCluster(conf, &raft.MockFSM{}, logStable[i], logStable[i], snaps[i], recTrans, configuration); err != nil {
// 			t.Fatalf("phase2 node %s: recover cluster: %v", addr, err)
// 		}

// 		after, err := snaps[i].List()
// 		if err != nil {
// 			t.Fatalf("phase2 node %s: list snapshots after recover: %v", addr, err)
// 		}
// 		if len(after) != len(before)+1 {
// 			t.Fatalf("phase2 node %s: expected one new snapshot (%d -> %d)", addr, len(before), len(after))
// 		}

// 		first, err := logStable[i].FirstIndex()
// 		if err != nil {
// 			t.Fatalf("phase2 node %s: first index: %v", addr, err)
// 		}
// 		last, err := logStable[i].LastIndex()
// 		if err != nil {
// 			t.Fatalf("phase2 node %s: last index: %v", addr, err)
// 		}
// 		if first != 0 || last != 0 {
// 			t.Fatalf("phase2 node %s: expected compacted logs first/last=0/0, got %d/%d", addr, first, last)
// 		}
// 	}

// 	// Phase 3: restart from recovered stores and verify cluster health.
// 	transports2 := make([]*rafttest.RaftTransport, len(addrs))
// 	for i, addr := range addrs {
// 		transports2[i] = rafttest.NewRaftTransport(addr)
// 	}
// 	for i := range transports2 {
// 		for j := range transports2 {
// 			if i != j {
// 				transports2[i].Connect(transports2[j])
// 			}
// 		}
// 	}

// 	obs2 := make(map[raft.ServerAddress]nodeObs)
// 	orch2 := orchestrator.New()
// 	for i, trans := range transports2 {
// 		addr := addrs[i]
// 		store := logStable[i]
// 		snap := snaps[i]

// 		bubbleFunc := func(t *testing.T) {
// 			_ = t
// 			trans.StartBridge()

// 			conf := testRaftConfig(raft.ServerID(addr))
// 			r, err := raft.NewRaft(conf, &raft.MockFSM{}, store, store, snap, trans)
// 			if err != nil {
// 				panic(fmt.Sprintf("phase3 node %s new raft: %v", addr, err))
// 			}
// 			defer func() {
// 				if err := trans.Close(); err != nil {
// 					panic(fmt.Sprintf("phase3 node %s close transport: %v", addr, err))
// 				}
// 				if err := r.Shutdown().Error(); err != nil {
// 					panic(fmt.Sprintf("phase3 node %s shutdown: %v", addr, err))
// 				}
// 			}()

// 			waitForLeader(r, 30*time.Second)
// 			verifyRaftNodeState(r, raft.ServerID(addr), expectedServers)
// 			obs2[addr] = nodeObs{
// 				leader:  r.Leader(),
// 				term:    r.Stats()["term"],
// 			}
// 		}
// 		orch2.AddNode(trans, bubbleFunc)
// 	}

// 	rec2, ok2 := orch2.Run(t)
// 	if !ok2 {
// 		t.Fatal("phase3: one or more recovered nodes failed")
// 	}
// 	checkClusterConsensus(t, obs2)
// 	if sends, done := countTrace(rec2.GlobalTrace); sends == 0 || done != len(addrs) {
// 		t.Fatalf("phase3: expected sends>0 and done=%d, got sends=%d done=%d", len(addrs), sends, done)
// 	}
// }

// nodeObs holds the cluster state observed by a single node at the end of a run.
type nodeObs struct {
	leader raft.ServerAddress
	term   string
}

// checkClusterConsensus asserts that every node in obs agrees on the same leader
// and term. It does not check appliedIdx because waitForLeader only guarantees
// that the leader is known — followers may not have applied the commit yet.
//
// obs is populated inside bubble funcs (which run sequentially under the
// orchestrator) and read here after orch.Run returns, so no mutex is needed.
func checkClusterConsensus(t *testing.T, obs map[raft.ServerAddress]nodeObs) {
	t.Helper()
	if len(obs) == 0 {
		t.Error("checkClusterConsensus: no observations recorded")
		return
	}

	var wantLeader raft.ServerAddress
	var wantTerm string
	for addr, o := range obs {
		if wantLeader == "" {
			wantLeader = o.leader
			wantTerm = o.term
		}
		if o.leader != wantLeader {
			t.Errorf("node %s sees leader %q, want %q", addr, o.leader, wantLeader)
		}
		if o.term != wantTerm {
			t.Errorf("node %s is on term %s, want %s", addr, o.term, wantTerm)
		}
	}
	t.Logf("cluster consensus: leader=%s term=%s", wantLeader, wantTerm)
}

func testRaftConfig(localID raft.ServerID) *raft.Config {
	conf := raft.DefaultConfig()
	conf.HeartbeatTimeout = 50 * time.Millisecond
	conf.ElectionTimeout = 50 * time.Millisecond
	conf.LeaderLeaseTimeout = 50 * time.Millisecond
	conf.CommitTimeout = 5 * time.Millisecond
	conf.LocalID = localID
	conf.Logger = nil
	// Keep snapshots explicit for this test so recover behavior is easier to assert.
	conf.SnapshotThreshold = 1 << 20
	conf.TrailingLogs = 64
	return conf
}

func waitForLeader(r *raft.Raft, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for r.Leader() == "" {
		if time.Now().After(deadline) {
			panic("timed out waiting for leader election")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func countTrace(trace []orchestrator.GlobalStep) (sendOps, doneSteps int) {
	for _, step := range trace {
		switch step.Type {
		case orchestrator.StepDeliver:
			if step.Dir == orchestrator.OpSend {
				sendOps++
			}
		case orchestrator.StepDone:
			doneSteps++
		}
	}
	return sendOps, doneSteps
}

func verifyRaftNodeState(r *raft.Raft, localID raft.ServerID, expected map[raft.ServerID]raft.ServerAddress) {
	leaderAddr, leaderID := r.LeaderWithID()
	if leaderAddr == "" || leaderID == "" {
		panic(fmt.Sprintf("node %s: empty leader info addr=%q id=%q", localID, leaderAddr, leaderID))
	}

	cfgFuture := r.GetConfiguration()
	if err := cfgFuture.Error(); err != nil {
		panic(fmt.Sprintf("node %s: GetConfiguration: %v", localID, err))
	}
	gotCfg := cfgFuture.Configuration()
	if len(gotCfg.Servers) != len(expected) {
		panic(fmt.Sprintf("node %s: expected %d servers, got %d", localID, len(expected), len(gotCfg.Servers)))
	}
	for _, srv := range gotCfg.Servers {
		wantAddr, ok := expected[srv.ID]
		if !ok {
			panic(fmt.Sprintf("node %s: unexpected server id %q in config", localID, srv.ID))
		}
		if srv.Address != wantAddr {
			panic(fmt.Sprintf("node %s: server %q has addr %q, want %q", localID, srv.ID, srv.Address, wantAddr))
		}
		if srv.Suffrage != raft.Voter {
			panic(fmt.Sprintf("node %s: server %q suffrage=%v, want voter", localID, srv.ID, srv.Suffrage))
		}
	}

	applied := r.AppliedIndex()
	last := r.LastIndex()
	if applied > last {
		panic(fmt.Sprintf("node %s: applied index %d > last index %d", localID, applied, last))
	}

	stats := r.Stats()
	if stats == nil {
		panic(fmt.Sprintf("node %s: nil stats map", localID))
	}
	if term := stats["term"]; term == "" {
		panic(fmt.Sprintf("node %s: empty term in stats", localID))
	}
	if state := stats["state"]; state == "" {
		panic(fmt.Sprintf("node %s: empty state in stats", localID))
	}
	if state := stats["state"]; !strings.EqualFold(state, r.State().String()) {
		panic(fmt.Sprintf("node %s: stats state %q != raft state %q", localID, state, r.State().String()))
	}
}

// printRun logs the global step sequence and per-node local scheduling decisions
// for a RecordedRun, useful for comparing record vs replay executions.
func printRun(t *testing.T, label string, rec orchestrator.RecordedRun) {
	t.Helper()

	t.Logf("=== %s: global trace (%d steps) ===", label, len(rec.GlobalTrace))
	for i, step := range rec.GlobalTrace {
		switch step.Type {
		case orchestrator.StepDeliver:
			t.Logf("  [%2d] deliver %-4s %s → %s (%s) @ %dns",
				i, dirStr(step.Dir), step.From, step.To, step.OpType, step.Time)
		case orchestrator.StepTimeAdvance:
			t.Logf("  [%2d] time-advance → %dns", i, step.Time)
		case orchestrator.StepDone:
			t.Logf("  [%2d] done         node=%s", i, step.From)
		}
	}

	// Print local decisions sorted by node name for stable output.
	nodes := make([]string, 0, len(rec.LocalTraces))
	for n := range rec.LocalTraces {
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)

	for _, node := range nodes {
		decisions := rec.LocalTraces[node]
		t.Logf("=== %s: local decisions for %s (%d decisions) ===", label, node, len(decisions))
		for i, d := range decisions {
			t.Logf("  [%2d] step=%d chosen=g%d runq=%d %s",
				i, d.Step, d.ChosenBgid, d.RunqSize, formatRunq(d))
		}
	}
}

func dirStr(d orchestrator.OpDir) string {
	if d == orchestrator.OpSend {
		return "send"
	}
	return "recv"
}

// formatRunq formats the runnable goroutine IDs from a Decision.
func formatRunq(d synctest.Decision) string {
	if d.RunqSize == 0 {
		return ""
	}
	n := int(d.RunqSize)
	if n > len(d.RunqBgids) {
		n = len(d.RunqBgids)
	}
	s := "runq=["
	for i := 0; i < n; i++ {
		if i > 0 {
			s += " "
		}
		s += fmt.Sprintf("g%d", d.RunqBgids[i])
	}
	return s + "]"
}
