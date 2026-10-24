package rafteval_test

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/shubhaankar/synctest/evaluations/bubblegun"
	"github.com/shubhaankar/synctest/evaluations/raft"
	"github.com/shubhaankar/synctest/orchestrator"
)

const workloadTimeout = 5 * time.Second

func TestMain(m *testing.M) {
	runtime.GOMAXPROCS(4)
	existing := os.Getenv("GODEBUG")
	separator := ""
	if existing != "" {
		separator = ","
	}
	_ = os.Setenv("GODEBUG", existing+separator+"randseednop=0")
	os.Exit(m.Run())
}

func TestLeaderElection(t *testing.T) {
	runScenario(t, "raft/leader-election", 1, 1)
}

func TestReplicateCommand(t *testing.T) {
	runScenario(t, "raft/replicate-command", 1, 1)
}

func TestApplyRejectsFollowers(t *testing.T) {
	command := []byte("leader-command")
	setup := rafteval.NewClusterSetup(3, func(t *testing.T, node *rafteval.Node) {
		node.WaitForLeader(t, workloadTimeout)
		if node.IsLeader() {
			if _, err := node.Apply(command, workloadTimeout); err != nil {
				t.Fatalf("node %s: apply command: %v", node.ID, err)
			}
		} else {
			future := node.Raft.Apply([]byte("follower-command"), time.Second)
			if err := future.Error(); !errors.Is(err, raft.ErrNotLeader) {
				t.Fatalf("node %s: follower apply error = %v, want %v", node.ID, err, raft.ErrNotLeader)
			}
		}

		history := node.WaitForCommands(t, 1, workloadTimeout)
		if got := string(history[0].Data); got != string(command) {
			t.Fatalf("node %s applied %q, want %q", node.ID, got, command)
		}
	}, rafteval.WithSeed(42))

	result := orchestrator.New().ExploreWith(t, setup, &orchestrator.DPOR{Bound: 1}, orchestrator.GlobalMaxRuns(1))
	if result.Failed != 0 || result.Runs != 1 {
		t.Fatalf("follower rejection result: runs=%d failed=%d", result.Runs, result.Failed)
	}
}

func TestBarrierWaitsForAppliedCommands(t *testing.T) {
	const commandCount = 8
	setup := rafteval.NewClusterSetup(3, func(t *testing.T, node *rafteval.Node) {
		node.WaitForLeader(t, workloadTimeout)
		if node.IsLeader() {
			for i := range commandCount {
				node.Raft.Apply([]byte(fmt.Sprintf("barrier-command-%02d", i)), 0)
			}
			if err := node.Raft.Barrier(workloadTimeout).Error(); err != nil {
				t.Fatalf("node %s: barrier: %v", node.ID, err)
			}
			if got := len(node.FSM.History()); got != commandCount {
				t.Fatalf("node %s: barrier returned after %d commands, want %d", node.ID, got, commandCount)
			}
		}

		node.WaitForCommands(t, commandCount, workloadTimeout)
	}, rafteval.WithSeed(42))

	result := orchestrator.New().ExploreWith(t, setup, &orchestrator.DPOR{Bound: 1}, orchestrator.GlobalMaxRuns(1))
	if result.Failed != 0 || result.Runs != 1 {
		t.Fatalf("barrier result: runs=%d failed=%d", result.Runs, result.Failed)
	}
}

func TestVerifyLeader(t *testing.T) {
	setup := rafteval.NewClusterSetup(3, func(t *testing.T, node *rafteval.Node) {
		node.WaitForLeader(t, workloadTimeout)
		if node.IsLeader() {
			if err := node.Raft.VerifyLeader().Error(); err != nil {
				t.Fatalf("node %s: verify leader: %v", node.ID, err)
			}
		}
	}, rafteval.WithSeed(42))

	result := orchestrator.New().ExploreWith(t, setup, &orchestrator.DPOR{Bound: 1}, orchestrator.GlobalMaxRuns(1))
	if result.Failed != 0 || result.Runs != 1 {
		t.Fatalf("verify leader result: runs=%d failed=%d", result.Runs, result.Failed)
	}
}

func TestLeadershipTransfer(t *testing.T) {
	setup := rafteval.NewClusterSetupFactory(3, func() rafteval.NodeTest {
		var observers atomic.Int32
		return func(t *testing.T, node *rafteval.Node) {
			oldLeader := node.WaitForLeader(t, workloadTimeout)
			observers.Add(1)
			if node.IsLeader() {
				deadline := time.Now().Add(workloadTimeout)
				for observers.Load() != 3 {
					if !time.Now().Before(deadline) {
						t.Fatalf("node %s: not all nodes observed leader %s", node.ID, oldLeader)
					}
					time.Sleep(5 * time.Millisecond)
				}
				if err := node.Raft.LeadershipTransfer().Error(); err != nil {
					t.Fatalf("node %s: transfer leadership: %v", node.ID, err)
				}
			}

			deadline := time.Now().Add(workloadTimeout)
			for node.Raft.Leader() == "" || node.Raft.Leader() == oldLeader {
				if !time.Now().Before(deadline) {
					t.Fatalf("node %s: leadership did not transfer from %s", node.ID, oldLeader)
				}
				time.Sleep(5 * time.Millisecond)
			}
		}
	}, rafteval.WithSeed(42))

	result := orchestrator.New().ExploreWith(t, setup, &orchestrator.DPOR{Bound: 1}, orchestrator.GlobalMaxRuns(1))
	if result.Failed != 0 || result.Runs != 1 {
		t.Fatalf("leadership transfer result: runs=%d failed=%d", result.Runs, result.Failed)
	}
}

func TestConcurrentApplies(t *testing.T) {
	runScenario(t, "raft/concurrent-applies", 1, 1)
}

func TestExploreConcurrentApplies(t *testing.T) {
	runScenario(t, "raft/concurrent-applies", 3, 2)
}

func runScenario(t *testing.T, name string, maxRuns, minRuns int) {
	t.Helper()
	registry := bubblegun.NewRegistry()
	rafteval.RegisterScenarios(registry)
	scenario, err := registry.Scenario(name)
	if err != nil {
		t.Fatal(err)
	}
	input, err := scenario.Generate(42)
	if err != nil {
		t.Fatal(err)
	}
	setup, err := scenario.Setup(input)
	if err != nil {
		t.Fatal(err)
	}
	result := orchestrator.New().ExploreWith(
		t,
		setup,
		&orchestrator.DPOR{Bound: 1},
		orchestrator.WithTrialLimits(scenario.TrialLimits),
		orchestrator.GlobalMaxRuns(maxRuns),
	)
	if result.Failed != 0 || result.Runs < minRuns {
		t.Fatalf("%s: runs=%d failed=%d", name, result.Runs, result.Failed)
	}
}
