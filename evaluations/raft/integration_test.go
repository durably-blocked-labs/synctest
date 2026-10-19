package rafteval_test

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
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
	setup := rafteval.NewClusterSetup(3, func(t *testing.T, node *rafteval.Node) {
		leader := node.WaitForLeader(t, workloadTimeout)
		if leader == "" {
			t.Fatalf("node %s observed an empty leader", node.ID)
		}

		future := node.Raft.GetConfiguration()
		if err := future.Error(); err != nil {
			t.Fatalf("node %s: get configuration: %v", node.ID, err)
		}
		if got := len(future.Configuration().Servers); got != 3 {
			t.Fatalf("node %s: configuration has %d servers, want 3", node.ID, got)
		}
	}, rafteval.WithSeed(42))

	orch := orchestrator.New()
	result := orch.ExploreWith(t, setup, &orchestrator.DPOR{Bound: 1, GlobalOnly: true}, orchestrator.GlobalMaxRuns(1))
	if result.Failed != 0 || result.Runs != 1 {
		t.Fatalf("election result: runs=%d failed=%d", result.Runs, result.Failed)
	}
}

func TestReplicateCommand(t *testing.T) {
	command := []byte("set x=1")
	setup := rafteval.NewClusterSetup(3, func(t *testing.T, node *rafteval.Node) {
		node.WaitForLeader(t, workloadTimeout)
		if node.IsLeader() {
			if _, err := node.Apply(command, workloadTimeout); err != nil {
				t.Fatalf("node %s: apply command: %v", node.ID, err)
			}
		}

		history := node.WaitForCommands(t, 1, workloadTimeout)
		if got := string(history[0].Data); got != string(command) {
			t.Fatalf("node %s applied %q, want %q", node.ID, got, command)
		}
	}, rafteval.WithSeed(42))

	orch := orchestrator.New()
	result := orch.ExploreWith(t, setup, &orchestrator.DPOR{Bound: 1, GlobalOnly: true}, orchestrator.GlobalMaxRuns(1))
	if result.Failed != 0 || result.Runs != 1 {
		t.Fatalf("replication result: runs=%d failed=%d", result.Runs, result.Failed)
	}
}

func TestConcurrentApplies(t *testing.T) {
	const commandCount = 8
	setup := concurrentApplySetup(commandCount)
	orch := orchestrator.New()
	result := orch.ExploreWith(t, setup, &orchestrator.DPOR{Bound: 1, GlobalOnly: true}, orchestrator.GlobalMaxRuns(1))
	if result.Failed != 0 || result.Runs != 1 {
		t.Fatalf("concurrent apply result: runs=%d failed=%d", result.Runs, result.Failed)
	}
}

func TestExploreConcurrentApplies(t *testing.T) {
	setup := concurrentApplySetup(4)
	orch := orchestrator.New()
	result := orch.ExploreWith(
		t,
		setup,
		&orchestrator.DPOR{Bound: 1, GlobalOnly: true, ConservativeGlobal: true},
		orchestrator.GlobalMaxRuns(3),
	)
	if result.Failed != 0 {
		t.Fatalf("DPOR found a failure in %d runs", result.Runs)
	}
	if result.Runs < 2 {
		t.Fatalf("DPOR explored %d run, want at least 2", result.Runs)
	}
}

func concurrentApplySetup(commandCount int) func(*orchestrator.Orchestrator) {
	return rafteval.NewClusterSetup(3, func(t *testing.T, node *rafteval.Node) {
		node.WaitForLeader(t, workloadTimeout)
		if node.IsLeader() {
			var wg sync.WaitGroup
			errors := make(chan error, commandCount)
			for i := range commandCount {
				command := []byte(fmt.Sprintf("command-%02d", i))
				wg.Add(1)
				go func() {
					defer wg.Done()
					_, err := node.Apply(command, workloadTimeout)
					errors <- err
				}()
			}
			wg.Wait()
			close(errors)
			for err := range errors {
				if err != nil {
					t.Fatalf("node %s: concurrent apply: %v", node.ID, err)
				}
			}
		}

		history := node.WaitForCommands(t, commandCount, workloadTimeout)
		seen := make(map[string]bool, commandCount)
		for _, entry := range history {
			seen[string(entry.Data)] = true
		}
		for i := range commandCount {
			command := fmt.Sprintf("command-%02d", i)
			if !seen[command] {
				t.Errorf("node %s did not apply %q", node.ID, command)
			}
		}
	}, rafteval.WithSeed(42), rafteval.WithConfig(func(config *raft.Config) {
		config.HeartbeatTimeout = 100 * time.Millisecond
		config.ElectionTimeout = 100 * time.Millisecond
	}))
}
