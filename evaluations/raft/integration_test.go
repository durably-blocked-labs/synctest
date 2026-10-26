package rafteval_test

import (
	"os"
	"runtime"
	"testing"

	"github.com/shubhaankar/synctest/evaluations/bubblegun"
	"github.com/shubhaankar/synctest/evaluations/raft"
	"github.com/shubhaankar/synctest/orchestrator"
)

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
