package rafteval

import (
	"encoding/json"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/shubhaankar/synctest/evaluations/bubblegun"
	"github.com/shubhaankar/synctest/orchestrator"
)

const scenarioTimeout = 5 * time.Second

var raftTrialLimits = orchestrator.TrialLimits{
	Decisions:         4096,
	PendingOperations: 4096,
	TraceBytes:        8 << 20,
}

type scenarioInput struct {
	Seed         int64 `json:"seed"`
	CommandCount int   `json:"command_count,omitempty"`
}

func RegisterScenarios(registry *bubblegun.Registry) {
	scenarios := []bubblegun.Scenario{
		newLeaderElectionScenario(),
		newReplicateCommandScenario(),
		newConcurrentAppliesScenario(),
		newInjectedLostUpdateScenario(),
	}
	for _, scenario := range scenarios {
		registry.RegisterScenario(scenario)
	}
}

func newLeaderElectionScenario() bubblegun.Scenario {
	return newScenario("raft/leader-election", 0, leaderElectionSetup)
}

func newReplicateCommandScenario() bubblegun.Scenario {
	return newScenario("raft/replicate-command", 1, replicateCommandSetup)
}

func newConcurrentAppliesScenario() bubblegun.Scenario {
	return newScenario("raft/concurrent-applies", 4, concurrentAppliesSetup)
}

func newInjectedLostUpdateScenario() bubblegun.Scenario {
	return newScenario("raft/injected-lost-update", 2, injectedLostUpdateSetup)
}

func newScenario(name string, commandCount int, setup func(scenarioInput) func(*orchestrator.Orchestrator)) bubblegun.Scenario {
	return bubblegun.Scenario{
		Name:        name,
		TrialLimits: raftTrialLimits,
		Generate: func(attempt int) (json.RawMessage, error) {
			return json.Marshal(scenarioInput{Seed: int64(attempt), CommandCount: commandCount})
		},
		Setup: func(raw json.RawMessage) (func(*orchestrator.Orchestrator), error) {
			input, err := decodeScenarioInput(raw)
			if err != nil {
				return nil, err
			}
			return setup(input), nil
		},
	}
}

func leaderElectionSetup(input scenarioInput) func(*orchestrator.Orchestrator) {
	return NewClusterSetup(3, func(t *testing.T, node *Node) {
		leader := node.WaitForLeader(t, scenarioTimeout)
		if leader == "" {
			t.Fatalf("node %s observed an empty leader", node.ID)
		}
		configuration := node.Raft.GetConfiguration()
		if err := configuration.Error(); err != nil {
			t.Fatalf("node %s: get configuration: %v", node.ID, err)
		}
		if got := len(configuration.Configuration().Servers); got != 3 {
			t.Fatalf("node %s: configuration has %d servers, want 3", node.ID, got)
		}
	}, WithSeed(input.Seed))
}

func replicateCommandSetup(input scenarioInput) func(*orchestrator.Orchestrator) {
	command := []byte(fmt.Sprintf("attempt-%d-command", input.Seed))
	return NewClusterSetup(3, func(t *testing.T, node *Node) {
		node.WaitForLeader(t, scenarioTimeout)
		if node.IsLeader() {
			if _, err := node.Apply(command, scenarioTimeout); err != nil {
				t.Fatalf("node %s: apply command: %v", node.ID, err)
			}
		}
		history := node.WaitForCommands(t, 1, scenarioTimeout)
		if got := string(history[0].Data); got != string(command) {
			t.Fatalf("node %s applied %q, want %q", node.ID, got, command)
		}
	}, WithSeed(input.Seed))
}

func concurrentAppliesSetup(input scenarioInput) func(*orchestrator.Orchestrator) {
	return NewClusterSetup(3, func(t *testing.T, node *Node) {
		node.WaitForLeader(t, scenarioTimeout)
		if node.IsLeader() {
			errors := make(chan error, input.CommandCount)
			for i := range input.CommandCount {
				command := []byte(fmt.Sprintf("attempt-%d-command-%02d", input.Seed, i))
				go func() {
					_, err := node.Apply(command, scenarioTimeout)
					errors <- err
				}()
			}
			for range input.CommandCount {
				err := <-errors
				if err != nil {
					t.Fatalf("node %s: concurrent apply: %v", node.ID, err)
				}
			}
		}

		history := node.WaitForCommands(t, input.CommandCount, scenarioTimeout)
		seen := make(map[string]bool, input.CommandCount)
		for _, entry := range history {
			seen[string(entry.Data)] = true
		}
		for i := range input.CommandCount {
			command := fmt.Sprintf("attempt-%d-command-%02d", input.Seed, i)
			if !seen[command] {
				t.Errorf("node %s did not apply %q", node.ID, command)
			}
		}
	}, WithSeed(input.Seed), WithConfig(func(config *raft.Config) {
		config.HeartbeatTimeout = 100 * time.Millisecond
		config.ElectionTimeout = 100 * time.Millisecond
	}))
}

type splitSequence struct {
	next atomic.Int64
}

func (s *splitSequence) TakeSplit() int64 {
	next := s.next.Load()
	runtime.Gosched()
	s.next.Store(next + 1)
	return next
}

func (s *splitSequence) TakeAtomic() int64 {
	return s.next.Add(1) - 1
}

func injectedLostUpdateSetup(input scenarioInput) func(*orchestrator.Orchestrator) {
	return NewClusterSetup(1, func(t *testing.T, node *Node) {
		node.WaitForLeader(t, scenarioTimeout)
		if node.IsLeader() {
			sequence := new(splitSequence)
			errors := make(chan error, input.CommandCount)
			apply := func(next func() int64) {
				command := []byte(fmt.Sprintf("attempt-%d-sequence-%02d", input.Seed, next()))
				_, err := node.Apply(command, scenarioTimeout)
				errors <- err
			}
			go apply(sequence.TakeSplit)
			go apply(sequence.TakeAtomic)
			for range input.CommandCount {
				if err := <-errors; err != nil {
					t.Fatalf("node %s: concurrent apply: %v", node.ID, err)
				}
			}
		}

		history := node.WaitForCommands(t, input.CommandCount, scenarioTimeout)
		seen := make(map[string]bool, input.CommandCount)
		for _, entry := range history {
			seen[string(entry.Data)] = true
		}
		for i := range input.CommandCount {
			command := fmt.Sprintf("attempt-%d-sequence-%02d", input.Seed, i)
			if !seen[command] {
				t.Errorf("node %s did not apply %q", node.ID, command)
			}
		}
	}, WithSeed(input.Seed), WithConfig(func(config *raft.Config) {
		config.HeartbeatTimeout = 100 * time.Millisecond
		config.ElectionTimeout = 100 * time.Millisecond
	}))
}

func decodeScenarioInput(raw json.RawMessage) (scenarioInput, error) {
	var input scenarioInput
	if err := json.Unmarshal(raw, &input); err != nil {
		return input, fmt.Errorf("decode Raft scenario input: %w", err)
	}
	if input.Seed < 1 {
		return input, fmt.Errorf("scenario seed must be positive")
	}
	return input, nil
}
