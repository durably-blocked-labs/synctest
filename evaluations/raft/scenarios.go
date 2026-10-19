package rafteval

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/shubhaankar/synctest/evaluations/framework"
	"github.com/shubhaankar/synctest/orchestrator"
)

const scenarioTimeout = 5 * time.Second

type scenarioInput struct {
	Seed         int64 `json:"seed"`
	CommandCount int   `json:"command_count,omitempty"`
}

func RegisterScenarios(registry *framework.Registry) error {
	scenarios := []framework.Scenario{
		newLeaderElectionScenario(),
		newReplicateCommandScenario(),
		newConcurrentAppliesScenario(),
	}
	for _, scenario := range scenarios {
		if err := registry.RegisterScenario(scenario); err != nil {
			return err
		}
	}
	return nil
}

func newLeaderElectionScenario() framework.Scenario {
	return framework.Scenario{
		Name: "raft/leader-election",
		Generate: func(attempt int) (json.RawMessage, error) {
			return json.Marshal(scenarioInput{Seed: int64(attempt)})
		},
		Setup: func(raw json.RawMessage) (func(*orchestrator.Orchestrator), error) {
			input, err := decodeScenarioInput(raw)
			if err != nil {
				return nil, err
			}
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
			}, WithSeed(input.Seed)), nil
		},
	}
}

func newReplicateCommandScenario() framework.Scenario {
	return framework.Scenario{
		Name: "raft/replicate-command",
		Generate: func(attempt int) (json.RawMessage, error) {
			return json.Marshal(scenarioInput{Seed: int64(attempt), CommandCount: 1})
		},
		Setup: func(raw json.RawMessage) (func(*orchestrator.Orchestrator), error) {
			input, err := decodeScenarioInput(raw)
			if err != nil {
				return nil, err
			}
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
			}, WithSeed(input.Seed)), nil
		},
	}
}

func newConcurrentAppliesScenario() framework.Scenario {
	return framework.Scenario{
		Name: "raft/concurrent-applies",
		Generate: func(attempt int) (json.RawMessage, error) {
			return json.Marshal(scenarioInput{Seed: int64(attempt), CommandCount: 4})
		},
		Setup: func(raw json.RawMessage) (func(*orchestrator.Orchestrator), error) {
			input, err := decodeScenarioInput(raw)
			if err != nil {
				return nil, err
			}
			return concurrentScenarioSetup(input), nil
		},
	}
}

func concurrentScenarioSetup(input scenarioInput) func(*orchestrator.Orchestrator) {
	return NewClusterSetup(3, func(t *testing.T, node *Node) {
		node.WaitForLeader(t, scenarioTimeout)
		if node.IsLeader() {
			var wait sync.WaitGroup
			errors := make(chan error, input.CommandCount)
			for i := 0; i < input.CommandCount; i++ {
				command := []byte(fmt.Sprintf("attempt-%d-command-%02d", input.Seed, i))
				wait.Add(1)
				go func() {
					defer wait.Done()
					_, err := node.Apply(command, scenarioTimeout)
					errors <- err
				}()
			}
			wait.Wait()
			close(errors)
			for err := range errors {
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
		for i := 0; i < input.CommandCount; i++ {
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
