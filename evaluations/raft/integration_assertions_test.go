package rafteval_test

import (
	"fmt"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

// A pruned replay is an attempted run, not a completed workload. Infrastructure
// and resource outcomes must never satisfy an API test just because Failed is 0.
func validateCompletedExploration(result orchestrator.ExplorationResult, minimum, maximum int) error {
	completed := result.Runs - result.Diverged
	if result.Failed != 0 || result.Incomplete != 0 || result.Cause != nil ||
		result.Diverged < 0 || completed < minimum || result.Runs > maximum {
		return fmt.Errorf("runs=%d completed=%d failed=%d incomplete=%d diverged=%d cause=%v",
			result.Runs, completed, result.Failed, result.Incomplete, result.Diverged, result.Cause)
	}
	return nil
}

func TestCompletedExplorationRejectsFalsePasses(t *testing.T) {
	for _, item := range []struct {
		name   string
		result orchestrator.ExplorationResult
	}{
		{"incomplete", orchestrator.ExplorationResult{Runs: 1, Incomplete: 1}},
		{"cause", orchestrator.ExplorationResult{Runs: 1, Cause: fmt.Errorf("framework failure")}},
		{"pruned", orchestrator.ExplorationResult{Runs: 1, Diverged: 1}},
		{"assertion", orchestrator.ExplorationResult{Runs: 1, Failed: 1}},
		{"no-workload", orchestrator.ExplorationResult{}},
	} {
		t.Run(item.name, func(t *testing.T) {
			if err := validateCompletedExploration(item.result, 1, 1); err == nil {
				t.Fatal("accepted an unsuccessful API workload")
			}
		})
	}
	if err := validateCompletedExploration(orchestrator.ExplorationResult{Runs: 1}, 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := validateCompletedExploration(orchestrator.ExplorationResult{Runs: 3, Diverged: 1}, 2, 3); err != nil {
		t.Fatal("legitimate pruning erased two completed workloads:", err)
	}
}
