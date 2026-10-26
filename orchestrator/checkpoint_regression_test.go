package orchestrator

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

func checkpointPolicy(name string) CheckpointableAlgorithm {
	if name == "chess" {
		return &CHESS{Bound: 2}
	}
	return &DPOR{Bound: 2}
}

func checkpointTrace(a Algorithm) Trace {
	var trace Trace
	for i := 0; i < 2; i++ {
		dp := DecisionPoint{Kind: Local, Node: "node", Step: i,
			Alts: []Alt{{ID: "B1", BGID: 1}, {ID: "B2", BGID: 2}, {ID: "B3", BGID: 3}}}
		trace = append(trace, stepForDecision(dp, a.Decide(dp)))
	}
	return trace
}

func seededCheckpointPolicy(t *testing.T, name string) CheckpointableAlgorithm {
	t.Helper()
	a := checkpointPolicy(name)
	if !a.BeforeRun() {
		t.Fatal("initial run missing")
	}
	a.AfterRun(RunResult{Passed: true, Trace: checkpointTrace(a)})
	return a
}

func assertCheckpointAccounting(t *testing.T, a CheckpointableAlgorithm) {
	t.Helper()
	actual, sum := 0, 0
	switch a := a.(type) {
	case *CHESS:
		actual = a.frontierSteps
		for _, item := range a.stack {
			sum += len(item.prefix)
		}
	case *DPOR:
		actual = a.frontierSteps
		for _, item := range a.stack {
			sum += len(item.prefix)
		}
	}
	if actual != sum || actual < 0 {
		t.Fatalf("frontier accounting %d, actual retained steps %d", actual, sum)
	}
}

func TestCheckpointRestoreMatchesUninterruptedContinuation(t *testing.T) {
	for _, name := range []string{"chess", "dpor"} {
		t.Run(name, func(t *testing.T) {
			original := seededCheckpointPolicy(t, name)
			data, err := original.MarshalCheckpoint()
			if err != nil {
				t.Fatal(err)
			}
			restored := checkpointPolicy(name)
			if err := restored.UnmarshalCheckpoint(data); err != nil {
				t.Fatal(err)
			}
			assertCheckpointAccounting(t, restored)
			for run := 0; run < 50; run++ {
				left, right := original.BeforeRun(), restored.BeforeRun()
				if left != right {
					t.Fatal("restored frontier ended at a different point")
				}
				if !left {
					return
				}
				a, b := checkpointTrace(original), checkpointTrace(restored)
				if !reflect.DeepEqual(a, b) {
					t.Fatalf("continuation %d changed", run)
				}
				original.AfterRun(RunResult{Passed: true, Trace: a})
				restored.AfterRun(RunResult{Passed: true, Trace: b})
				assertCheckpointAccounting(t, restored)
				first, _ := original.MarshalCheckpoint()
				second, _ := restored.MarshalCheckpoint()
				if string(first) != string(second) {
					t.Fatal("restored policy state differs after continuation")
				}
			}
			t.Fatal("finite continuation did not finish")
		})
	}
}

func TestCheckpointRestoresAfterShortDivergedAttempt(t *testing.T) {
	for _, name := range []string{"chess", "dpor"} {
		t.Run(name, func(t *testing.T) {
			a := seededCheckpointPolicy(t, name)
			if !a.BeforeRun() {
				t.Fatal("missing non-FIFO prefix")
			}
			a.AfterRun(RunResult{Diverged: true})
			data, err := a.MarshalCheckpoint()
			if err != nil {
				t.Fatal(err)
			}
			restored := checkpointPolicy(name)
			if err := restored.UnmarshalCheckpoint(data); err != nil {
				t.Fatal("valid pruned-attempt history was rejected:", err)
			}
			assertCheckpointAccounting(t, restored)
			if a.BeforeRun() != restored.BeforeRun() {
				t.Fatal("restored pruned frontier changed")
			}
		})
	}
}

func TestCheckpointRejectsCorruptionAndCapsAtomically(t *testing.T) {
	for _, name := range []string{"chess", "dpor"} {
		t.Run(name, func(t *testing.T) {
			source := seededCheckpointPolicy(t, name)
			valid, err := source.MarshalCheckpoint()
			if err != nil {
				t.Fatal(err)
			}
			for _, corruption := range []string{"negative-count", "tree-count", "unknown-field", "prefix-index", "nonfifo", "parent", "frontier-cap", "step-cap", "tree-cap", "seen"} {
				if corruption == "seen" && name != "dpor" {
					continue
				}
				t.Run(corruption, func(t *testing.T) {
					var payload map[string]any
					if err := json.Unmarshal(valid, &payload); err != nil {
						t.Fatal(err)
					}
					receiver := seededCheckpointPolicy(t, name)
					switch corruption {
					case "negative-count":
						payload["count"] = -1
					case "tree-count":
						payload["count"] = 2
					case "unknown-field":
						payload["invented"] = true
					case "prefix-index":
						payload["stack"].([]any)[0].(map[string]any)["prefix"].([]any)[0].(map[string]any)["Index"] = 99
					case "nonfifo":
						payload["stack"].([]any)[0].(map[string]any)["non_fifo"] = 99
					case "parent":
						payload["stack"].([]any)[0].(map[string]any)["parent"] = 99
					case "frontier-cap":
						switch a := receiver.(type) {
						case *CHESS:
							a.MaxFrontier = 1
						case *DPOR:
							a.MaxFrontier = 1
						}
					case "step-cap":
						switch a := receiver.(type) {
						case *CHESS:
							a.MaxFrontierSteps = 1
						case *DPOR:
							a.MaxFrontierSteps = 1
						}
					case "tree-cap":
						// A second completed tree node exceeds an explicit one-node cap.
						payload["tree"] = append(payload["tree"].([]any), map[string]any{"Run": 2, "Parent": 0, "BranchStep": 0, "NonFIFO": 1, "TraceLen": 2})
						payload["count"] = 2
						switch a := receiver.(type) {
						case *CHESS:
							a.MaxTree = 1
						case *DPOR:
							a.MaxTree = 1
						}
					case "seen":
						payload["seen"] = map[string]any{}
					}
					before, _ := receiver.MarshalCheckpoint()
					var original any
					switch a := receiver.(type) {
					case *CHESS:
						original = *a
					case *DPOR:
						original = *a
					}
					data, _ := json.Marshal(payload)
					if err := receiver.UnmarshalCheckpoint(data); err == nil {
						t.Fatalf("accepted %s corruption", corruption)
					}
					after, _ := receiver.MarshalCheckpoint()
					if string(before) != string(after) {
						t.Fatal("failed restore changed serialized receiver")
					}
					var unchanged bool
					switch a := receiver.(type) {
					case *CHESS:
						unchanged = reflect.DeepEqual(original, *a)
					case *DPOR:
						unchanged = reflect.DeepEqual(original, *a)
					}
					if !unchanged {
						t.Fatal("failed restore changed caps or transient state")
					}
				})
			}
		})
	}
}

func TestCheckpointRestoreRecomputesExactCapAndResetsTransientState(t *testing.T) {
	for _, name := range []string{"chess", "dpor"} {
		t.Run(name, func(t *testing.T) {
			source := seededCheckpointPolicy(t, name)
			data, _ := source.MarshalCheckpoint()
			receiver := checkpointPolicy(name)
			switch a := receiver.(type) {
			case *CHESS:
				a.MaxFrontierSteps, a.frontierSteps, a.prefixIdx, a.budgetErr = 6, 999, 3, fmt.Errorf("old terminal")
			case *DPOR:
				a.MaxFrontierSteps, a.frontierSteps, a.prefixIdx, a.budgetErr, a.frontierCalled = 6, 999, 3, fmt.Errorf("old terminal"), true
			}
			if err := receiver.UnmarshalCheckpoint(data); err != nil {
				t.Fatal(err)
			}
			assertCheckpointAccounting(t, receiver)
			if err := receiver.(interface{ SearchError() error }).SearchError(); err != nil {
				t.Fatal("stale terminal state retained")
			}
			if !receiver.BeforeRun() {
				t.Fatal("valid restored frontier not continued")
			}
			assertCheckpointAccounting(t, receiver)
		})
	}
}
