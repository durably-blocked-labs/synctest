package orchestrator

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Checkpoints use versioned JSON so campaign state remains inspectable and can
// be embedded directly in the repository's existing JSON artifacts.
const checkpointVersion = 1

// CheckpointableAlgorithm is an Algorithm whose between-run exploration state
// can be saved and restored.
// Callers must bind checkpoints to the same effective policy configuration,
// workload and runtime identity. This format does not restore live goroutines
// or reconcile a checkpoint with a separately persisted run log after a crash.
type CheckpointableAlgorithm interface {
	Algorithm
	MarshalCheckpoint() ([]byte, error)
	UnmarshalCheckpoint(data []byte) error
}

type checkpointMetadata struct {
	Version   int    `json:"version"`
	Algorithm string `json:"algorithm"`
}

func validateCheckpoint(data []byte, algorithm string, dst any) error {
	if len(data) > 64<<20 {
		return fmt.Errorf("%s checkpoint exceeds 64 MiB", algorithm)
	}
	var metadata struct {
		checkpointMetadata
		Count *int
	}
	if err := json.Unmarshal(data, &metadata); err != nil {
		return fmt.Errorf("decode %s checkpoint: %w", algorithm, err)
	}
	if metadata.Version != checkpointVersion {
		return fmt.Errorf("unsupported %s checkpoint version %d", algorithm, metadata.Version)
	}
	if metadata.Algorithm != algorithm {
		return fmt.Errorf("checkpoint algorithm %q cannot be restored into %s", metadata.Algorithm, algorithm)
	}
	if metadata.Count == nil || *metadata.Count < 0 {
		return fmt.Errorf("invalid %s checkpoint count", algorithm)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return fmt.Errorf("decode %s checkpoint: %w", algorithm, err)
	}
	return nil
}

func checkpointCap(value, fallback int) int {
	if value <= 0 {
		return fallback
	}
	return value
}

func validateCheckpointTree(count int, tree []RunNode, limit int) error {
	if count != len(tree) || len(tree) > limit {
		return fmt.Errorf("checkpoint tree/count exceeds or disagrees with the configured tree limit")
	}
	for i, node := range tree {
		// CHESS records the requested prefix's count even when divergence
		// produces a shorter trace. It need not fit the observed TraceLen.
		if node.Run != i+1 || node.TraceLen < 0 || node.NonFIFO < 0 {
			return fmt.Errorf("invalid checkpoint tree node %d", i)
		}
		if node.Parent < -1 || node.Parent >= i || (node.Parent == -1 && node.BranchStep != -1) ||
			(node.Parent >= 0 && (node.BranchStep < 0 || node.BranchStep >= tree[node.Parent].TraceLen)) {
			return fmt.Errorf("invalid checkpoint tree edge %d", i)
		}
	}
	return nil
}

func validateCheckpointPrefix(prefix Trace, nonFIFO, parent, branch, bound int, tree []RunNode) error {
	if nonFIFO < 0 || nonFIFO > bound || nonFIFO != countTraceNonFIFO(prefix) {
		return fmt.Errorf("checkpoint prefix disagrees with its non-FIFO bound/count")
	}
	if len(prefix) == 0 {
		if parent != -1 || branch != -1 || len(tree) != 0 {
			return fmt.Errorf("invalid empty checkpoint prefix")
		}
	} else if parent < 0 || parent >= len(tree) || branch != len(prefix)-1 || branch >= tree[parent].TraceLen {
		return fmt.Errorf("invalid checkpoint prefix parent/branch")
	}
	for i, step := range prefix {
		if step.Kind > Select || step.Alternatives < 1 || step.Index < 0 || step.Index >= step.Alternatives {
			return fmt.Errorf("invalid checkpoint decision %d", i)
		}
		for _, n := range []int{len(step.AltIDs), len(step.AltResources), len(step.AltOccurrences), len(step.RunqBGIDs)} {
			if n != 0 && n != int(step.Alternatives) {
				return fmt.Errorf("invalid checkpoint decision cardinality %d", i)
			}
		}
		if len(step.AltIDs) > 0 && step.ChosenID != step.AltIDs[step.Index] {
			return fmt.Errorf("invalid checkpoint chosen identity %d", i)
		}
		if len(step.AltOccurrences) > 0 && step.ChosenOccurrence != step.AltOccurrences[step.Index] {
			return fmt.Errorf("invalid checkpoint occurrence %d", i)
		}
		if step.Kind == Select && (len(step.SelectCases) != int(step.Alternatives) || step.Site == "" || step.Actor == 0) {
			return fmt.Errorf("invalid checkpoint select identity %d", i)
		}
	}
	return nil
}
