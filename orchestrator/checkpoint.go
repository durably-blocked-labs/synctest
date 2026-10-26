package orchestrator

import (
	"encoding/json"
	"fmt"
)

// Checkpoints use versioned JSON so campaign state remains inspectable and can
// be embedded directly in the repository's existing JSON artifacts.
const checkpointVersion = 1

// CheckpointableAlgorithm is an Algorithm whose between-run exploration state
// can be saved and restored.
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
	var metadata checkpointMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return fmt.Errorf("decode %s checkpoint: %w", algorithm, err)
	}
	if metadata.Version != checkpointVersion {
		return fmt.Errorf("unsupported %s checkpoint version %d", algorithm, metadata.Version)
	}
	if metadata.Algorithm != algorithm {
		return fmt.Errorf("checkpoint algorithm %q cannot be restored into %s", metadata.Algorithm, algorithm)
	}
	if err := json.Unmarshal(data, dst); err != nil {
		return fmt.Errorf("decode %s checkpoint: %w", algorithm, err)
	}
	return nil
}
