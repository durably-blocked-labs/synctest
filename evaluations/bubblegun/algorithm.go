package bubblegun

import (
	"encoding/json"
	"fmt"

	"github.com/shubhaankar/synctest/orchestrator"
)

func newAlgorithm(config AlgorithmConfig, seed int64) (orchestrator.CheckpointableAlgorithm, error) {
	bound := config.ContextBound
	if bound == 0 {
		bound = 2
	}

	switch config.Name {
	case AlgorithmFIFO:
		return &fifoAlgorithm{}, nil
	case AlgorithmCHESS:
		return &orchestrator.CHESS{Bound: bound, GlobalOnly: config.GlobalOnly}, nil
	case AlgorithmDPOR:
		return &orchestrator.DPOR{
			Bound:              bound,
			GlobalOnly:         config.GlobalOnly,
			ConservativeGlobal: config.ConservativeGlobal,
		}, nil
	case AlgorithmPCT:
		return &orchestrator.PCT{
			Depth: config.Depth, MaxSteps: config.MaxSteps, Seed: seed,
			GlobalOnly: config.GlobalOnly,
		}, nil
	case AlgorithmRandom:
		return &orchestrator.Random{Seed: seed, GlobalOnly: config.GlobalOnly}, nil
	default:
		return nil, fmt.Errorf("unknown algorithm %q", config.Name)
	}
}

type fifoAlgorithm struct {
	completed bool
}

func (f *fifoAlgorithm) BeforeRun() bool {
	return !f.completed
}

func (f *fifoAlgorithm) Decide(orchestrator.DecisionPoint) int {
	return 0
}

func (f *fifoAlgorithm) AfterRun(orchestrator.RunResult) {
	f.completed = true
}

func (f *fifoAlgorithm) MarshalCheckpoint() ([]byte, error) {
	return json.Marshal(struct {
		Completed bool `json:"completed"`
	}{Completed: f.completed})
}

func (f *fifoAlgorithm) UnmarshalCheckpoint(data []byte) error {
	var checkpoint struct {
		Completed bool `json:"completed"`
	}
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return fmt.Errorf("decode FIFO checkpoint: %w", err)
	}
	f.completed = checkpoint.Completed
	return nil
}
