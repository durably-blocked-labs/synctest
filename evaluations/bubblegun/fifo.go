package bubblegun

import (
	"encoding/json"
	"fmt"

	"github.com/shubhaankar/synctest/orchestrator"
)

type fifoAlgorithm struct {
	completed bool
}

var _ orchestrator.CheckpointableAlgorithm = (*fifoAlgorithm)(nil)

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
