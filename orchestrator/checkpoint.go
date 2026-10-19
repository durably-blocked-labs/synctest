package orchestrator

import (
	"encoding/json"
	"fmt"
)

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

type chessCheckpointItem struct {
	Prefix  Trace `json:"prefix"`
	NonFIFO int   `json:"non_fifo"`
	Parent  int   `json:"parent"`
	Branch  int   `json:"branch"`
}

type chessCheckpoint struct {
	checkpointMetadata
	Stack []chessCheckpointItem `json:"stack"`
	Count int                   `json:"count"`
	Tree  []RunNode             `json:"tree"`
}

func (s *CHESS) MarshalCheckpoint() ([]byte, error) {
	stack := make([]chessCheckpointItem, len(s.stack))
	for i, item := range s.stack {
		stack[i] = chessCheckpointItem{
			Prefix:  item.prefix,
			NonFIFO: item.nonFIFO,
			Parent:  item.parent,
			Branch:  item.branch,
		}
	}
	return json.Marshal(chessCheckpoint{
		checkpointMetadata: checkpointMetadata{Version: checkpointVersion, Algorithm: "chess"},
		Stack:              stack,
		Count:              s.count,
		Tree:               s.tree,
	})
}

func (s *CHESS) UnmarshalCheckpoint(data []byte) error {
	var checkpoint chessCheckpoint
	if err := validateCheckpoint(data, "chess", &checkpoint); err != nil {
		return err
	}
	stack := make([]chessItem, len(checkpoint.Stack))
	for i, item := range checkpoint.Stack {
		stack[i] = chessItem{
			prefix:  item.Prefix,
			nonFIFO: item.NonFIFO,
			parent:  item.Parent,
			branch:  item.Branch,
		}
	}
	s.stack = stack
	s.count = checkpoint.Count
	s.tree = checkpoint.Tree
	s.current = chessItem{}
	s.prefixIdx = 0
	return nil
}

type dporCheckpointItem struct {
	Prefix  Trace `json:"prefix"`
	NonFIFO int   `json:"non_fifo"`
	Parent  int   `json:"parent"`
	Branch  int   `json:"branch"`
	Score   int   `json:"score"`
}

type dporCheckpoint struct {
	checkpointMetadata
	Stack []dporCheckpointItem `json:"stack"`
	Seen  map[string]struct{}  `json:"seen"`
	Count int                  `json:"count"`
	Tree  []RunNode            `json:"tree"`
}

func (d *DPOR) MarshalCheckpoint() ([]byte, error) {
	stack := make([]dporCheckpointItem, len(d.stack))
	for i, item := range d.stack {
		stack[i] = dporCheckpointItem{
			Prefix:  item.prefix,
			NonFIFO: item.nonFIFO,
			Parent:  item.parent,
			Branch:  item.branch,
			Score:   item.score,
		}
	}
	return json.Marshal(dporCheckpoint{
		checkpointMetadata: checkpointMetadata{Version: checkpointVersion, Algorithm: "dpor"},
		Stack:              stack,
		Seen:               d.seen,
		Count:              d.count,
		Tree:               d.tree,
	})
}

func (d *DPOR) UnmarshalCheckpoint(data []byte) error {
	var checkpoint dporCheckpoint
	if err := validateCheckpoint(data, "dpor", &checkpoint); err != nil {
		return err
	}
	stack := make([]dporItem, len(checkpoint.Stack))
	for i, item := range checkpoint.Stack {
		stack[i] = dporItem{
			prefix:  item.Prefix,
			nonFIFO: item.NonFIFO,
			parent:  item.Parent,
			branch:  item.Branch,
			score:   item.Score,
		}
	}
	d.stack = stack
	d.seen = checkpoint.Seen
	d.count = checkpoint.Count
	d.tree = checkpoint.Tree
	d.current = dporItem{}
	d.prefixIdx = 0
	return nil
}

type countCheckpoint struct {
	checkpointMetadata
	Count int `json:"count"`
}

func (p *PCT) MarshalCheckpoint() ([]byte, error) {
	return json.Marshal(countCheckpoint{
		checkpointMetadata: checkpointMetadata{Version: checkpointVersion, Algorithm: "pct"},
		Count:              p.count,
	})
}

func (p *PCT) UnmarshalCheckpoint(data []byte) error {
	var checkpoint countCheckpoint
	if err := validateCheckpoint(data, "pct", &checkpoint); err != nil {
		return err
	}
	p.count = checkpoint.Count
	p.rng = nil
	p.priority = nil
	p.nextLow = 0
	p.changePoints = nil
	p.step = 0
	return nil
}

func (r *Random) MarshalCheckpoint() ([]byte, error) {
	return json.Marshal(countCheckpoint{
		checkpointMetadata: checkpointMetadata{Version: checkpointVersion, Algorithm: "random"},
		Count:              r.count,
	})
}

func (r *Random) UnmarshalCheckpoint(data []byte) error {
	var checkpoint countCheckpoint
	if err := validateCheckpoint(data, "random", &checkpoint); err != nil {
		return err
	}
	r.count = checkpoint.Count
	r.rng = nil
	return nil
}

var (
	_ CheckpointableAlgorithm = (*CHESS)(nil)
	_ CheckpointableAlgorithm = (*DPOR)(nil)
	_ CheckpointableAlgorithm = (*PCT)(nil)
	_ CheckpointableAlgorithm = (*Random)(nil)
)
