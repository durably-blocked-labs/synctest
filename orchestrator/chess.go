package orchestrator

import (
	"encoding/json"
	"fmt"
)

// CHESS implements CHESS-inspired bounded DFS exploration. Bound limits
// non-default choices per trace, rather than context switches or instruction
// preemptions. The search replays prefixes and branches at exposed decision
// points where alternatives exist.
type CHESS struct {
	// Explicit retained search-state safety caps. Zero uses bounded defaults.
	MaxFrontier      int
	MaxFrontierSteps int
	MaxTree          int
	frontierSteps    int
	budgetErr        error

	// Bound is the max non-FIFO decisions per trace; zero keeps the FIFO baseline.
	Bound int

	stack     []chessItem
	current   chessItem
	prefixIdx int // position within current.prefix during replay
	tree      []RunNode
	count     int
}

type chessItem struct {
	prefix  Trace
	nonFIFO int
	parent  int // index in tree, -1 for root
	branch  int // which step was changed
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

var _ CheckpointableAlgorithm = (*CHESS)(nil)

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
	candidate := *s
	candidate.MaxFrontier = checkpointCap(s.MaxFrontier, 4096)
	candidate.MaxFrontierSteps = checkpointCap(s.MaxFrontierSteps, 1<<20)
	candidate.MaxTree = checkpointCap(s.MaxTree, 10000)
	if candidate.Bound < 0 || len(checkpoint.Stack) > candidate.MaxFrontier {
		return fmt.Errorf("checkpoint exceeds CHESS bound/frontier limit")
	}
	if err := validateCheckpointTree(checkpoint.Count, checkpoint.Tree, candidate.MaxTree); err != nil {
		return err
	}
	steps := 0
	stack := make([]chessItem, len(checkpoint.Stack))
	for i, item := range checkpoint.Stack {
		if len(item.Prefix) > candidate.MaxFrontierSteps-steps {
			return fmt.Errorf("checkpoint exceeds CHESS frontier-step limit")
		}
		if err := validateCheckpointPrefix(item.Prefix, item.NonFIFO, item.Parent, item.Branch, candidate.Bound, checkpoint.Tree); err != nil {
			return err
		}
		steps += len(item.Prefix)
		stack[i] = chessItem{
			prefix:  item.Prefix,
			nonFIFO: item.NonFIFO,
			parent:  item.Parent,
			branch:  item.Branch,
		}
	}
	candidate.stack = stack
	candidate.count = checkpoint.Count
	candidate.tree = checkpoint.Tree
	candidate.current = chessItem{}
	candidate.prefixIdx = 0
	candidate.frontierSteps = steps
	candidate.budgetErr = nil
	*s = candidate
	return nil
}

// RunNode records one run in the exploration tree for benchmarking.
type RunNode struct {
	Run        int
	Parent     int // index of parent RunNode, -1 for root
	BranchStep int // which trace step diverged from parent
	NonFIFO    int
	Passed     bool
	TraceLen   int
}

// BeforeRun prepares the next run. Returns false when exploration is done.
func (s *CHESS) BeforeRun() bool {
	if s.budgetErr != nil {
		return false
	}
	if s.MaxFrontier <= 0 {
		s.MaxFrontier = 4096
	}
	if s.MaxFrontierSteps <= 0 {
		s.MaxFrontierSteps = 1 << 20
	}
	if s.MaxTree <= 0 {
		s.MaxTree = 10000
	}
	if s.count == 0 {
		// Seed with empty prefix (FIFO baseline).
		s.stack = []chessItem{{prefix: nil, nonFIFO: 0, parent: -1, branch: -1}}
	}
	if len(s.stack) == 0 {
		return false
	}
	s.current = s.stack[len(s.stack)-1]
	s.stack = s.stack[:len(s.stack)-1]
	s.frontierSteps -= len(s.current.prefix)
	s.prefixIdx = 0
	return true
}

// Decide follows the replay prefix, then returns 0 (FIFO) at the frontier.
// Mismatches due to varying decision counts panic with
// replayDivergenceError, which the orchestrator catches and prunes.
func (s *CHESS) Decide(dp DecisionPoint) int {
	if s.prefixIdx < len(s.current.prefix) {
		p := s.current.prefix[s.prefixIdx]
		if err := matchStep(p, dp, false); err != nil {
			panic(replayDivergenceError{Step: dp.Step, Detail: err.Error()})
		}
		s.prefixIdx++
		return int(p.Index)
	}
	// At frontier: FIFO.
	return 0
}

// AfterRun records the run and pushes unexplored alternatives onto the stack.
func (s *CHESS) AfterRun(result RunResult) {
	if len(s.tree) >= s.MaxTree {
		s.budgetErr = ResourceLimitError{Resource: "search tree", Limit: s.MaxTree}
		return
	}
	s.count++

	nodeIdx := len(s.tree)
	s.tree = append(s.tree, RunNode{
		Run:        s.count,
		Parent:     s.current.parent,
		BranchStep: s.current.branch,
		NonFIFO:    s.current.nonFIFO,
		Passed:     result.Passed,
		TraceLen:   len(result.Trace),
	})

	if result.Diverged {
		return
	}

	s.pushAllAlts(result, nodeIdx)
}

// pushAllAlts pushes alternatives from all steps (both global and local).
// Two passes: local first (explored later in LIFO), global second (explored first).
func (s *CHESS) pushAllAlts(result RunResult, nodeIdx int) {
	pushAlts := func(wantGlobal bool) {
		for i := len(result.Trace) - 1; i >= len(s.current.prefix); i-- {
			step := result.Trace[i]
			isGlobal := step.Kind == Global
			if wantGlobal != isGlobal {
				continue
			}
			if step.Alternatives <= 1 {
				continue
			}
			for alt := int32(1); alt < step.Alternatives; alt++ {
				if alt == step.Index {
					continue
				}
				newNonFIFO := s.current.nonFIFO + 1
				if newNonFIFO > s.Bound {
					continue
				}
				if !s.reservePrefix(i + 1) {
					return
				}
				newPrefix := make(Trace, i+1)
				copy(newPrefix, result.Trace[:i])
				newPrefix[i] = branchStep(step, alt)
				s.stack = append(s.stack, chessItem{
					prefix:  newPrefix,
					nonFIFO: newNonFIFO,
					parent:  nodeIdx,
					branch:  i,
				})
			}
		}
	}
	pushAlts(false) // local (explored later)
	pushAlts(true)  // global (explored first)
}

// Tree returns the exploration tree for benchmarking export.
func (s *CHESS) Tree() []RunNode {
	return s.tree
}

// Count returns the number of runs completed so far.
func (s *CHESS) Count() int {
	return s.count
}

func (s *CHESS) reservePrefix(steps int) bool {
	if len(s.stack) >= s.MaxFrontier || s.frontierSteps+steps > s.MaxFrontierSteps {
		s.budgetErr = ResourceLimitError{Resource: "search frontier", Limit: s.MaxFrontierSteps}
		return false
	}
	s.frontierSteps += steps
	return true
}
func (s *CHESS) SearchError() error { return s.budgetErr }
