package orchestrator

// CHESS implements context-bounded DFS exploration (the algorithm from
// the CHESS paper). It systematically explores scheduling interleavings
// by replaying prefixes and branching at decision points where
// alternatives exist.
type CHESS struct {
	// Bound is the max non-FIFO decisions per trace (default 2).
	Bound int

	// GlobalOnly restricts branching to Global decisions only.
	// When true, local decisions always return FIFO (index 0), and
	// prefixes contain only Global steps.
	GlobalOnly bool

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

// RunNode records one run in the exploration tree for benchmarking.
type RunNode struct {
	Run        int
	Parent     int // index of parent RunNode, -1 for root
	BranchStep int // which trace step diverged from parent
	NonFIFO    int
	Passed     bool
	UserFailed bool
	Diverged   bool
	TraceLen   int
}

// BeforeRun prepares the next run. Returns false when exploration is done.
func (s *CHESS) BeforeRun() bool {
	if s.Bound == 0 {
		s.Bound = 2
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
	s.prefixIdx = 0
	return true
}

// Decide follows the replay prefix, then returns 0 (FIFO) at the frontier.
//
// In GlobalOnly mode, local decisions always return 0 and don't consume
// prefix entries. The prefix contains only Global steps.
//
// In non-GlobalOnly mode, the prefix contains the full unified trace.
// Mismatches (due to varying local decision counts) panic with
// replayDivergenceError, which the orchestrator catches and prunes.
func (s *CHESS) Decide(dp DecisionPoint) int {
	if s.GlobalOnly && dp.Kind == Local {
		return 0 // FIFO for local in GlobalOnly mode
	}
	if s.prefixIdx < len(s.current.prefix) {
		p := s.current.prefix[s.prefixIdx]
		if p.Kind != dp.Kind || p.Node != dp.Node {
			panic(replayDivergenceError{Step: dp.Step})
		}
		if int(p.Index) >= dp.N() {
			panic(replayDivergenceError{Step: dp.Step, Index: int(p.Index), QueueSize: dp.N()})
		}
		s.prefixIdx++
		return int(p.Index)
	}
	// At frontier: FIFO.
	return 0
}

// AfterRun records the run and pushes unexplored alternatives onto the stack.
func (s *CHESS) AfterRun(result RunResult) {
	s.count++

	nodeIdx := len(s.tree)
	s.tree = append(s.tree, RunNode{
		Run:        s.count,
		Parent:     s.current.parent,
		BranchStep: s.current.branch,
		NonFIFO:    s.current.nonFIFO,
		Passed:     result.Passed,
		UserFailed: result.UserFailed,
		Diverged:   result.Diverged,
		TraceLen:   len(result.Trace),
	})

	if result.Diverged {
		return
	}

	if s.GlobalOnly {
		s.pushGlobalOnlyAlts(result, nodeIdx)
	} else {
		s.pushAllAlts(result, nodeIdx)
	}
}

// pushGlobalOnlyAlts pushes alternatives from only the global steps.
// The prefix contains only Global steps, so replaying is deterministic
// (local scheduling decisions don't affect prefix alignment).
func (s *CHESS) pushGlobalOnlyAlts(result RunResult, nodeIdx int) {
	// Extract global steps from the trace.
	var globalSteps Trace
	for _, step := range result.Trace {
		if step.Kind == Global {
			globalSteps = append(globalSteps, step)
		}
	}

	for i := len(globalSteps) - 1; i >= len(s.current.prefix); i-- {
		step := globalSteps[i]
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
			newPrefix := make(Trace, i+1)
			copy(newPrefix, globalSteps[:i])
			newPrefix[i] = NewStep{
				Kind:         Global,
				Index:        alt,
				Alternatives: step.Alternatives,
			}
			s.stack = append(s.stack, chessItem{
				prefix:  newPrefix,
				nonFIFO: newNonFIFO,
				parent:  nodeIdx,
				branch:  i,
			})
		}
	}
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
				newPrefix := make(Trace, i+1)
				copy(newPrefix, result.Trace[:i])
				newPrefix[i] = NewStep{
					Kind:         step.Kind,
					Node:         step.Node,
					Index:        alt,
					Alternatives: step.Alternatives,
				}
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
