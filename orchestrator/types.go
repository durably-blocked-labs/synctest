package orchestrator

import "time"

// DecisionKind classifies the type of scheduling decision.
type DecisionKind uint8

const (
	Local  DecisionKind = iota // within a bubble: which goroutine runs next
	Global                     // all bubbles idle: which message to deliver
	Select                     // enabled select case; NOT a goroutine choice
)

// Alt describes one choosable option at a decision point.
type Alt struct {
	// Stable identifier within one run. Format: "B5" for local goroutine,
	// "msg:A->B(Request)" for global message. Used as map key by PCT
	// priorities and DPOR dependence. Always set, cheap.
	ID string

	// Occurrence distinguishes repeated messages of the same class. PCT uses ID.
	Occurrence string
	CaseIndex  int32 // runtime select partition index, not source case order

	// -- Global (Kind == Global) --
	From    string // sender node
	To      string // target node (also the "resource" for DPOR)
	MsgType string // RPC type name

	// -- Local (Kind == Local) --
	BGID    uint32  // bubble goroutine ID
	SpawnPC uintptr // PC of the go statement that spawned this goroutine
}

// DecisionPoint is what the algorithm sees at each scheduling choice.
// Ephemeral -- lives only during the Decide call.
type DecisionPoint struct {
	Kind  DecisionKind
	Step  int    // 0-indexed position in the unified trace
	Node  string // bubble name for Local, "" for Global
	Alts  []Alt  // choosable alternatives (len >= 1)
	Site  string // stable same-binary select site key, empty for scheduling
	Actor uint32 // select actor, never a case identity
}

// N returns the number of alternatives.
func (dp DecisionPoint) N() int { return len(dp.Alts) }

// NewStep is the compact record of one decision in a trace.
// Stored after the run. Does NOT carry the full []Alt -- that's ephemeral.
type NewStep struct {
	Kind             DecisionKind
	Node             string // "" for global
	Index            int32  // which alternative was chosen (0 = default/FIFO)
	Alternatives     int32  // how many choices existed
	ChosenID         string // Alt.ID of the chosen alternative
	Resource         string // for DPOR: "from|to" for global, node for local
	AltIDs           []string
	AltResources     []string
	AltOccurrences   []string
	ChosenOccurrence string
	Site             string
	Actor            uint32
	SelectCases      []int32

	// -- Global detail (populated only for Global steps) --
	From    string // sender node
	To      string // target node
	MsgType string // RPC type name

	// -- Local detail (populated only for Local steps) --
	ChosenBGID uint32   // BGID of chosen goroutine
	RunqBGIDs  []uint32 // all runnable BGIDs at this decision
}

// Trace is the complete sequence of decisions from one run.
type Trace []NewStep

// RunResult is the outcome of one execution.
type RunResult struct {
	Trace          Trace
	Passed         bool
	Diverged       bool
	UserFailed     bool          // true when t.Errorf/t.Fatalf was called (not just deadlock)
	DivergenceStep int           // -1 if no divergence
	Elapsed        time.Duration // wall clock for this run

	// GlobalTrace preserved for metrics.go compatibility.
	// Contains delivery events, time advances, and done markers.
	GlobalTrace []GlobalStep

	Outcome          Outcome
	SelectControlled bool
	Cause            any  // exact in-process cause; artifacts persist a diagnostic separately
	WorkerUnsafe     bool // process must terminate; no subsequent setup/reset
	TeardownError    error
	PersistenceError error
}

// Outcome is a terminal execution classification, not a correctness proof.
type Outcome string

const (
	OutcomePassed         Outcome = "passed"
	OutcomeAssertion      Outcome = "assertion-failed"
	OutcomeDiverged       Outcome = "diverged"
	OutcomeInfrastructure Outcome = "infrastructure-failed"
	OutcomeResourceLimit  Outcome = "resource-limit"
	OutcomePanic          Outcome = "panic"
	OutcomeCancelled      Outcome = "cancelled"
	OutcomeUnsupported    Outcome = "unsupported"
)

// Successful excludes incomplete executions even when no workload bug was found.
func (r ExplorationResult) Successful() bool { return r.Failed == 0 && r.Incomplete == 0 }

// Algorithm controls multi-run exploration.
//
// The orchestrator calls BeforeRun before each execution. If it returns
// false, exploration is done. During the run, Decide is called at every
// decision point -- global and local. After the run, AfterRun receives
// the result.
//
// Decide may panic with replayDivergenceError to signal that the
// execution has diverged from the expected prefix. The orchestrator
// catches this panic, marks the run as diverged, and calls AfterRun
// with Diverged=true.
type Algorithm interface {
	BeforeRun() bool
	Decide(dp DecisionPoint) int
	AfterRun(result RunResult)
}

// ExplorationResult summarizes a multi-run exploration.
type ExplorationResult struct {
	Runs       int
	Failed     int
	FirstBug   int           // run number of first failure, -1 if none
	Elapsed    time.Duration // total wall clock
	Incomplete int           // infrastructure/resource/unclassified failures, never success
	Diverged   int
	Cause      any
}
