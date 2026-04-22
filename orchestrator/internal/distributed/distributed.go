// Package distributed provides the local orchestrator for synctest bubbles
// in distributed system tests.
//
// A Bubble wraps one synctest bubble. It handles scheduling decisions
// internally via a synchronous callback (scheduleFn). It only contacts
// the global orchestrator when the bubble goes IDLE.
//
// Local scheduling decisions MUST be synchronous (no channel hop) because
// the hook runs during rootInHook on the bubble P. Blocking on a channel
// would extend the rootInHook window, allowing cross-P goready to deposit
// goroutines on the bubble P's runqueue — causing pidleput crashes on
// bubble teardown.
package distributed

import (
	"math/rand"
	"testing/synctest"
)

// BubbleState is the state of a bubble at a hook invocation.
type BubbleState = synctest.BubbleState

// IdleState is sent to the global orchestrator when the bubble goes idle.
type IdleState struct {
	Name  string
	State BubbleState
}

// Resume is the global orchestrator's instruction after receiving an IdleState.
type Resume struct {
	AdvanceTimeTo int64
	DelegateIdle  bool
}

// LocalStep records one local scheduling decision with enriched detail.
type LocalStep struct {
	Index        int32
	Alternatives int32
	ChosenBGID   uint32
	RunqBGIDs    []uint32
}

// Bubble is the local orchestrator for one synctest bubble.
type Bubble struct {
	Name string

	// Idle is read by the global orchestrator.
	Idle <-chan IdleState

	// Resume is written by the global orchestrator.
	Resume chan<- Resume

	// Internal.
	idle   chan IdleState
	resume chan Resume

	// scheduleFn is called synchronously in the hook for every local
	// scheduling decision. Set by the orchestrator to wrap Algorithm.Decide.
	// If nil, returns 0 (FIFO).
	scheduleFn func(BubbleState) int32

	seed    int64
	hasSeed bool

	// Local decision log, drained by the orchestrator at idle/done points.
	localLog    []LocalStep
	lastDrained int
}

// Option configures a Bubble.
type Option func(*Bubble)

// WithScheduler sets a custom scheduling function for local decisions.
func WithScheduler(fn func(BubbleState) int32) Option {
	return func(b *Bubble) { b.scheduleFn = fn }
}

// WithSeed sets the global rand seed at the start of the bubble.
func WithSeed(seed int64) Option {
	return func(b *Bubble) { b.seed = seed; b.hasSeed = true }
}

// NewBubble creates a local orchestrator for one node.
func NewBubble(name string, opts ...Option) *Bubble {
	idle := make(chan IdleState, 1)
	resume := make(chan Resume, 1)
	b := &Bubble{
		Name:   name,
		Idle:   idle,
		Resume: resume,
		idle:   idle,
		resume: resume,
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// SetScheduleFn replaces the scheduling callback. Called by the orchestrator
// after NewBubble to wire up Algorithm.Decide for exploration.
func (b *Bubble) SetScheduleFn(fn func(BubbleState) int32) {
	b.scheduleFn = fn
}

// Hook returns the decision hook function for this bubble.
//
// For scheduling decisions (Idle == false): handled synchronously via
// schedule() — no channel hop, no cross-M blocking.
// For idle (Idle == true): sends state to global orchestrator, waits for Resume.
func (b *Bubble) Hook() func(BubbleState) int32 {
	seeded := false
	return func(state BubbleState) int32 {
		if !seeded && b.hasSeed {
			rand.Seed(b.seed) //nolint:staticcheck
			seeded = true
		}
		if !state.Idle {
			return b.schedule(state)
		}
		// Idle — ask the global orchestrator.
		b.idle <- IdleState{Name: b.Name, State: state}
		r := <-b.resume
		if r.AdvanceTimeTo > 0 {
			synctest.SetTime(r.AdvanceTimeTo)
		}
		if r.DelegateIdle {
			return -1
		}
		return 0
	}
}

// schedule handles a non-idle scheduling decision synchronously.
func (b *Bubble) schedule(state BubbleState) int32 {
	var idx int32
	if b.scheduleFn != nil {
		idx = b.scheduleFn(state)
	}
	// Record enriched local step.
	bgids := make([]uint32, state.RunnableN)
	for i := int32(0); i < state.RunnableN; i++ {
		bgids[i] = state.RunnableBgid[i]
	}
	b.localLog = append(b.localLog, LocalStep{
		Index:        idx,
		Alternatives: state.RunnableN,
		ChosenBGID:   state.RunnableBgid[idx],
		RunqBGIDs:    bgids,
	})
	return idx
}

// DrainLocal returns local scheduling decisions recorded since the last drain.
// Called by the orchestrator after each idle/done event to build the unified trace.
func (b *Bubble) DrainLocal() []LocalStep {
	steps := b.localLog[b.lastDrained:]
	b.lastDrained = len(b.localLog)
	out := make([]LocalStep, len(steps))
	copy(out, steps)
	return out
}

// HasNewLocal returns true if local scheduling decisions have been recorded
// since the last drain. Used to detect spurious idle signals.
func (b *Bubble) HasNewLocal() bool {
	return len(b.localLog) > b.lastDrained
}
