// Package distributed provides the local orchestrator for synctest bubbles
// in distributed system tests.
//
// A Bubble wraps one synctest bubble. It handles scheduling decisions
// internally (FIFO or prefix replay). It only contacts the global
// orchestrator when the bubble goes IDLE — when all goroutines are
// blocked and the bubble has nothing left to do on its own.
//
// When idle, the Bubble sends its state (timers, blocked goroutines,
// ExternalWait count) on the Idle channel. The global orchestrator
// reads this, decides to either deliver a network message or advance
// time, does that, then sends a Resume to unblock the hook.
//
// The bubble then resumes automatically — the delivered message wakes
// a goroutine, or the advanced time fires a timer. The local
// orchestrator handles the resulting scheduling decisions internally
// until the bubble goes idle again.
package distributed

import (
	"math/rand"
	"testing/synctest"
)

// BubbleState is the state of a bubble at a hook invocation.
type BubbleState = synctest.BubbleState

// IdleState is sent to the global orchestrator when the bubble goes idle.
type IdleState struct {
	Name  string      // which node
	State BubbleState // full state: timers, ExternalWait, blocked count, etc.
}

// Resume is the global orchestrator's instruction after receiving an IdleState.
//
// The orchestrator should do ONE of:
//   - Deliver a network message (write to the transport's inbox channel),
//     then send Resume{} — the bubble resumes when the woken goroutine runs.
//   - Advance time: send Resume{AdvanceTimeTo: t} — the bubble fires timers
//     whose deadline <= t and resumes if any goroutine wakes.
//   - Both: deliver a message AND advance time.
type Resume struct {
	// AdvanceTimeTo advances the bubble's fake clock before returning
	// from the hook. 0 means don't advance.
	AdvanceTimeTo int64

	// DelegateIdle tells the bubble to hand control back to the local runtime
	// idle/drain logic after returning from the hook instead of immediately
	// re-entering the orchestrator handshake.
	DelegateIdle bool
}

// LocalStep records one local scheduling decision made by the bubble's hook.
// Used by the global orchestrator to build the chronological unified trace.
type LocalStep struct {
	Index        int32 // which goroutine was chosen (0 = FIFO)
	Alternatives int32 // how many runnable goroutines existed (RunqSize)
}

// Bubble is the local orchestrator for one synctest bubble.
//
// Scheduling decisions are handled internally (FIFO by default, or
// following a prefix for replay). The global orchestrator only sees
// idle notifications.
type Bubble struct {
	Name string

	// Idle is read by the global orchestrator.
	// An IdleState appears here when the bubble has nothing left to do.
	// The orchestrator must respond on Resume for each IdleState received.
	Idle <-chan IdleState

	// Resume is written by the global orchestrator.
	// Send exactly one Resume for each IdleState received.
	Resume chan<- Resume

	// Internal.
	idle   chan IdleState
	resume chan Resume

	// Scheduling: prefix for replay, step counter.
	prefix     []synctest.Decision
	prefixStep int

	// Optional: custom scheduling function for non-replay scenarios.
	// If nil, uses FIFO (index 0) at the frontier.
	scheduleFn func(BubbleState) int32

	// Seed for the global rand source. Set at the start of Hook().
	seed    int64
	hasSeed bool

	// Local decision log for building the unified exploration trace.
	// Appended by schedule(), drained by the orchestrator after each
	// idle point via DrainLocal().
	localLog    []LocalStep
	lastDrained int
}

// Option configures a Bubble.
type Option func(*Bubble)

// WithPrefix sets a scheduling prefix for replay. The hook follows
// these decisions before defaulting to FIFO at the frontier.
func WithPrefix(prefix []synctest.Decision) Option {
	return func(b *Bubble) { b.prefix = prefix }
}

// WithScheduler sets a custom scheduling function for frontier decisions.
// Called when the prefix is exhausted and the runq has choices.
func WithScheduler(fn func(BubbleState) int32) Option {
	return func(b *Bubble) { b.scheduleFn = fn }
}

// WithSeed sets the global rand seed at the start of the bubble.
// This controls the P's fastrand, which determines:
//   - select statement ordering (which case wins when multiple are ready)
//   - raft election timeout jitter (randomTimeout)
//
// Different seeds explore different select interleavings within the bubble.
// Requires GODEBUG=randseednop=0 to take effect (Go 1.24+).
func WithSeed(seed int64) Option {
	return func(b *Bubble) { b.seed = seed; b.hasSeed = true }
}

// NewBubble creates a local orchestrator for one node.
// Called outside the bubble by the global orchestrator.
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

// Hook returns the decision hook function for this bubble.
// Pass it to synctest.SetDecisionHook inside the bubble.
//
// For scheduling decisions (Idle == false): handled locally.
// For idle (Idle == true): sends state to global orchestrator, waits for Resume.
//
// If WithSeed was set, the seed is applied on the first hook call
// (inside the bubble, before any raft rand calls).
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

// schedule handles a non-idle scheduling decision locally.
func (b *Bubble) schedule(state BubbleState) int32 {
	var idx int32
	// Follow prefix if available.
	if b.prefixStep < len(b.prefix) {
		idx = b.prefix[b.prefixStep].Index
		b.prefixStep++
	} else if b.scheduleFn != nil {
		// Custom scheduler if set.
		idx = b.scheduleFn(state)
	}
	// Record for the unified trace.
	b.localLog = append(b.localLog, LocalStep{Index: idx, Alternatives: state.RunnableN})
	return idx
}

// DrainLocal returns local scheduling decisions recorded since the last drain.
// Called by the global orchestrator after each idle/done event to build the
// chronological unified trace. Safe to call only when the bubble is frozen
// in its hook (not concurrently with schedule).
func (b *Bubble) DrainLocal() []LocalStep {
	steps := b.localLog[b.lastDrained:]
	b.lastDrained = len(b.localLog)
	out := make([]LocalStep, len(steps))
	copy(out, steps)
	return out
}
