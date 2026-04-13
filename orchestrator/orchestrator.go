// Package orchestrator coordinates multiple synctest bubbles acting as
// distributed nodes that communicate via controlled message delivery.
//
// Each node runs in its own synctest bubble with a distributed.Bubble local
// orchestrator managing intra-bubble scheduling. The global orchestrator
// only intervenes when ALL bubbles are idle — when every goroutine in every
// bubble is blocked waiting on orchestrator-controlled channels (ExternalWait).
//
// At each idle point the global orchestrator:
//  1. Drains each node's outbox to collect pending OpSend operations.
//  2. Picks the next op to execute (FIFO for Run; recorded order for Replay).
//  3. Calls op.Execute() to deliver the message to the target's mailbox.
//  4. Sends Resume{} to the target bubble so its bridge goroutine wakes.
//
// When no messages are pending, the orchestrator advances the global virtual
// clock to the earliest pending timer across all bubbles and resumes every
// bubble with Resume{AdvanceTimeTo: t}.
//
// Core invariant: at most one bubble is active (not frozen in its hook) at
// any time, guaranteeing fully deterministic cross-node scheduling.
package orchestrator

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/shubhaankar/synctest/orchestrator/internal/distributed"
)

// OpDir classifies the direction of a pending operation.
type OpDir int

const (
	OpSend OpDir = iota
)

// PendingOp is the unit of work submitted by a transport to the orchestrator.
// In the ExternalWait model only OpSend ops are submitted. The Execute closure
// delivers the message directly into the target node's mailbox channel.
type PendingOp struct {
	Dir     OpDir
	From    string // sender addr
	To      string // target addr
	Type    string // RPC type name for tracing ("AppendEntries", etc.)
	Execute func() // writes message to target's mailbox
}

// NodeTransport is the interface transport implementations must satisfy.
type NodeTransport interface {
	Addr() string
	Outbox() <-chan *PendingOp
}

// shutdowner is optionally implemented by transports to support graceful
// shutdown during divergence recovery and drain.
type shutdowner interface{ Shutdown() }

// GlobalStepType classifies what happened in one orchestrator loop iteration.
type GlobalStepType int

const (
	StepDeliver     GlobalStepType = iota // an op was executed
	StepTimeAdvance                       // global virtual clock advanced
	StepDone                              // a node's bubble completed
)

// GlobalStep records one orchestrator event.
type GlobalStep struct {
	Type   GlobalStepType
	Dir    OpDir
	From   string
	To     string
	OpType string
	Time   int64
}

// GlobalDecision records one global scheduling choice during exploration.
// Index is which op in the schedulable queue was delivered (0 = FIFO).
// QueueSize is the number of schedulable ops available at that point.
type GlobalDecision struct {
	Index     int // index into schedulable queue (0 = FIFO)
	QueueSize int // number of schedulable ops at this step
}

// Step is one entry in the unified exploration trace. Global delivery
// decisions have Node == ""; local scheduling decisions have Node set
// to the node address. The same DFS branching logic handles both.
type Step struct {
	Node         string // "" = global delivery, non-empty = local scheduling
	Index        int32  // chosen alternative (0 = FIFO)
	Alternatives int32  // how many choices existed at this point
}

// RunObserver is called once after each run inside Explore.
// Useful for exporting metrics without changing Explore's control flow.
type RunObserver func(runNum int, nonFIFO int, elapsed time.Duration, rec RecordedRun, passed bool)

// ExploreOption configures Explore behavior.
type ExploreOption func(*exploreConfig)

type exploreConfig struct {
	bound    int         // max non-FIFO delivery decisions per trace (default 2)
	maxRuns  int         // hard cap on total runs (0 = unlimited)
	observer RunObserver // optional per-run callback
}

// GlobalBound sets the maximum number of non-FIFO delivery decisions per
// explored trace. Defaults to 2.
func GlobalBound(k int) ExploreOption { return func(c *exploreConfig) { c.bound = k } }

// GlobalMaxRuns sets a hard cap on the total number of traces explored.
// Defaults to 0 (unlimited).
func GlobalMaxRuns(n int) ExploreOption { return func(c *exploreConfig) { c.maxRuns = n } }

// WithObserver registers a callback invoked once after each Explore run.
func WithObserver(fn RunObserver) ExploreOption { return func(c *exploreConfig) { c.observer = fn } }

// RecordedRun captures a complete orchestrator run for deterministic replay.
//
// GlobalTrace is the sequence of global events (message deliveries, time
// advances, node completions). GlobalDecisions records the index-based
// delivery choices (one per delivery step). LocalTraces contains the per-node
// scheduling decisions recorded inside each bubble. Together they fully
// specify the execution and can be fed back into Replay to reproduce it.
type RecordedRun struct {
	GlobalTrace     []GlobalStep
	GlobalDecisions []GlobalDecision
	LocalTraces     map[string][]synctest.Decision

	// Trace is the unified chronological sequence of global delivery
	// and local scheduling decisions, interleaved in the order they
	// occurred. Used by ExploreAll for DFS branching.
	Trace []Step
}

type replayDivergenceError struct {
	Step      int
	Index     int
	QueueSize int
}

func (e replayDivergenceError) Error() string {
	return fmt.Sprintf("orchestrator replay divergence at step %d: chose index %d with queue size %d", e.Step, e.Index, e.QueueSize)
}

// nodeCtrl is the orchestrator's per-node state.
// The bubble field is nil until Run or Replay initialises it.
type nodeCtrl struct {
	transport  NodeTransport
	testFunc   func(t *testing.T)
	bubbleOpts []distributed.Option // per-node options (e.g. WithScheduler)
	bubble     *distributed.Bubble  // assigned in initBubbles; nil after AddNode
	done       chan struct{}
	localTrace []synctest.Decision // captured when bubble completes
	passed     bool                // whether the bubble's test passed
}

// Orchestrator manages nodes and controls cross-node message delivery.
type Orchestrator struct {
	nodes      map[string]*nodeCtrl
	order      []string // insertion order for deterministic iteration
	globalTime int64

	schedulable []*PendingOp // ops ready to execute

	trace []GlobalStep
}

// NodeOption is a per-node configuration option for AddNode.
type NodeOption = distributed.Option

// WithScheduler sets a custom local scheduling function for a node's bubble.
// The function is called at each scheduling decision point (when multiple
// goroutines are runnable). It receives the bubble state and returns the
// index of the goroutine to run (0 = FIFO default).
func WithScheduler(fn func(distributed.BubbleState) int32) NodeOption {
	return distributed.WithScheduler(fn)
}

// BubbleState is the state of a bubble at a scheduling decision point.
type BubbleState = distributed.BubbleState

// New creates an empty orchestrator.
func New() *Orchestrator {
	return &Orchestrator{
		nodes: make(map[string]*nodeCtrl),
	}
}

// AddNode registers a node transport and its bubble test function with
// optional per-node distributed.Options (e.g. WithScheduler).
// The bubble is created lazily when Run or Replay is called.
func (o *Orchestrator) AddNode(transport NodeTransport, f func(t *testing.T), opts ...distributed.Option) {
	addr := transport.Addr()
	ctrl := &nodeCtrl{
		transport:  transport,
		testFunc:   f,
		bubbleOpts: opts,
	}
	o.nodes[addr] = ctrl
	o.order = append(o.order, addr)
}

// initBubbles (re-)creates all per-node bubbles.
// localPrefixes maps node address → local scheduling prefix to replay;
// a nil map creates fresh bubbles with default FIFO scheduling.
// Per-node bubbleOpts are prepended before any prefix option, so
// WithScheduler takes effect at the frontier.
func (o *Orchestrator) initBubbles(localPrefixes map[string][]synctest.Decision) {
	for _, addr := range o.order {
		ctrl := o.nodes[addr]
		// Start with per-node options (e.g. WithScheduler).
		opts := append([]distributed.Option{}, ctrl.bubbleOpts...)
		if localPrefixes != nil {
			if prefix, ok := localPrefixes[addr]; ok && len(prefix) > 0 {
				opts = append(opts, distributed.WithPrefix(prefix))
			}
		}
		ctrl.bubble = distributed.NewBubble(addr, opts...)
		ctrl.done = make(chan struct{})
		ctrl.localTrace = nil
		ctrl.passed = false
	}
}

// startBubble launches a node's test function inside a synctest bubble using
// synctest.Explore so the local scheduling trace is captured without calling
// t.FailNow() mid-goroutine. Failures are propagated via t.Fail().
func (o *Orchestrator) startBubble(t *testing.T, ctrl *nodeCtrl) {
	b := ctrl.bubble
	userFn := ctrl.testFunc
	go func() {
		defer close(ctrl.done)
		trace, ok := synctest.Explore(t, func(t *testing.T) {
			synctest.SetDecisionHook(b.Hook())
			userFn(t)
		}, nil)
		ctrl.localTrace = trace
		ctrl.passed = ok
	}()
}

// pickFn selects which op to deliver from the schedulable queue.
// Returns the index into schedulable. Called once per delivery step.
type pickFn func(schedulable []*PendingOp, step int) int

// Run starts all registered nodes and drives cross-node message delivery until
// all bubbles complete. Delivery order is FIFO over the ops drained from each
// node's outbox. Returns a RecordedRun that can be fed into Replay.
func (o *Orchestrator) Run(t *testing.T) (RecordedRun, bool) {
	t.Helper()
	o.initBubbles(nil)
	rec, ok := o.runOnce(t, func(_ []*PendingOp, _ int) int { return 0 })
	return rec, ok
}

// Replay re-runs all registered nodes, replaying the local scheduling
// decisions from rec.LocalTraces and following rec.GlobalDecisions for
// cross-node message delivery order. Both global and local behaviour should
// be identical to the original run.
//
// Replay requires a freshly configured Orchestrator (same AddNode calls as
// the original run, but new transport instances with the same connectivity).
func (o *Orchestrator) Replay(t *testing.T, rec RecordedRun) (RecordedRun, bool) {
	t.Helper()
	o.initBubbles(rec.LocalTraces)
	pick := func(schedulable []*PendingOp, step int) int {
		if step < len(rec.GlobalDecisions) {
			d := rec.GlobalDecisions[step]
			if d.QueueSize != len(schedulable) {
				panic(replayDivergenceError{
					Step:      step,
					Index:     d.Index,
					QueueSize: len(schedulable),
				})
			}
			return d.Index
		}
		panic(replayDivergenceError{
			Step:      step,
			Index:     -1,
			QueueSize: 0,
		})
	}
	rec2, ok := o.runOnce(t, pick)
	return rec2, ok
}

// exploreItem is one pending work item on the DFS stack.
type exploreItem struct {
	prefix  []GlobalDecision
	nonFIFO int // number of non-FIFO decisions in this prefix
}

// Explore enumerates different global message delivery orderings using DFS
// with context bounding, stopping at the first interleaving that fails.
//
// setup is called before each run to register fresh nodes (transports + bubble
// funcs). It must call o.AddNode for every node, exactly as one would before
// calling Run. Explore resets o.nodes and o.order before each setup call so
// the orchestrator is clean.
//
// Options:
//
//	GlobalBound(k)   — max non-FIFO delivery choices per trace (default 2)
//	GlobalMaxRuns(n) — hard cap on total traces explored (default unlimited)
//
// Returns true if all explored interleavings passed, false on the first failure.
func (o *Orchestrator) Explore(t *testing.T, setup func(*Orchestrator), opts ...ExploreOption) bool {
	t.Helper()
	cfg := &exploreConfig{bound: 2}
	verbose := testing.Verbose()
	for _, opt := range opts {
		opt(cfg)
	}

	stack := []exploreItem{{prefix: nil, nonFIFO: 0}}
	runCount := 0

	for len(stack) > 0 && (cfg.maxRuns == 0 || runCount < cfg.maxRuns) {
		// Pop from stack (DFS: last in, first out).
		item := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		// Reset orchestrator state and re-register nodes via setup.
		o.nodes = make(map[string]*nodeCtrl)
		o.order = nil
		setup(o)
		o.initBubbles(nil)

		// Force a GC before starting the next run. Each run ends with
		// synctest.Explore returning, which allocates bubble bookkeeping inside
		// the runtime and can leave GC workers interacting with stale bubble P
		// state. Running GC here flushes those workers before any new bubbles
		// are created.
		runtime.GC()

		prefix := item.prefix
		pick := func(schedulable []*PendingOp, step int) int {
			if step < len(prefix) {
				d := prefix[step]
				if d.QueueSize != len(schedulable) {
					panic(replayDivergenceError{
						Step:      step,
						Index:     d.Index,
						QueueSize: len(schedulable),
					})
				}
				return d.Index
			}
			return 0
		}
		runStart := time.Now()
		rec, passed, diverged := o.runOnceForExplore(t, pick)
		if diverged != nil {
			if verbose {
				t.Logf("orchestrator: explore: pruned invalid prefix %v (%v)", item.prefix, *diverged)
			}
			continue
		}
		runCount++
		if cfg.observer != nil {
			cfg.observer(runCount, item.nonFIFO, time.Since(runStart), rec, passed)
		}

		// Log a compact delivery sequence so different orderings are visible.
		if verbose {
			var sb strings.Builder
			for _, step := range o.trace {
				if step.Type == StepDeliver {
					if sb.Len() > 0 {
						sb.WriteByte(' ')
					}
					fmt.Fprintf(&sb, "%s→%s(%s)", step.From, step.To, step.OpType)
				}
			}
			t.Logf("orchestrator: explore: run %d prefix=%v\n\ttrace: %s", runCount, item.prefix, sb.String())
		}

		if !passed {
			t.Logf("orchestrator: explore: FAILED on run %d (prefix len %d)", runCount, len(item.prefix))
			return false
		}

		// Enqueue new work items for unexplored alternatives past the current prefix.
		// Push in reverse step order so that earlier-step alternatives sit on top
		// of the stack and are explored first. This prioritises orderings that
		// diverge from FIFO at the earliest possible point, which is where most
		// distributed-protocol violations manifest.
		for step := len(rec.GlobalDecisions) - 1; step >= len(item.prefix); step-- {
			d := rec.GlobalDecisions[step]
			if d.QueueSize <= 1 {
				continue
			}
			for alt := 1; alt < d.QueueSize; alt++ {
				newNonFIFO := item.nonFIFO + 1 // alt > 0, so always non-FIFO
				if newNonFIFO > cfg.bound {
					continue
				}
				newPrefix := make([]GlobalDecision, step+1)
				copy(newPrefix, rec.GlobalDecisions[:step])
				newPrefix[step] = GlobalDecision{Index: alt, QueueSize: d.QueueSize}
				stack = append(stack, exploreItem{prefix: newPrefix, nonFIFO: newNonFIFO})
			}
		}
	}

	if verbose {
		t.Logf("orchestrator: explore: explored %d interleavings, all passed", runCount)
	}
	return true
}


// splitPrefix decomposes a unified trace into separate global and per-node
// local prefixes suitable for initBubbles and the pick function.
func splitPrefix(steps []Step) ([]GlobalDecision, map[string][]synctest.Decision) {
	var global []GlobalDecision
	local := map[string][]synctest.Decision{}
	for _, s := range steps {
		if s.Node == "" {
			global = append(global, GlobalDecision{Index: int(s.Index), QueueSize: int(s.Alternatives)})
		} else {
			local[s.Node] = append(local[s.Node], synctest.Decision{Index: s.Index, RunqSize: s.Alternatives})
		}
	}
	return global, local
}

// ExploreAll enumerates different interleavings across both global message
// delivery orderings AND per-node local goroutine scheduling using DFS with
// a shared context bound.
//
// Unlike Explore (global-only), ExploreAll uses a unified chronological
// trace that interleaves global delivery and local scheduling decisions.
// Branching at any step N truncates the trace — no stale local prefixes
// are carried across global reorderings. This creates a proper exploration
// tree suitable for CHESS-style algorithms.
//
// setup is called before each run to register fresh nodes. Options:
//
//	GlobalBound(k)   — max non-FIFO decisions per trace (default 2)
//	GlobalMaxRuns(n) — hard cap on total traces (default unlimited)
//
// Returns true if all explored interleavings passed, false on the first failure.
func (o *Orchestrator) ExploreAll(t *testing.T, setup func(*Orchestrator), opts ...ExploreOption) bool {
	t.Helper()
	cfg := &exploreConfig{bound: 2}
	verbose := testing.Verbose()
	for _, opt := range opts {
		opt(cfg)
	}

	type workItem struct {
		prefix  []Step
		nonFIFO int
	}

	stack := []workItem{{prefix: nil, nonFIFO: 0}}
	runCount := 0

	for len(stack) > 0 && (cfg.maxRuns == 0 || runCount < cfg.maxRuns) {
		item := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		// Reset orchestrator state and re-register nodes via setup.
		o.nodes = make(map[string]*nodeCtrl)
		o.order = nil
		setup(o)

		// Decompose the unified prefix into global + per-node local prefixes
		// for the runtime's two-layer interface.
		globalPrefix, localPrefixes := splitPrefix(item.prefix)
		o.initBubbles(localPrefixes)

		// Force GC to flush mark workers from the previous bubble before
		// creating a fresh set of distributed bubbles.
		runtime.GC()

		pick := func(schedulable []*PendingOp, step int) int {
			if step < len(globalPrefix) {
				d := globalPrefix[step]
				if d.QueueSize != len(schedulable) {
					panic(replayDivergenceError{
						Step:      step,
						Index:     d.Index,
						QueueSize: len(schedulable),
					})
				}
				return d.Index
			}
			return 0
		}
		runStart := time.Now()
		rec, passed, diverged := o.runOnceForExplore(t, pick)
		if diverged != nil {
			if verbose {
				t.Logf("orchestrator: exploreAll: pruned (prefix len %d, %v)", len(item.prefix), *diverged)
			}
			continue
		}
		runCount++
		if cfg.observer != nil {
			cfg.observer(runCount, item.nonFIFO, time.Since(runStart), rec, passed)
		}

		// Log a compact delivery sequence.
		if verbose {
			var sb strings.Builder
			for _, step := range o.trace {
				if step.Type == StepDeliver {
					if sb.Len() > 0 {
						sb.WriteByte(' ')
					}
					fmt.Fprintf(&sb, "%s→%s(%s)", step.From, step.To, step.OpType)
				}
			}
			t.Logf("orchestrator: exploreAll: run %d prefixLen=%d nonFIFO=%d\n\ttrace: %s",
				runCount, len(item.prefix), item.nonFIFO, sb.String())
		}

		if !passed {
			t.Logf("orchestrator: exploreAll: FAILED on run %d", runCount)
			return false
		}

		// Enumerate alternatives from the unified trace in two passes.
		// Pass 1 (local) is pushed first → explored later (deeper in LIFO stack).
		// Pass 2 (global) is pushed second → explored first (top of stack).
		// This prioritises global delivery reorderings, which fundamentally
		// alter protocol execution, over local goroutine scheduling variants.
		pushAlternatives := func(globalOnly bool) {
			for i := len(rec.Trace) - 1; i >= len(item.prefix); i-- {
				s := rec.Trace[i]
				isGlobal := s.Node == ""
				if globalOnly != isGlobal {
					continue
				}
				if s.Alternatives <= 1 {
					continue
				}
				for alt := int32(1); alt < s.Alternatives; alt++ {
					if alt == s.Index {
						continue // skip the choice already taken
					}
					newNonFIFO := item.nonFIFO + 1
					if newNonFIFO > cfg.bound {
						continue
					}
					// Truncate at step i and replace with the alternative.
					// Everything after i is discarded — no stale prefixes.
					newPrefix := make([]Step, i+1)
					copy(newPrefix, rec.Trace[:i])
					newPrefix[i] = Step{Node: s.Node, Index: alt, Alternatives: s.Alternatives}
					stack = append(stack, workItem{prefix: newPrefix, nonFIFO: newNonFIFO})
				}
			}
		}
		pushAlternatives(false) // local alternatives (explored later)
		pushAlternatives(true)  // global alternatives (explored first)
	}

	if verbose {
		t.Logf("orchestrator: exploreAll: explored %d interleavings, all passed", runCount)
	}
	return true
}

func (o *Orchestrator) runOnceForExplore(t *testing.T, pick pickFn) (rec RecordedRun, passed bool, diverged *replayDivergenceError) {
	defer func() {
		if r := recover(); r != nil {
			if d, ok := r.(replayDivergenceError); ok {
				diverged = &d
				o.cleanupBubbles()
				return
			}
			panic(r)
		}
	}()
	rec, passed = o.runOnce(t, pick)
	return rec, passed, nil
}



// cleanupBubbles tears down orphaned bubbles after a divergence panic.
// It signals transports to close (unblocking bridge goroutines in
// ExternalWait), drains idle channels so hooks don't block on send,
// sends DelegateIdle resumes to unblock roots frozen in their hooks,
// and waits for each bubble to terminate.
func (o *Orchestrator) cleanupBubbles() {
	// Phase 1: shut down all transports (non-blocking).
	// Closes closeCh, causing bridge goroutines to exit ExternalWait.
	for _, addr := range o.order {
		ctrl := o.nodes[addr]
		if s, ok := ctrl.transport.(shutdowner); ok {
			s.Shutdown()
		}
	}
	// Phase 2: drain idle channels and send DelegateIdle.
	// The hook sends to b.idle BEFORE reading b.resume. If the idle
	// channel is full (buffer=1, previous idle not consumed), the hook
	// blocks on the send and never reads our Resume. Drain idle first
	// to guarantee the hook can complete its send and read Resume.
	for _, addr := range o.order {
		ctrl := o.nodes[addr]
		if ctrl.bubble == nil {
			continue
		}
		// Drain any buffered idle state.
		select {
		case <-ctrl.bubble.Idle:
		default:
		}
		// Send DelegateIdle. The hook may not be blocked yet (bubble
		// still running), so use non-blocking send — the buffered
		// channel (cap=1) holds it until the hook reads.
		select {
		case ctrl.bubble.Resume <- distributed.Resume{DelegateIdle: true}:
		default:
		}
	}
	// Phase 3: wait for bubbles to finish.
	// With delegateIdle persistent (no longer cleared on generic wake)
	// and transports shut down (externalWait → 0), bubbles should
	// terminate via deadlock detection almost immediately. Use a short
	// grace period for any remaining cleanup.
	for _, addr := range o.order {
		ctrl := o.nodes[addr]
		if ctrl.done == nil {
			continue
		}
		select {
		case <-ctrl.done:
		case <-time.After(1 * time.Second):
		}
	}
}

// bubbleEvent is one event from a fan-in goroutine: either idle or done.
type bubbleEvent struct {
	addr string
	idle *distributed.IdleState
	done bool
}

// waitNodeEvent blocks until a specific node reports idle or completes.
// Reading directly from per-node channels (no fan-in) guarantees
// deterministic event processing in o.order.
func waitNodeEvent(addr string, ctrl *nodeCtrl) bubbleEvent {
	select {
	case idle := <-ctrl.bubble.Idle:
		idleCopy := idle
		return bubbleEvent{addr: addr, idle: &idleCopy}
	case <-ctrl.done:
		return bubbleEvent{addr: addr, done: true}
	}
}

// runOnce is the unified implementation for Run, Replay, and Explore.
//
// pick selects which op to deliver at each step. For Run it always returns 0
// (FIFO). For Replay it follows rec.GlobalDecisions. For Explore it follows
// the prefix then defaults to 0.
//
// Algorithm:
//  1. Start all bubbles.
//  2. Wait for every active, non-pending-idle bubble to report idle (IdleState)
//     or to complete (done channel).
//  3. Once all active bubbles are idle, drain their outboxes to collect pending
//     OpSend operations.
//  4. If sends are available: deliver the next one (chosen by pick), resume target.
//  5. If no sends but timers exist: advance global time, resume all idle bubbles.
//  6. If neither: resume all (allowing bubbles to finish) and return.
//  7. Repeat from step 2.
func (o *Orchestrator) runOnce(t *testing.T, pick pickFn) (RecordedRun, bool) {
	// Reset orchestrator run state.
	o.trace = nil
	o.schedulable = nil
	o.globalTime = 0
	verbose := testing.Verbose()

	n := len(o.nodes)
	var globalDecisions []GlobalDecision
	var unified []Step // chronological unified trace (global + local interleaved)

	// pendingIdle holds bubbles that sent an IdleState but have not yet
	// received a Resume. They are frozen in their hook.
	pendingIdle := make(map[string]distributed.IdleState)
	doneSet := make(map[string]bool)
	active := n

	// drainLocalSteps appends a node's new local scheduling decisions to
	// the unified trace. Called when a bubble goes idle or done.
	drainLocalSteps := func(addr string) {
		ctrl := o.nodes[addr]
		if ctrl.bubble == nil {
			return
		}
		for _, d := range ctrl.bubble.DrainLocal() {
			unified = append(unified, Step{Node: addr, Index: d.Index, Alternatives: d.Alternatives})
		}
	}

	markDone := func(addr, reason string) {
		if doneSet[addr] {
			return
		}
		drainLocalSteps(addr)
		o.drainOutbox(addr)
		delete(pendingIdle, addr)
		doneSet[addr] = true
		active--
		o.trace = append(o.trace, GlobalStep{Type: StepDone, From: addr})
		if verbose {
			t.Logf("orchestrator: node %q bubble done %s", addr, reason)
		}
	}

	recordIdle := func(addr string, idle distributed.IdleState) {
		ctrl := o.nodes[addr]
		select {
		case <-ctrl.done:
			markDone(addr, "after idle report")
		default:
			drainLocalSteps(addr)
			pendingIdle[addr] = idle
		}
	}

	enqueueOp := func(op *PendingOp) {
		o.schedulable = append(o.schedulable, op)
	}

	reapPendingDone := func(reason string) {
		for _, addr := range o.order {
			if doneSet[addr] {
				continue
			}
			if _, ok := pendingIdle[addr]; !ok {
				continue
			}
			ctrl := o.nodes[addr]
			select {
			case <-ctrl.done:
				markDone(addr, reason)
			default:
			}
		}
	}

	drainAllOutboxes := func() {
		for _, addr := range o.order {
			if doneSet[addr] {
				continue
			}
			ctrl := o.nodes[addr]
		drain:
			for {
				select {
				case op := <-ctrl.transport.Outbox():
					enqueueOp(op)
				default:
					break drain
				}
			}
		}
	}

	// Start bubbles one at a time, waiting for each to reach its first idle
	// point before starting the next. Sequential startup eliminates concurrent
	// access to shared resources (e.g. a seeded *rand.Rand in the application
	// under test) during the initialization phase, which is necessary for
	// fully deterministic replay. After startup all active bubbles are in
	// pendingIdle and the delivery loop below takes over.
	for _, addr := range o.order {
		o.startBubble(t, o.nodes[addr])
		ev := waitNodeEvent(addr, o.nodes[addr])
		switch {
		case ev.idle != nil:
			recordIdle(addr, *ev.idle)
		case ev.done:
			markDone(addr, "during init")
		}
	}

	for active > 0 {
		reapPendingDone("while pending idle")

		// Drain all currently visible sends before deciding whether we need to wait.
		drainAllOutboxes()

		// Wait until every active node is either pending idle or done.
		// Collect events from all needed nodes first (order-agnostic — each
		// bubble's Idle channel is buffered so the send doesn't block), then
		// process them in registration order. This enforces the synchronous-
		// round invariant: drainLocalSteps and drainOutbox happen in o.order,
		// making the unified trace and schedulable queue deterministic.
		type neededAddr struct {
			addr string
			ctrl *nodeCtrl
		}
		var needed []neededAddr
		for _, addr := range o.order {
			if doneSet[addr] {
				continue
			}
			if _, ok := pendingIdle[addr]; ok {
				continue
			}
			needed = append(needed, neededAddr{addr, o.nodes[addr]})
		}
		// Phase 1: collect (blocks per-node, but Idle is buffered so safe).
		collected := make(map[string]bubbleEvent, len(needed))
		for _, n := range needed {
			collected[n.addr] = waitNodeEvent(n.addr, n.ctrl)
		}
		// Phase 2: process in registration order.
		for _, addr := range o.order {
			ev, ok := collected[addr]
			if !ok {
				continue
			}
			switch {
			case ev.done:
				markDone(ev.addr, "after event wait")
			case ev.idle != nil:
				recordIdle(ev.addr, *ev.idle)
			}
		}

		if active == 0 {
			break
		}

		// ── Step 3: drain late-visible outboxes (non-blocking) ──
		//
		// Transports publish onto buffered outboxes, so draining here does not
		// resume or otherwise perturb bubble state. It snapshots any sends
		// that became visible after the idle barrier above. Drain each node's
		// outbox completely (loop, not single select) so the schedulable queue
		// contains every pending op at the decision point.
		drainAllOutboxes()

		// ── Steps 4–6: make a scheduling decision ──

		if len(o.schedulable) > 0 {
			// Index-based delivery selection via pick function.
			step := len(globalDecisions)
			chosenIdx := pick(o.schedulable, step)
			if chosenIdx < 0 || chosenIdx >= len(o.schedulable) {
				panic(replayDivergenceError{
					Step:      step,
					Index:     chosenIdx,
					QueueSize: len(o.schedulable),
				})
			}

			globalDecisions = append(globalDecisions, GlobalDecision{
				Index:     chosenIdx,
				QueueSize: len(o.schedulable),
			})
			unified = append(unified, Step{
				Node:         "",
				Index:        int32(chosenIdx),
				Alternatives: int32(len(o.schedulable)),
			})

			op := o.schedulable[chosenIdx]
			o.schedulable = append(o.schedulable[:chosenIdx], o.schedulable[chosenIdx+1:]...)

			// Execute delivers the message to the target node's mailbox.
			// The target's bridge goroutine is frozen in ExternalWait on
			// that mailbox, so it will wake when its bubble receives Resume.
			op.Execute()

			o.trace = append(o.trace, GlobalStep{
				Type:   StepDeliver,
				Dir:    op.Dir,
				From:   op.From,
				To:     op.To,
				OpType: op.Type,
				Time:   o.globalTime,
			})
			if verbose {
				t.Logf("orchestrator: deliver %s %s→%s (%s)", dirName(op.Dir), op.From, op.To, op.Type)
			}

			// Resume the target bubble so its bridge goroutine can process
			// the newly delivered message.
			if targetCtrl, ok := o.nodes[op.To]; ok {
				if _, pending := pendingIdle[op.To]; pending {
					targetCtrl.bubble.Resume <- distributed.Resume{}
					delete(pendingIdle, op.To)
				}
			}

		} else {
			// No pending sends. Check for timers.
			var earliest int64
			for _, idleState := range pendingIdle {
				if idleState.State.NextTimer > 0 {
					if earliest == 0 || idleState.State.NextTimer < earliest {
						earliest = idleState.State.NextTimer
					}
				}
			}

			if earliest > 0 {
				// Advance time: resume idle bubbles one at a time in registration
				// order. Sequential resumption eliminates concurrent rand access
				// across bubbles (e.g. raft's randomTimeout calls), which is the
				// key requirement for bit-for-bit replay determinism.
				o.globalTime = earliest
				o.trace = append(o.trace, GlobalStep{Type: StepTimeAdvance, Time: earliest})
				if verbose {
					t.Logf("orchestrator: advance time to %d ns", earliest)
				}

				for _, addr := range o.order {
					if _, idle := pendingIdle[addr]; !idle {
						continue
					}
					delete(pendingIdle, addr)
					ctrl := o.nodes[addr]
					ctrl.bubble.Resume <- distributed.Resume{AdvanceTimeTo: earliest}
					ev := waitNodeEvent(addr, ctrl)
					switch {
					case ev.idle != nil:
						recordIdle(addr, *ev.idle)
					case ev.done:
						markDone(addr, "during time advance")
					}
				}

			} else {
				// No sends and no timers — drain and terminate.
				//
				// DelegateIdle tells the runtime to handle idle locally. The
				// bubble detects deadlock (all goroutines blocked, no timers,
				// no external) and terminates. delegateIdle persists until a
				// real scheduling event clears it, so the hook never re-fires
				// into a dead channel.
				//
				// If a bubble has bridges in ExternalWait (ExternalWait > 0),
				// the runtime won't break out of the event loop because
				// externalWait > 0 suppresses termination. Shut down the
				// transport first so bridges exit and externalWait drops to 0.
				if verbose {
					t.Logf("orchestrator: no pending sends or timers — resuming all to drain")
				}
				for _, addr := range o.order {
					idleState, idle := pendingIdle[addr]
					if !idle {
						continue
					}
					delete(pendingIdle, addr)
					ctrl := o.nodes[addr]

					if idleState.State.ExternalWait > 0 {
						if s, ok := ctrl.transport.(shutdowner); ok {
							s.Shutdown()
						}
					}

					ctrl.bubble.Resume <- distributed.Resume{DelegateIdle: true}
					ev := waitNodeEvent(addr, ctrl)
					switch {
					case ev.idle != nil:
						recordIdle(addr, *ev.idle)
					case ev.done:
						markDone(addr, "during drain")
					}
				}
			}
		}
	}

	// Collect per-node results.
	allPassed := true
	localTraces := make(map[string][]synctest.Decision, len(o.nodes))
	for _, addr := range o.order {
		ctrl := o.nodes[addr]
		localTraces[addr] = ctrl.localTrace
		if !ctrl.passed {
			allPassed = false
		}
	}

	return RecordedRun{
		GlobalTrace:     o.trace,
		GlobalDecisions: globalDecisions,
		LocalTraces:     localTraces,
		Trace:           unified,
	}, allPassed
}

// drainOutbox non-blockingly drains all pending ops from a node's outbox into
// o.schedulable. Called when a node's bubble finishes so that messages it sent
// just before exiting (e.g. RA-lock reply-after-release) are not lost.
func (o *Orchestrator) drainOutbox(addr string) {
	ctrl := o.nodes[addr]
	for {
		select {
		case op := <-ctrl.transport.Outbox():
			o.schedulable = append(o.schedulable, op)
		default:
			return
		}
	}
}

func dirName(d OpDir) string {
	return "send"
}
