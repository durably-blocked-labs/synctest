// Package orchestrator coordinates multiple synctest bubbles acting as
// distributed nodes that communicate via controlled message delivery.
//
// Each node runs in its own synctest bubble. Local scheduling decisions
// are handled synchronously inside each bubble via a callback. Global
// delivery decisions are made by the orchestrator. Both paths can be
// controlled by a pluggable Algorithm (CHESS, PCT, DPOR, etc.) via
// the ExploreWith API.
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

// ─── Existing types (backward compatibility) ───

// OpDir classifies the direction of a pending operation.
type OpDir int

const (
	OpSend OpDir = iota
)

// PendingOp is the unit of work submitted by a transport to the orchestrator.
type PendingOp struct {
	Dir     OpDir
	From    string
	To      string
	Type    string
	Execute func()
}

// NodeTransport is the interface transport implementations must satisfy.
type NodeTransport interface {
	Addr() string
	Outbox() <-chan *PendingOp
}

type shutdowner interface{ Shutdown() }

// GlobalStepType classifies what happened in one orchestrator loop iteration.
type GlobalStepType int

const (
	StepDeliver     GlobalStepType = iota
	StepTimeAdvance
	StepDone
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

// GlobalDecision records one global scheduling choice.
type GlobalDecision struct {
	Index     int
	QueueSize int
}

// Step is one entry in the unified trace (old format for RecordedRun).
type Step struct {
	Node         string
	Index        int32
	Alternatives int32
}

// RecordedRun captures a complete orchestrator run for deterministic replay.
type RecordedRun struct {
	GlobalTrace     []GlobalStep
	GlobalDecisions []GlobalDecision
	LocalTraces     map[string][]synctest.Decision
	Trace           []Step
}

type replayDivergenceError struct {
	Step      int
	Index     int
	QueueSize int
}

func (e replayDivergenceError) Error() string {
	return fmt.Sprintf("orchestrator replay divergence at step %d: chose index %d with queue size %d", e.Step, e.Index, e.QueueSize)
}

// ─── Configuration ───

// RunObserver is called once after each run inside Explore/ExploreWith.
type RunObserver func(runNum int, nonFIFO int, elapsed time.Duration, rr RunResult, passed bool)

// ExploreOption configures Explore behavior.
type ExploreOption func(*exploreConfig)

type exploreConfig struct {
	bound     int
	maxRuns   int
	observers []RunObserver
}

// GlobalBound sets the maximum number of non-FIFO decisions per explored trace.
func GlobalBound(k int) ExploreOption { return func(c *exploreConfig) { c.bound = k } }

// GlobalMaxRuns sets a hard cap on the total number of traces explored.
func GlobalMaxRuns(n int) ExploreOption { return func(c *exploreConfig) { c.maxRuns = n } }

// WithObserver registers a callback invoked once after each run. Multiple observers stack.
func WithObserver(fn RunObserver) ExploreOption {
	return func(c *exploreConfig) { c.observers = append(c.observers, fn) }
}

// ─── Core types ───

type nodeCtrl struct {
	transport  NodeTransport
	testFunc   func(t *testing.T)
	bubbleOpts []distributed.Option
	bubble     *distributed.Bubble
	done       chan struct{}
	localTrace []synctest.Decision
	passed     bool
}

// Orchestrator manages nodes and controls cross-node message delivery.
type Orchestrator struct {
	nodes      map[string]*nodeCtrl
	order      []string
	globalTime int64

	schedulable []*PendingOp
	trace       []GlobalStep
}

// NodeOption is a per-node configuration option for AddNode.
type NodeOption = distributed.Option

// WithScheduler sets a custom local scheduling function for a node's bubble.
func WithScheduler(fn func(distributed.BubbleState) int32) NodeOption {
	return distributed.WithScheduler(fn)
}

// BubbleState is the state of a bubble at a scheduling decision point.
type BubbleState = distributed.BubbleState

// ─── Construction ───

// New creates an empty orchestrator.
func New() *Orchestrator {
	return &Orchestrator{nodes: make(map[string]*nodeCtrl)}
}

// AddNode registers a node transport and its bubble test function.
func (o *Orchestrator) AddNode(transport NodeTransport, f func(t *testing.T), opts ...distributed.Option) {
	addr := transport.Addr()
	o.nodes[addr] = &nodeCtrl{transport: transport, testFunc: f, bubbleOpts: opts}
	o.order = append(o.order, addr)
}

// initBubbles (re-)creates all per-node bubbles.
func (o *Orchestrator) initBubbles() {
	for _, addr := range o.order {
		ctrl := o.nodes[addr]
		ctrl.bubble = distributed.NewBubble(addr, ctrl.bubbleOpts...)
		ctrl.done = make(chan struct{})
		ctrl.localTrace = nil
		ctrl.passed = false
	}
}

// startBubble launches a node's test function inside a synctest bubble.
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

// ─── Decision helpers ───

func buildLocalAlts(state distributed.BubbleState) []Alt {
	alts := make([]Alt, state.RunnableN)
	for i := int32(0); i < state.RunnableN; i++ {
		alts[i] = Alt{
			ID:   fmt.Sprintf("B%d", state.RunnableBgid[i]),
			BGID: state.RunnableBgid[i],
		}
	}
	return alts
}

func buildGlobalAlts(schedulable []*PendingOp) []Alt {
	alts := make([]Alt, len(schedulable))
	for i, op := range schedulable {
		alts[i] = Alt{
			ID:      fmt.Sprintf("msg:%s→%s(%s)", op.From, op.To, op.Type),
			From:    op.From,
			To:      op.To,
			MsgType: op.Type,
		}
	}
	return alts
}

// ─── Event handling ───

type bubbleEvent struct {
	addr string
	idle *distributed.IdleState
	done bool
}

// waitNodeEvent blocks until a node reports idle or completes.
func waitNodeEvent(addr string, ctrl *nodeCtrl) bubbleEvent {
	select {
	case idle := <-ctrl.bubble.Idle:
		idleCopy := idle
		return bubbleEvent{addr: addr, idle: &idleCopy}
	case <-ctrl.done:
		return bubbleEvent{addr: addr, done: true}
	}
}

// ─── Core execution ───

// run is the unified implementation for all execution modes.
// decide is called for global decisions directly. For local decisions,
// the orchestrator sets scheduleFn on each bubble to call decide synchronously.
func (o *Orchestrator) run(t *testing.T, decide func(DecisionPoint) int) RunResult {
	if runtime.GOMAXPROCS(0) < 2 {
		t.Fatal("orchestrator requires GOMAXPROCS >= 2")
	}

	o.trace = nil
	o.schedulable = nil
	o.globalTime = 0
	verbose := testing.Verbose()

	n := len(o.nodes)
	var unified Trace
	traceStep := 0

	// Set scheduleFn on each bubble to call decide for local decisions.
	// The callback runs synchronously inside the hook — no channels.
	// traceStep is shared: safe because only one bubble is active at a time,
	// and the orchestrator is blocked in waitNodeEvent while a bubble runs.
	for _, addr := range o.order {
		node := addr
		ctrl := o.nodes[addr]
		ctrl.bubble.SetScheduleFn(func(state distributed.BubbleState) int32 {
			dp := DecisionPoint{
				Kind: Local,
				Step: traceStep,
				Node: node,
				Alts: buildLocalAlts(state),
			}
			idx := decide(dp)
			traceStep++
			return int32(idx)
		})
	}

	pendingIdle := make(map[string]distributed.IdleState)
	doneSet := make(map[string]bool)
	active := n

	// drainLocalSteps appends a node's local decisions to the unified trace.
	drainLocalSteps := func(addr string) {
		ctrl := o.nodes[addr]
		if ctrl.bubble == nil {
			return
		}
		for _, d := range ctrl.bubble.DrainLocal() {
			unified = append(unified, NewStep{
				Kind:         Local,
				Node:         addr,
				Index:        d.Index,
				Alternatives: d.Alternatives,
				ChosenID:     fmt.Sprintf("B%d", d.ChosenBGID),
				ChosenBGID:   d.ChosenBGID,
				RunqBGIDs:    d.RunqBGIDs,
			})
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

	// ── Startup: sequential ──
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

	// ── Main delivery loop ──
	for active > 0 {
		reapPendingDone("while pending idle")
		drainAllOutboxes()

		// Wait until every active node is either pending idle or done.
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
		collected := make(map[string]bubbleEvent, len(needed))
		for _, n := range needed {
			collected[n.addr] = waitNodeEvent(n.addr, n.ctrl)
		}
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

		drainAllOutboxes()

		if len(o.schedulable) > 0 {
			// ── Global delivery decision ──
			dp := DecisionPoint{
				Kind: Global,
				Step: traceStep,
				Alts: buildGlobalAlts(o.schedulable),
			}
			chosenIdx := decide(dp)
			if chosenIdx < 0 || chosenIdx >= len(o.schedulable) {
				panic(replayDivergenceError{
					Step: traceStep, Index: chosenIdx, QueueSize: len(o.schedulable),
				})
			}

			op := o.schedulable[chosenIdx]
			o.schedulable = append(o.schedulable[:chosenIdx], o.schedulable[chosenIdx+1:]...)

			unified = append(unified, NewStep{
				Kind:         Global,
				Index:        int32(chosenIdx),
				Alternatives: int32(len(dp.Alts)),
				ChosenID:     dp.Alts[chosenIdx].ID,
				Resource:     op.To,
				From:         op.From,
				To:           op.To,
				MsgType:      op.Type,
			})
			traceStep++

			op.Execute()

			o.trace = append(o.trace, GlobalStep{
				Type: StepDeliver, Dir: op.Dir,
				From: op.From, To: op.To, OpType: op.Type, Time: o.globalTime,
			})
			if verbose {
				t.Logf("orchestrator: deliver %s %s→%s (%s)", dirName(op.Dir), op.From, op.To, op.Type)
			}

			if targetCtrl, ok := o.nodes[op.To]; ok {
				if _, pending := pendingIdle[op.To]; pending {
					// Resume the target bubble so it processes the delivered message.
					// Race: the bridge goroutine (in ExternalWait) may not have
					// reattached yet, causing a spurious idle. We retry until
					// HasNewLocal() confirms the message was processed, with a
					// wall-clock timeout that treats stuck bridges as divergences.
					delete(pendingIdle, op.To)
					deadline := time.Now().Add(1 * time.Millisecond)
					for {
						targetCtrl.bubble.Resume <- distributed.Resume{}
						ev := waitNodeEvent(op.To, targetCtrl)
						if ev.done {
							markDone(op.To, "after delivery")
							break
						}
						if ev.idle != nil {
							if targetCtrl.bubble.HasNewLocal() {
								recordIdle(op.To, *ev.idle)
								break
							}
							// Spurious idle: bridge hasn't reattached yet.
							if time.Now().After(deadline) {
								// Infrastructure failure — treat as divergence.
								panic(replayDivergenceError{
									Step: traceStep, Index: chosenIdx,
									QueueSize: len(dp.Alts),
								})
							}
						}
					}
				}
			}

		} else {
			var earliest int64
			for _, idleState := range pendingIdle {
				if idleState.State.NextTimer > 0 {
					if earliest == 0 || idleState.State.NextTimer < earliest {
						earliest = idleState.State.NextTimer
					}
				}
			}

			if earliest > 0 {
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

	allPassed := true
	userFailed := false
	for _, addr := range o.order {
		ctrl := o.nodes[addr]
		if !ctrl.passed {
			allPassed = false
			// Distinguish assertion failure from deadlock:
			// synctest.Explore returns (nil, false) on deadlock/panic,
			// (trace, false) when t.Errorf was called.
			if ctrl.localTrace != nil {
				userFailed = true
			}
		}
	}

	return RunResult{
		Trace:          unified,
		Passed:         allPassed,
		UserFailed:     userFailed,
		DivergenceStep: -1,
		GlobalTrace:    o.trace,
	}
}

// ─── Exploration ───

// ExploreWith runs the system repeatedly using the given algorithm.
func (o *Orchestrator) ExploreWith(
	t *testing.T,
	setup func(*Orchestrator),
	algo Algorithm,
	opts ...ExploreOption,
) ExplorationResult {
	t.Helper()
	start := time.Now()
	result := ExplorationResult{FirstBug: -1}
	cfg := &exploreConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	for algo.BeforeRun() {
		if cfg.maxRuns > 0 && result.Runs >= cfg.maxRuns {
			break
		}
		o.nodes = make(map[string]*nodeCtrl)
		o.order = nil
		setup(o)
		o.initBubbles()

		runtime.GC()
		runStart := time.Now()
		rr := o.runForExplore(t, algo.Decide)

		rr.Elapsed = time.Since(runStart)
		result.Runs++

		// Divergences are NOT bugs — the prefix didn't match, so the run
		// should be discarded. Only count non-divergent failures as bugs.
		isBug := !rr.Passed && !rr.Diverged && rr.UserFailed
		if isBug {
			result.Failed++
			if result.FirstBug == -1 {
				result.FirstBug = result.Runs
			}
		}

		if len(cfg.observers) > 0 && !rr.Diverged {
			nonFIFO := 0
			for _, s := range rr.Trace {
				if s.Index != 0 {
					nonFIFO++
				}
			}
			for _, obs := range cfg.observers {
				obs(result.Runs, nonFIFO, rr.Elapsed, rr, rr.Passed)
			}
		}

		algo.AfterRun(rr)

		if isBug {
			break
		}
	}

	result.Elapsed = time.Since(start)
	return result
}

// runForExplore wraps run with panic recovery for divergence.
func (o *Orchestrator) runForExplore(t *testing.T, decide func(DecisionPoint) int) RunResult {
	var rr RunResult
	defer func() {
		if r := recover(); r != nil {
			if d, ok := r.(replayDivergenceError); ok {
				rr.Diverged = true
				rr.DivergenceStep = d.Step
				o.cleanupBubbles()
				return
			}
			panic(r)
		}
	}()
	rr = o.run(t, decide)
	return rr
}

// ─── Public API ───

// Run executes the system once with FIFO scheduling.
func (o *Orchestrator) Run(t *testing.T) (RecordedRun, bool) {
	t.Helper()
	o.initBubbles()
	rr := o.run(t, func(dp DecisionPoint) int { return 0 })
	return o.buildRecordedRun(rr), rr.Passed
}

// RunWith executes the system once with a custom decision function.
func (o *Orchestrator) RunWith(t *testing.T, decide func(DecisionPoint) int) RunResult {
	t.Helper()
	o.initBubbles()
	return o.run(t, decide)
}

// Replay re-runs the system following a previously recorded trace.
func (o *Orchestrator) Replay(t *testing.T, rec RecordedRun) (RecordedRun, bool) {
	t.Helper()
	o.initBubbles()
	rr := o.run(t, replayGlobalOnly(rec.GlobalDecisions))
	return o.buildRecordedRun(rr), rr.Passed
}

// Explore enumerates global message delivery orderings using CHESS DFS.
func (o *Orchestrator) Explore(t *testing.T, setup func(*Orchestrator), opts ...ExploreOption) bool {
	t.Helper()
	cfg := &exploreConfig{bound: 2}
	for _, opt := range opts {
		opt(cfg)
	}
	algo := &CHESS{Bound: cfg.bound, GlobalOnly: true}
	result := o.ExploreWith(t, setup, algo, opts...)
	return result.Failed == 0
}

// ExploreAll enumerates global + local interleavings using CHESS DFS.
func (o *Orchestrator) ExploreAll(t *testing.T, setup func(*Orchestrator), opts ...ExploreOption) bool {
	t.Helper()
	cfg := &exploreConfig{bound: 2}
	for _, opt := range opts {
		opt(cfg)
	}
	algo := &CHESS{Bound: cfg.bound}
	result := o.ExploreWith(t, setup, algo, opts...)
	return result.Failed == 0
}

// ─── Conversion helpers ───

func (o *Orchestrator) buildRecordedRun(rr RunResult) RecordedRun {
	var globalDecisions []GlobalDecision
	for _, s := range rr.Trace {
		if s.Kind == Global {
			globalDecisions = append(globalDecisions, GlobalDecision{
				Index: int(s.Index), QueueSize: int(s.Alternatives),
			})
		}
	}
	oldTrace := make([]Step, len(rr.Trace))
	for i, s := range rr.Trace {
		oldTrace[i] = Step{Node: s.Node, Index: s.Index, Alternatives: s.Alternatives}
	}
	localTraces := make(map[string][]synctest.Decision, len(o.nodes))
	for _, addr := range o.order {
		localTraces[addr] = o.nodes[addr].localTrace
	}
	return RecordedRun{
		GlobalTrace:     rr.GlobalTrace,
		GlobalDecisions: globalDecisions,
		LocalTraces:     localTraces,
		Trace:           oldTrace,
	}
}

func replayGlobalOnly(decisions []GlobalDecision) func(DecisionPoint) int {
	globalStep := 0
	return func(dp DecisionPoint) int {
		if dp.Kind == Local {
			return 0
		}
		if globalStep < len(decisions) {
			d := decisions[globalStep]
			if d.QueueSize != dp.N() {
				panic(replayDivergenceError{Step: dp.Step, Index: d.Index, QueueSize: dp.N()})
			}
			globalStep++
			return d.Index
		}
		panic(replayDivergenceError{Step: dp.Step, Index: -1, QueueSize: 0})
	}
}

// ─── Cleanup ───

func (o *Orchestrator) cleanupBubbles() {
	for _, addr := range o.order {
		ctrl := o.nodes[addr]
		if s, ok := ctrl.transport.(shutdowner); ok {
			s.Shutdown()
		}
	}
	for _, addr := range o.order {
		ctrl := o.nodes[addr]
		if ctrl.bubble == nil {
			continue
		}
		select {
		case <-ctrl.bubble.Idle:
		default:
		}
		select {
		case ctrl.bubble.Resume <- distributed.Resume{DelegateIdle: true}:
		default:
		}
	}
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

// ─── Utilities ───

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

// verbose logging helper for explore.
func deliveryLog(trace []GlobalStep) string {
	var sb strings.Builder
	for _, step := range trace {
		if step.Type == StepDeliver {
			if sb.Len() > 0 {
				sb.WriteByte(' ')
			}
			fmt.Fprintf(&sb, "%s→%s(%s)", step.From, step.To, step.OpType)
		}
	}
	return sb.String()
}
