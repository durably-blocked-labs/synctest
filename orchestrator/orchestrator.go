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
// any time in the controlled path. External application work is not isolated.
//
// Execution requires GOMAXPROCS >= node count + 1 (and at least 2): each
// frozen node retains its owner P, and the coordinator needs a spare host P.
// Insufficient capacity is rejected before starting targets, never adjusted
// silently. Explicit runtime.GOMAXPROCS calls override the environment.
package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"io"
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
	Dir        OpDir
	From       string
	To         string
	Type       string
	Execute    func() // legacy compatibility path, not acknowledged
	Deliver    func(context.Context) (DeliveryReceipt, error)
	Occurrence string
}

// NodeTransport is the interface transport implementations must satisfy.
type NodeTransport interface {
	Addr() string
	Outbox() <-chan *PendingOp
}

type shutdowner interface{ Shutdown() }
type teardownCloser interface{ CloseForTeardown() error }

// GlobalStepType classifies what happened in one orchestrator loop iteration.
type GlobalStepType int

const (
	StepDeliver GlobalStepType = iota
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
	Outcome         Outcome
	Cause           any
	WorkerUnsafe    bool
}

type replayDivergenceError struct {
	Step      int
	Index     int
	QueueSize int
	Detail    string
}

func (e replayDivergenceError) Error() string {
	return fmt.Sprintf("orchestrator replay divergence at step %d: chose index %d with queue size %d", e.Step, e.Index, e.QueueSize) + ": " + e.Detail
}

// ─── Configuration ───

// RunObserver is called once after each run inside Explore/ExploreWith.
type RunObserver func(runNum int, nonFIFO int, elapsed time.Duration, rr RunResult, passed bool)

// ExploreOption configures Explore behavior.
type ExploreOption func(*exploreConfig)

type exploreConfig struct {
	bound                int
	maxRuns              int
	observers            []RunObserver
	sink                 TrialSink
	limits               TrialLimits
	ctx                  context.Context
	strict               bool
	selectChoices        bool
	continueAfterFailure bool
}

// GlobalBound sets the maximum number of non-FIFO decisions per explored trace.
func GlobalBound(k int) ExploreOption { return func(c *exploreConfig) { c.bound = k } }

// GlobalMaxRuns sets a hard cap on the total number of traces explored.
func GlobalMaxRuns(n int) ExploreOption { return func(c *exploreConfig) { c.maxRuns = n } }

// WithObserver registers a callback invoked once after each run. Multiple observers stack.
func WithObserver(fn RunObserver) ExploreOption {
	return func(c *exploreConfig) { c.observers = append(c.observers, fn) }
}

func ContinueAfterFailure() ExploreOption {
	return func(c *exploreConfig) { c.continueAfterFailure = true }
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
	started    bool
	cause      any
	hookCause  any
	unsafe     bool
	stopped    bool
	selectFn   func(synctest.SelectState) int32
}

// Orchestrator manages nodes and controls cross-node message delivery.
type Orchestrator struct {
	nodes      map[string]*nodeCtrl
	order      []string
	globalTime int64

	schedulable   []*PendingOp
	trace         []GlobalStep
	ctx           context.Context
	limits        TrialLimits
	strict        bool
	unsafe        bool
	partial       Trace
	traceBytes    int
	selectChoices bool
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
	return &Orchestrator{nodes: make(map[string]*nodeCtrl), limits: defaultTrialLimits()}
}

// AddNode registers a node transport and its bubble test function.
func (o *Orchestrator) AddNode(transport NodeTransport, f func(t *testing.T), opts ...distributed.Option) {
	addr := transport.Addr()
	o.nodes[addr] = &nodeCtrl{transport: transport, testFunc: f, bubbleOpts: opts}
	o.order = append(o.order, addr)
}

// initBubbles (re-)creates all per-node bubbles.
func (o *Orchestrator) initBubbles() {
	if o.unsafe || !WorkerReusable() {
		panic("orchestrator: unsafe worker cannot reset; terminate process")
	}
	for _, addr := range o.order {
		ctrl := o.nodes[addr]
		ctrl.bubble = distributed.NewBubble(addr, ctrl.bubbleOpts...)
		ctrl.done = make(chan struct{})
		ctrl.localTrace = nil
		ctrl.passed = false
		ctrl.started = false
		ctrl.cause = nil
		ctrl.hookCause = nil
		ctrl.unsafe = false
		ctrl.stopped = false
	}
}

// startBubble launches a node's test function inside a synctest bubble.
func (o *Orchestrator) startBubble(t *testing.T, ctrl *nodeCtrl) {
	ctrl.started = true
	b := ctrl.bubble
	userFn := ctrl.testFunc
	go func() {
		defer close(ctrl.done)
		var workloadCause any
		result := synctest.ExploreResult(t, func(t *testing.T) {
			defer func() {
				if cause := recover(); cause != nil {
					workloadCause = cause
				}
			}()
			synctest.SetDecisionHook(b.Hook())
			if ctrl.selectFn != nil {
				synctest.SetSelectHook(ctrl.selectFn)
			}
			userFn(t)
		}, nil)
		ctrl.localTrace = result.Trace
		ctrl.passed = result.OK
		ctrl.cause = result.Cause
		ctrl.unsafe = result.WorkerUnsafe
		if workloadCause != nil {
			ctrl.cause = workloadCause
			ctrl.passed = false
			ctrl.unsafe = true
		}
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
			ID:         fmt.Sprintf("msg:%s→%s(%s)", op.From, op.To, op.Type),
			From:       op.From,
			To:         op.To,
			MsgType:    op.Type,
			Occurrence: op.Occurrence,
		}
	}
	return alts
}

func altIDsAndResources(dp DecisionPoint) ([]string, []string) {
	ids := make([]string, len(dp.Alts))
	resources := make([]string, len(dp.Alts))
	for i, alt := range dp.Alts {
		ids[i] = alt.ID
		if dp.Kind != Global {
			resources[i] = dp.Node
		} else {
			resources[i] = endpointResource(alt.From, alt.To)
		}
	}
	return ids, resources
}

func endpointResource(from, to string) string {
	return from + "|" + to
}

// ─── Event handling ───

type bubbleEvent struct {
	addr string
	idle *distributed.IdleState
	done bool
}

// waitNodeEvent blocks until a node reports idle or completes.
func waitNodeEvent(ctx context.Context, addr string, ctrl *nodeCtrl) bubbleEvent {
	select {
	case <-ctx.Done():
		panic(context.Cause(ctx))
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
	// Every frozen node root retains its owner P while parked in the idle
	// hook. A separate host P must remain available to run the coordinator.
	required := len(o.order) + 1
	if required < 2 {
		required = 2
	}
	if available := runtime.GOMAXPROCS(0); available < required {
		panic(ConfigurationError{Available: available, Required: required})
	}

	if o.strict {
		for _, addr := range o.order {
			if err := validateTransport(addr, o.nodes[addr].transport); err != nil {
				panic(err)
			}
		}
	}

	o.trace = nil
	o.schedulable = nil
	o.globalTime = 0
	o.partial = nil
	o.traceBytes = 0
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
			defer func() {
				if cause := recover(); cause != nil {
					ctrl.hookCause = cause
					panic(cause)
				}
			}()
			dp := DecisionPoint{Kind: Local, Step: traceStep, Node: node, Alts: buildLocalAlts(state)}
			if traceStep >= o.limits.Decisions {
				panic(ResourceLimitError{Resource: "decisions", Limit: o.limits.Decisions})
			}
			idx := decide(dp)
			CheckDecision(dp, idx)
			unified = append(unified, o.recordDecision(dp, idx))
			o.partial = unified
			traceStep++
			return int32(idx)
		})
		ctrl.selectFn = nil
		if o.selectChoices {
			ctrl.selectFn = func(state synctest.SelectState) int32 {
				defer func() {
					if cause := recover(); cause != nil {
						ctrl.hookCause = cause
						panic(cause)
					}
				}()
				dp := SelectDecisionPoint(state, traceStep, node)
				if traceStep >= o.limits.Decisions {
					panic(ResourceLimitError{Resource: "decisions", Limit: o.limits.Decisions})
				}
				idx := decide(dp)
				CheckDecision(dp, idx)
				unified = append(unified, o.recordDecision(dp, idx))
				o.partial = unified
				traceStep++
				return dp.Alts[idx].CaseIndex
			}
		}

	}

	pendingIdle := make(map[string]distributed.IdleState)
	doneSet := make(map[string]bool)
	active := n

	// drainLocalSteps appends a node's local decisions to the unified trace.
	drainLocalSteps := func(addr string) { o.nodes[addr].bubble.DrainLocal() }

	markDone := func(addr, reason string) {
		if doneSet[addr] {
			return
		}
		drainLocalSteps(addr)
		if c := o.nodes[addr]; c.unsafe {
			panic(nodeFailure{c})
		}
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
			tr := o.nodes[addr].transport
			if _, ok := tr.(AcknowledgedTransport); ok || o.strict {
				if err := validateTransport(addr, tr); err != nil {
					panic(err)
				}
			}
			o.drainOutbox(addr)
		}
	}

	// ── Startup: sequential ──
	for _, addr := range o.order {
		if err := context.Cause(o.ctx); err != nil {
			panic(err)
		}
		o.startBubble(t, o.nodes[addr])
		ev := waitNodeEvent(o.ctx, addr, o.nodes[addr])
		switch {
		case ev.idle != nil:
			recordIdle(addr, *ev.idle)
		case ev.done:
			markDone(addr, "during init")
		}
	}

	// ── Main delivery loop ──
	for active > 0 {
		if err := context.Cause(o.ctx); err != nil {
			panic(err)
		}
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
			collected[n.addr] = waitNodeEvent(o.ctx, n.addr, n.ctrl)
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

		if err := context.Cause(o.ctx); err != nil {
			panic(err)
		}
		if active == 0 {
			break
		}

		drainAllOutboxes()

		if err := context.Cause(o.ctx); err != nil {
			panic(err)
		}
		if len(o.schedulable) > 0 {
			// ── Global delivery decision ──
			dp := DecisionPoint{
				Kind: Global,
				Step: traceStep,
				Alts: buildGlobalAlts(o.schedulable),
			}
			if traceStep >= o.limits.Decisions {
				panic(ResourceLimitError{Resource: "decisions", Limit: o.limits.Decisions})
			}
			chosenIdx := globalDecision(decide, dp)
			if chosenIdx < 0 || chosenIdx >= len(o.schedulable) {
				panic(replayDivergenceError{
					Step: traceStep, Index: chosenIdx, QueueSize: len(o.schedulable),
				})
			}

			op := o.schedulable[chosenIdx]
			o.schedulable = append(o.schedulable[:chosenIdx], o.schedulable[chosenIdx+1:]...)
			unified = append(unified, o.recordDecision(dp, chosenIdx))
			o.partial = unified
			traceStep++
			var receipt DeliveryReceipt
			if op.Deliver != nil {
				var err error
				receipt, err = op.Deliver(o.ctx)
				if cause := context.Cause(o.ctx); cause != nil {
					panic(cause)
				}
				if err != nil {
					panic(err)
				}
				if err = context.Cause(o.ctx); err != nil {
					panic(err)
				}
				if receipt.WaitReady != nil {
					receipt.WaitReady()
				}
			} else {
				if o.strict {
					panic(fmt.Errorf("operation %q lacks delivery acknowledgement", op.Occurrence))
				}
				op.Execute()
			}

			o.trace = append(o.trace, GlobalStep{
				Type: StepDeliver, Dir: op.Dir,
				From: op.From, To: op.To, OpType: op.Type, Time: o.globalTime,
			})
			if verbose {
				t.Logf("orchestrator: deliver %s %s→%s (%s)", dirName(op.Dir), op.From, op.To, op.Type)
			}

			if targetCtrl, ok := o.nodes[op.To]; ok {
				if _, pending := pendingIdle[op.To]; pending {
					delete(pendingIdle, op.To)
					o.resume(targetCtrl, distributed.Resume{})
					if err := waitConsumption(o.ctx, targetCtrl, receipt); err != nil {
						panic(err)
					}
					ev := waitNodeEvent(o.ctx, op.To, targetCtrl)
					if ev.done {
						markDone(op.To, "after delivery")
					} else if ev.idle != nil {
						recordIdle(op.To, *ev.idle)
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
					if err := context.Cause(o.ctx); err != nil {
						panic(err)
					}
					if _, idle := pendingIdle[addr]; !idle {
						continue
					}
					delete(pendingIdle, addr)
					ctrl := o.nodes[addr]
					o.resume(ctrl, distributed.Resume{AdvanceTimeTo: earliest})
					ev := waitNodeEvent(o.ctx, addr, ctrl)
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
					if err := context.Cause(o.ctx); err != nil {
						panic(err)
					}
					idleState, idle := pendingIdle[addr]
					if !idle {
						continue
					}
					delete(pendingIdle, addr)
					ctrl := o.nodes[addr]

					if idleState.State.ExternalWait > 0 {
						if err := o.stopTransportForTeardown(ctrl); err != nil {
							panic(err)
						}
					}

					o.resume(ctrl, distributed.Resume{DelegateIdle: true})
					ev := waitNodeEvent(o.ctx, addr, ctrl)
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

	if err := context.Cause(o.ctx); err != nil {
		panic(err)
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
			if ctrl.cause == nil && !ctrl.unsafe {
				userFailed = true
			}
		}
	}

	rr := RunResult{
		Trace:          unified,
		Passed:         allPassed,
		UserFailed:     userFailed,
		DivergenceStep: -1,
		GlobalTrace:    o.trace,
	}
	rr.SelectControlled = o.selectChoices
	rr.Outcome = OutcomePassed
	if !rr.Passed {
		if userFailed {
			rr.Outcome = OutcomeAssertion
		} else {
			rr.Outcome = OutcomeInfrastructure
		}
	}
	for _, addr := range o.order {
		c := o.nodes[addr]
		if c.cause != nil {
			classifyCause(&rr, c.cause)
		}
		rr.WorkerUnsafe = rr.WorkerUnsafe || c.unsafe
	}
	return rr
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

	if cfg.limits.Decisions != 0 || cfg.limits.PendingOperations != 0 {
		if cfg.limits.Decisions <= 0 || cfg.limits.PendingOperations <= 0 {
			result.Incomplete = 1
			result.Cause = fmt.Errorf("invalid trial limits")
			return result
		}
		o.limits = cfg.limits
		if o.limits.TraceBytes <= 0 {
			o.limits.TraceBytes = 8 << 20
		}
	}
	o.ctx = cfg.ctx
	o.strict = cfg.strict
	o.selectChoices = cfg.selectChoices
	for cfg.maxRuns <= 0 || result.Runs < cfg.maxRuns {
		if !algo.BeforeRun() {
			if bounded, ok := algo.(interface{ SearchError() error }); ok {
				if err := bounded.SearchError(); err != nil {
					result.Incomplete++
					result.Cause = err
				}
			}
			break
		}
		if o.unsafe || !WorkerReusable() {
			result.Incomplete++
			result.Cause = fmt.Errorf("unsafe worker cannot start another trial")
			break
		}
		if cfg.ctx != nil && cfg.ctx.Err() != nil {
			result.Incomplete++
			result.Cause = cfg.ctx.Err()
			break
		}
		if cfg.sink != nil {
			if err := cfg.sink.StartTrial(result.Runs + 1); err != nil {
				result.Incomplete++
				result.Cause = err
				break
			}
		}
		runStart := time.Now()
		var rr RunResult
		if cause := o.prepare(setup); cause != nil {
			classifyCause(&rr, cause)
			if err := o.cleanupBubbles(); err != nil {
				rr.TeardownError = err
				rr.WorkerUnsafe = true
				o.unsafe = true
				RetireWorker()
			}
		} else {
			runtime.GC()
			rr = o.execute(t, algo.Decide)
		}

		if v, ok := algo.(prefixValidator); ok && !rr.Diverged && rr.Cause == nil {
			if err := v.ValidatePrefix(); err != nil {
				classifyCause(&rr, err)
			}
		}

		rr.Elapsed = time.Since(runStart)
		if cfg.sink != nil {
			if err := cfg.sink.EndTrial(result.Runs+1, rr); err != nil {
				rr.PersistenceError = err
				if rr.Cause == nil {
					classifyCause(&rr, err)
				} else {
					rr.Passed = false
				}
			}
		}

		result.Runs++

		if rr.Diverged {
			result.Diverged++
		}
		if rr.WorkerUnsafe || rr.TeardownError != nil || rr.PersistenceError != nil || (!rr.Passed && !rr.Diverged && !rr.UserFailed) {
			result.Incomplete++
			result.Cause = rr.Cause
			if rr.PersistenceError != nil {
				result.Cause = rr.PersistenceError
			}
		}
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
		if bounded, ok := algo.(interface{ SearchError() error }); ok {
			if err := bounded.SearchError(); err != nil {
				result.Incomplete++
				result.Cause = err
				break
			}
		}

		// Every run allocates fresh transports and starts bridge goroutines in the
		// case-study harnesses. Clean them up even after successful runs so
		// multi-run exploration does not leak state across iterations.
		if (isBug && !cfg.continueAfterFailure) || rr.WorkerUnsafe || rr.TeardownError != nil || rr.PersistenceError != nil || (!rr.Passed && !rr.Diverged && !rr.UserFailed) {
			break
		}
	}

	result.Elapsed = time.Since(start)
	return result
}

// runForExplore wraps run with panic recovery for divergence.
func (o *Orchestrator) runForExplore(t *testing.T, decide func(DecisionPoint) int) RunResult {
	return o.recoverForExplore(func() RunResult { return o.execute(t, decide) })
}

func (o *Orchestrator) recoverForExplore(run func() RunResult) (rr RunResult) {
	rr.DivergenceStep = -1
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
	rr = run()
	return rr
}

// ─── Public API ───

// Run executes the system once with FIFO scheduling.
func (o *Orchestrator) Run(t *testing.T) (RecordedRun, bool) {
	t.Helper()
	rr := o.execute(t, func(dp DecisionPoint) int { return 0 })
	return o.buildRecordedRun(rr), rr.Passed
}

// RunWith executes the system once with a custom decision function.
func (o *Orchestrator) RunWith(t *testing.T, decide func(DecisionPoint) int) RunResult {
	t.Helper()
	return o.execute(t, decide)
}

// Replay re-runs the system following a previously recorded trace.
func (o *Orchestrator) Replay(t *testing.T, rec RecordedRun) (RecordedRun, bool) {
	t.Helper()
	rr := o.execute(t, replayRecordedDecisions(rec.GlobalDecisions))
	return o.buildRecordedRun(rr), rr.Passed
}

// Explore enumerates local and delivery orderings using CHESS DFS.
func (o *Orchestrator) Explore(t *testing.T, setup func(*Orchestrator), opts ...ExploreOption) bool {
	t.Helper()
	cfg := &exploreConfig{bound: 2}
	for _, opt := range opts {
		opt(cfg)
	}
	algo := &CHESS{Bound: cfg.bound}
	result := o.ExploreWith(t, setup, algo, opts...)
	return result.Successful()
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
	return result.Successful()
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
		Outcome:         rr.Outcome, Cause: rr.Cause, WorkerUnsafe: rr.WorkerUnsafe,
	}
}

func replayRecordedDecisions(decisions []GlobalDecision) func(DecisionPoint) int {
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

func (o *Orchestrator) cleanupBubbles() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var errs []error
	for _, addr := range o.order {
		if err := o.stopTransport(o.nodes[addr]); err != nil {
			errs = append(errs, fmt.Errorf("stop %s: %w", addr, err))
		}
	}
	for _, addr := range o.order {
		ctrl := o.nodes[addr]
		if !ctrl.started {
			continue
		}
		ctrl.bubble.Cancel()
		select {
		case <-ctrl.done:
		case <-ctx.Done():
			errs = append(errs, fmt.Errorf("node %s did not join: %w", addr, ctx.Err()))
		}
	}
	return errors.Join(errs...)
}

func (o *Orchestrator) stopTransport(c *nodeCtrl) error {
	if c.stopped {
		return nil
	}
	c.stopped = true
	if s, ok := c.transport.(io.Closer); ok {
		return s.Close()
	}
	if s, ok := c.transport.(shutdowner); ok {
		s.Shutdown()
	}
	return nil
}

func (o *Orchestrator) stopTransportForTeardown(c *nodeCtrl) error {
	if c.stopped {
		return nil
	}
	if closer, ok := c.transport.(teardownCloser); ok {
		c.stopped = true
		return closer.CloseForTeardown()
	}
	return o.stopTransport(c)
}

func (o *Orchestrator) resume(c *nodeCtrl, r distributed.Resume) {
	select {
	case c.bubble.Resume <- r:
	case <-c.done:
	case <-o.ctx.Done():
		panic(context.Cause(o.ctx))
	}
}

// ─── Utilities ───

func (o *Orchestrator) drainOutbox(addr string) {
	ctrl := o.nodes[addr]
	for {
		select {
		case op, ok := <-ctrl.transport.Outbox():
			if !ok {
				return
			}
			o.enqueue(op)
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
