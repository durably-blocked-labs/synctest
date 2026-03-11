// Package orchestratorv2 coordinates multiple synctest bubbles acting as
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
package orchestratorv2

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/shubhaankar/synctest/distributed"
)

// OpDir classifies the direction of a pending operation.
type OpDir int

const (
	OpSend OpDir = iota
	OpRecv // retained for trace compatibility; no longer submitted by transports
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
	Addr()   string
	Outbox() <-chan *PendingOp
}

// GlobalStepType classifies what happened in one orchestrator loop iteration.
type GlobalStepType int

const (
	StepDeliver     GlobalStepType = iota // an op was executed
	StepTimeAdvance                        // global virtual clock advanced
	StepDone                               // a node's bubble completed
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

// ExploreOption configures Explore behavior.
type ExploreOption func(*exploreConfig)

type exploreConfig struct {
	bound   int // max non-FIFO delivery decisions per trace (default 2)
	maxRuns int // hard cap on total runs (0 = unlimited)
}

// GlobalBound sets the maximum number of non-FIFO delivery decisions per
// explored trace. Defaults to 2.
func GlobalBound(k int) ExploreOption { return func(c *exploreConfig) { c.bound = k } }

// GlobalMaxRuns sets a hard cap on the total number of traces explored.
// Defaults to 0 (unlimited).
func GlobalMaxRuns(n int) ExploreOption { return func(c *exploreConfig) { c.maxRuns = n } }

// RecordedRun captures a complete orchestrator run for deterministic replay.
//
// GlobalTrace is the sequence of global events (message deliveries, time
// advances, node completions). LocalTraces contains the per-node scheduling
// decisions recorded inside each bubble. Together they fully specify the
// execution and can be fed back into Replay to reproduce it exactly.
type RecordedRun struct {
	GlobalTrace []GlobalStep
	LocalTraces map[string][]synctest.Decision
}

// nodeCtrl is the orchestrator's per-node state.
// The bubble field is nil until Run or Replay initialises it.
type nodeCtrl struct {
	transport  NodeTransport
	testFunc   func(t *testing.T)
	bubble     *distributed.Bubble // assigned in initBubbles; nil after AddNode
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

// New creates an empty orchestrator.
func New() *Orchestrator {
	return &Orchestrator{
		nodes: make(map[string]*nodeCtrl),
	}
}

// AddNode registers a node transport and its bubble test function.
// The bubble is created lazily when Run or Replay is called.
func (o *Orchestrator) AddNode(transport NodeTransport, f func(t *testing.T)) {
	addr := transport.Addr()
	ctrl := &nodeCtrl{
		transport: transport,
		testFunc:  f,
	}
	o.nodes[addr] = ctrl
	o.order = append(o.order, addr)
}

// initBubbles (re-)creates all per-node bubbles.
// localPrefixes maps node address → local scheduling prefix to replay;
// a nil map creates fresh bubbles with default FIFO scheduling.
func (o *Orchestrator) initBubbles(localPrefixes map[string][]synctest.Decision) {
	for _, addr := range o.order {
		ctrl := o.nodes[addr]
		opts := []distributed.Option{}
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
		trace, ok := synctest.Explore(t, func(t *testing.T) {
			synctest.SetDecisionHook(b.Hook())
			userFn(t)
		}, nil)
		ctrl.localTrace = trace
		ctrl.passed = ok
		if !ok {
			t.Fail()
		}
		close(ctrl.done)
	}()
}

// Run starts all registered nodes and drives cross-node message delivery until
// all bubbles complete. Delivery order is FIFO over the ops drained from each
// node's outbox. Returns a RecordedRun that can be fed into Replay.
func (o *Orchestrator) Run(t *testing.T) (RecordedRun, bool) {
	t.Helper()
	o.initBubbles(nil)
	return o.run(t, nil)
}

// Replay re-runs all registered nodes, replaying the local scheduling
// decisions from rec.LocalTraces and following rec.GlobalTrace's delivery
// order for cross-node messages. Both global and local behaviour should be
// identical to the original run, making the returned RecordedRun equal to rec.
//
// Replay requires a freshly configured Orchestrator (same AddNode calls as
// the original run, but new transport instances with the same connectivity).
func (o *Orchestrator) Replay(t *testing.T, rec RecordedRun) (RecordedRun, bool) {
	t.Helper()
	o.initBubbles(rec.LocalTraces)
	return o.run(t, rec.GlobalTrace)
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
// calling Run. Explore	 resets o.nodes and o.order before each setup call so
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
		// synctest.Explore returning, which allocates make([]bubbleDecision, n)
		// inside synctestRunImpl. That allocation can trigger gcStart and
		// gcBgMarkStartWorkers, which launches mark worker goroutines that
		// interact with P state left over from the completed bubble — corrupting
		// sudogs. Running GC here flushes those workers cleanly before any new
		// bubbles are created.
		runtime.GC()

		_, decisions, passed := o.runWithGlobalPrefix(t, item.prefix)
		runCount++

		// Log a compact delivery sequence so different orderings are visible.
		var sb strings.Builder
		for _, step := range o.trace {
			if step.Type == StepDeliver {
				if sb.Len() > 0 {
					sb.WriteByte(' ')
				}
				fmt.Fprintf(&sb, "%s→%s(%s)", step.From, step.To, step.OpType)
			}
		}
		t.Logf("orchestratorv2: explore: run %d prefix=%v\n\ttrace: %s", runCount, item.prefix, sb.String())

		if !passed {
			t.Logf("orchestratorv2: explore: FAILED on run %d (prefix len %d)", runCount, len(item.prefix))
			return false
		}

		// Enqueue new work items for unexplored alternatives past the current prefix.
		for step := len(item.prefix); step < len(decisions); step++ {
			d := decisions[step]
			if d.QueueSize <= 1 {
				continue
			}
			for alt := 1; alt < d.QueueSize; alt++ {
				newNonFIFO := item.nonFIFO + 1 // alt > 0, so always non-FIFO
				if newNonFIFO > cfg.bound {
					continue
				}
				newPrefix := make([]GlobalDecision, step+1)
				copy(newPrefix, decisions[:step])
				newPrefix[step] = GlobalDecision{Index: alt, QueueSize: d.QueueSize}
				stack = append(stack, exploreItem{prefix: newPrefix, nonFIFO: newNonFIFO})
			}
		}
	}

	t.Logf("orchestratorv2: explore: explored %d interleavings, all passed", runCount)
	return true
}

// run is the shared implementation of Run and Replay.
//
// deliveryTrace is the sequence of GlobalSteps from a previous run; when
// non-nil, each StepDeliver entry is matched against the schedulable queue and
// that specific op is delivered first (replay ordering). When nil, FIFO order
// is used (normal run).
//
// Algorithm:
//  1. Start all bubbles.
//  2. Wait for every active, non-pending-idle bubble to report idle (IdleState)
//     or to complete (done channel).
//  3. Once all active bubbles are idle, drain their outboxes to collect pending
//     OpSend operations.
//  4. If sends are available: deliver the next one, resume the target.
//  5. If no sends but timers exist: advance global time, resume all idle bubbles.
//  6. If neither: resume all (allowing bubbles to finish) and return.
//  7. Repeat from step 2.
func (o *Orchestrator) run(t *testing.T, deliveryTrace []GlobalStep) (RecordedRun, bool) {
	// Reset orchestrator run state.
	o.trace = nil
	o.schedulable = nil
	o.globalTime = 0

	n := len(o.nodes)

	// Pre-filter delivery steps for O(1) indexed lookup during replay.
	var deliveries []GlobalStep
	deliveryIdx := 0
	if deliveryTrace != nil {
		for _, step := range deliveryTrace {
			if step.Type == StepDeliver {
				deliveries = append(deliveries, step)
			}
		}
	}

	// pendingIdle holds bubbles that sent an IdleState but have not yet
	// received a Resume. They are frozen in their hook.
	pendingIdle := make(map[string]distributed.IdleState)
	doneSet := make(map[string]bool)
	active := n

	// Start bubbles one at a time, waiting for each to reach its first idle
	// point before starting the next. Sequential startup eliminates concurrent
	// access to shared resources (e.g. a seeded *rand.Rand in the application
	// under test) during the initialization phase, which is necessary for
	// fully deterministic replay. After startup all active bubbles are in
	// pendingIdle and the delivery loop below takes over.
	for _, addr := range o.order {
		o.startBubble(t, o.nodes[addr])
		ctrl := o.nodes[addr]
		select {
		case idle := <-ctrl.bubble.Idle:
			pendingIdle[addr] = idle
		case <-ctrl.done:
			doneSet[addr] = true
			active--
			o.trace = append(o.trace, GlobalStep{Type: StepDone, From: addr})
			t.Logf("orchestratorv2: node %q bubble done during init", addr)
		}
	}

	for active > 0 {
		// ── Step 2: collect idle states from all non-pending, non-done bubbles ──
		//
		// Build a reflect.Select over the Idle and done channels of every bubble
		// that is neither already in pendingIdle nor already done. We loop until
		// every active bubble is accounted for.
		for {
			// Count how many active bubbles still need to report.
			needed := 0
			for _, addr := range o.order {
				if doneSet[addr] {
					continue
				}
				if _, ok := pendingIdle[addr]; ok {
					continue
				}
				needed++
			}
			if needed == 0 {
				break
			}

			// Build select cases: [idle_0, done_0, idle_1, done_1, ...]
			cases := make([]reflect.SelectCase, 0, needed*2)
			addrs := make([]string, 0, needed*2)
			isDone := make([]bool, 0, needed*2)

			for _, addr := range o.order {
				if doneSet[addr] {
					continue
				}
				if _, ok := pendingIdle[addr]; ok {
					continue
				}
				ctrl := o.nodes[addr]
				cases = append(cases, reflect.SelectCase{
					Dir:  reflect.SelectRecv,
					Chan: reflect.ValueOf(ctrl.bubble.Idle),
				})
				addrs = append(addrs, addr)
				isDone = append(isDone, false)

				cases = append(cases, reflect.SelectCase{
					Dir:  reflect.SelectRecv,
					Chan: reflect.ValueOf(ctrl.done),
				})
				addrs = append(addrs, addr)
				isDone = append(isDone, true)
			}

			chosen, val, _ := reflect.Select(cases)
			addr := addrs[chosen]

			if isDone[chosen] {
				doneSet[addr] = true
				delete(pendingIdle, addr)
				active--
				o.trace = append(o.trace, GlobalStep{Type: StepDone, From: addr})
				t.Logf("orchestratorv2: node %q bubble done", addr)
			} else {
				idleState := val.Interface().(distributed.IdleState)
				pendingIdle[addr] = idleState
			}
		}

		if active == 0 {
			break
		}

		// ── Step 3: drain outboxes (non-blocking) ──
		//
		// Each sending goroutine is frozen in ExternalWait on an unbuffered
		// outbox channel. Reading from that channel completes the goroutine's
		// ExternalWait (puts it in the bubble's runq) but the bubble stays
		// frozen — its hook is still blocking on Resume.
		for _, addr := range o.order {
			if doneSet[addr] {
				continue
			}
			ctrl := o.nodes[addr]
			select {
			case op := <-ctrl.transport.Outbox():
				o.schedulable = append(o.schedulable, op)
			default:
			}
		}

		// ── Steps 4–6: make a scheduling decision ──

		if len(o.schedulable) > 0 {
			// Select which op to deliver: follow recorded order during replay,
			// or take the first (FIFO) during a free run.
			var op *PendingOp
			if deliveries != nil && deliveryIdx < len(deliveries) {
				want := deliveries[deliveryIdx]
				idx := findDeliveryOp(o.schedulable, want)
				if idx < 0 {
					// The run has diverged from the recorded trace (e.g. due
					// to application-level randomness like timer jitter). Log
					// a warning and switch to FIFO for the remainder of the
					// replay — the replay is best-effort.
					t.Logf("orchestratorv2: replay diverged: no op matching %s→%s (%s); switching to FIFO",
						want.From, want.To, want.OpType)
					deliveries = nil // disable replay ordering for rest of run
					op = o.schedulable[0]
					o.schedulable = o.schedulable[1:]
				} else {
					op = o.schedulable[idx]
					o.schedulable = append(o.schedulable[:idx], o.schedulable[idx+1:]...)
					deliveryIdx++
				}
			} else {
				op = o.schedulable[0]
				o.schedulable = o.schedulable[1:]
			}

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
			t.Logf("orchestratorv2: deliver %s %s→%s (%s)", dirName(op.Dir), op.From, op.To, op.Type)

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
				t.Logf("orchestratorv2: advance time to %d ns", earliest)

				for _, addr := range o.order {
					if _, idle := pendingIdle[addr]; !idle {
						continue
					}
					delete(pendingIdle, addr)
					ctrl := o.nodes[addr]
					ctrl.bubble.Resume <- distributed.Resume{AdvanceTimeTo: earliest}
					select {
					case idleState := <-ctrl.bubble.Idle:
						pendingIdle[addr] = idleState
					case <-ctrl.done:
						if !doneSet[addr] {
							doneSet[addr] = true
							active--
							o.trace = append(o.trace, GlobalStep{Type: StepDone, From: addr})
							t.Logf("orchestratorv2: node %q bubble done during time advance", addr)
						}
					}
				}

			} else {
				// No sends and no timers. Resume idle bubbles one at a time so
				// they can finish any remaining internal work and exit.
				t.Logf("orchestratorv2: no pending sends or timers — resuming all to drain")
				for _, addr := range o.order {
					if _, idle := pendingIdle[addr]; !idle {
						continue
					}
					delete(pendingIdle, addr)
					ctrl := o.nodes[addr]
					ctrl.bubble.Resume <- distributed.Resume{}
					select {
					case idleState := <-ctrl.bubble.Idle:
						pendingIdle[addr] = idleState
					case <-ctrl.done:
						if !doneSet[addr] {
							doneSet[addr] = true
							active--
							o.trace = append(o.trace, GlobalStep{Type: StepDone, From: addr})
							t.Logf("orchestratorv2: node %q bubble done during drain", addr)
						}
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
		GlobalTrace: o.trace,
		LocalTraces: localTraces,
	}, allPassed
}

// runWithGlobalPrefix is the core engine for Explore. It runs a single trace
// following the given global delivery prefix and returns the full sequence of
// GlobalDecisions recorded (one per delivery step). The first len(prefix) steps
// follow the prefix; steps beyond that default to FIFO (index 0).
//
// Unlike run(), this method fully drains every node's outbox before making
// each delivery decision, giving the richest branching view at each step.
func (o *Orchestrator) runWithGlobalPrefix(t *testing.T, prefix []GlobalDecision) (RecordedRun, []GlobalDecision, bool) {
	// Reset orchestrator run state.
	o.trace = nil
	o.schedulable = nil
	o.globalTime = 0

	n := len(o.nodes)
	var globalDecisions []GlobalDecision

	pendingIdle := make(map[string]distributed.IdleState)
	doneSet := make(map[string]bool)
	active := n

	// Sequential startup (same as run()).
	for _, addr := range o.order {
		o.startBubble(t, o.nodes[addr])
		ctrl := o.nodes[addr]
		select {
		case idle := <-ctrl.bubble.Idle:
			pendingIdle[addr] = idle
		case <-ctrl.done:
			doneSet[addr] = true
			active--
			o.trace = append(o.trace, GlobalStep{Type: StepDone, From: addr})
			t.Logf("orchestratorv2: [explore] node %q done during init", addr)
		}
	}

	for active > 0 {
		// Collect idle states from all active, non-pending-idle bubbles.
		for {
			needed := 0
			for _, addr := range o.order {
				if doneSet[addr] {
					continue
				}
				if _, ok := pendingIdle[addr]; ok {
					continue
				}
				needed++
			}
			if needed == 0 {
				break
			}

			cases := make([]reflect.SelectCase, 0, needed*2)
			addrs := make([]string, 0, needed*2)
			isDone := make([]bool, 0, needed*2)

			for _, addr := range o.order {
				if doneSet[addr] {
					continue
				}
				if _, ok := pendingIdle[addr]; ok {
					continue
				}
				ctrl := o.nodes[addr]
				cases = append(cases, reflect.SelectCase{
					Dir:  reflect.SelectRecv,
					Chan: reflect.ValueOf(ctrl.bubble.Idle),
				})
				addrs = append(addrs, addr)
				isDone = append(isDone, false)

				cases = append(cases, reflect.SelectCase{
					Dir:  reflect.SelectRecv,
					Chan: reflect.ValueOf(ctrl.done),
				})
				addrs = append(addrs, addr)
				isDone = append(isDone, true)
			}

			chosen, val, _ := reflect.Select(cases)
			addr := addrs[chosen]

			if isDone[chosen] {
				doneSet[addr] = true
				delete(pendingIdle, addr)
				active--
				o.trace = append(o.trace, GlobalStep{Type: StepDone, From: addr})
				t.Logf("orchestratorv2: [explore] node %q done", addr)
			} else {
				pendingIdle[addr] = val.Interface().(distributed.IdleState)
			}
		}

		if active == 0 {
			break
		}

		// Drain outboxes (one op per node, same as run()).
		// Ops accumulate in o.schedulable across iterations, giving a growing
		// branching factor as the run progresses.
		for _, addr := range o.order {
			if doneSet[addr] {
				continue
			}
			ctrl := o.nodes[addr]
			select {
			case op := <-ctrl.transport.Outbox():
				o.schedulable = append(o.schedulable, op)
			default:
			}
		}

		if len(o.schedulable) > 0 {
			// Index-based delivery selection: follow prefix if available,
			// otherwise default to FIFO (index 0).
			step := len(globalDecisions)
			chosenIdx := 0
			if step < len(prefix) {
				chosenIdx = prefix[step].Index
				if chosenIdx >= len(o.schedulable) {
					chosenIdx = len(o.schedulable) - 1 // clamp on divergence
				}
			}

			globalDecisions = append(globalDecisions, GlobalDecision{
				Index:     chosenIdx,
				QueueSize: len(o.schedulable),
			})

			op := o.schedulable[chosenIdx]
			o.schedulable = append(o.schedulable[:chosenIdx], o.schedulable[chosenIdx+1:]...)

			op.Execute()
			o.trace = append(o.trace, GlobalStep{
				Type:   StepDeliver,
				Dir:    op.Dir,
				From:   op.From,
				To:     op.To,
				OpType: op.Type,
				Time:   o.globalTime,
			})
			t.Logf("orchestratorv2: [explore] deliver %s %s→%s (%s)", dirName(op.Dir), op.From, op.To, op.Type)

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
				o.globalTime = earliest
				o.trace = append(o.trace, GlobalStep{Type: StepTimeAdvance, Time: earliest})
				t.Logf("orchestratorv2: [explore] advance time to %d ns", earliest)

				for _, addr := range o.order {
					if _, idle := pendingIdle[addr]; !idle {
						continue
					}
					delete(pendingIdle, addr)
					ctrl := o.nodes[addr]
					ctrl.bubble.Resume <- distributed.Resume{AdvanceTimeTo: earliest}
					select {
					case idleState := <-ctrl.bubble.Idle:
						pendingIdle[addr] = idleState
					case <-ctrl.done:
						if !doneSet[addr] {
							doneSet[addr] = true
							active--
							o.trace = append(o.trace, GlobalStep{Type: StepDone, From: addr})
							t.Logf("orchestratorv2: [explore] node %q done during time advance", addr)
						}
					}
				}

			} else {
				t.Logf("orchestratorv2: [explore] no pending sends or timers — resuming all to drain")
				for _, addr := range o.order {
					if _, idle := pendingIdle[addr]; !idle {
						continue
					}
					delete(pendingIdle, addr)
					ctrl := o.nodes[addr]
					ctrl.bubble.Resume <- distributed.Resume{}
					select {
					case idleState := <-ctrl.bubble.Idle:
						pendingIdle[addr] = idleState
					case <-ctrl.done:
						if !doneSet[addr] {
							doneSet[addr] = true
							active--
							o.trace = append(o.trace, GlobalStep{Type: StepDone, From: addr})
							t.Logf("orchestratorv2: [explore] node %q done during drain", addr)
						}
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
		GlobalTrace: o.trace,
		LocalTraces: localTraces,
	}, globalDecisions, allPassed
}

// findDeliveryOp returns the index in schedulable of the first op whose
// From, To, and Type match those of the given GlobalStep.
// Returns -1 if no match is found.
func findDeliveryOp(schedulable []*PendingOp, want GlobalStep) int {
	for i, op := range schedulable {
		if op.From == want.From && op.To == want.To && op.Type == want.OpType {
			return i
		}
	}
	return -1
}

func dirName(d OpDir) string {
	if d == OpSend {
		return "send"
	}
	return "recv"
}
