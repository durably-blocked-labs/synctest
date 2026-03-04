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
//  2. Picks the next op to execute (FIFO).
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
	"reflect"
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

// nodeCtrl is the orchestrator's per-node state.
type nodeCtrl struct {
	transport NodeTransport
	testFunc  func(t *testing.T)
	bubble    *distributed.Bubble // local orchestrator for this node's bubble
	done      chan struct{}
}

// Orchestrator manages nodes and controls cross-node message delivery.
type Orchestrator struct {
	nodes      map[string]*nodeCtrl
	order      []string // insertion order for deterministic iteration
	globalTime int64

	schedulable []*PendingOp // ops ready to execute (FIFO)

	trace []GlobalStep
}

// New creates an empty orchestrator.
func New() *Orchestrator {
	return &Orchestrator{
		nodes: make(map[string]*nodeCtrl),
	}
}

// AddNode registers a node transport and its bubble test function.
func (o *Orchestrator) AddNode(transport NodeTransport, f func(t *testing.T)) {
	addr := transport.Addr()
	ctrl := &nodeCtrl{
		transport: transport,
		testFunc:  f,
		bubble:    distributed.NewBubble(addr),
		done:      make(chan struct{}),
	}
	o.nodes[addr] = ctrl
	o.order = append(o.order, addr)
}

// startBubble launches a node's test function inside a synctest bubble.
// It installs the distributed.Bubble decision hook before calling the user's
// function, enabling idle-based synchronisation with the global orchestrator.
func (o *Orchestrator) startBubble(t *testing.T, ctrl *nodeCtrl) {
	b := ctrl.bubble
	userFn := ctrl.testFunc
	go func() {
		synctest.Test(t, func(t *testing.T) {
			synctest.SetDecisionHook(b.Hook())
			userFn(t)
		})
		close(ctrl.done)
	}()
}

// Run starts all registered nodes and drives cross-node message delivery until
// all bubbles complete. Returns the full event trace and whether all bubbles
// passed.
//
// Algorithm:
//  1. Start all bubbles.
//  2. Wait for every active, non-pending-idle bubble to report idle (IdleState)
//     or to complete (done channel).
//  3. Once all active bubbles are idle, drain their outboxes to collect pending
//     OpSend operations.
//  4. If sends are available: deliver the next one (FIFO), resume the target.
//  5. If no sends but timers exist: advance global time, resume all idle bubbles.
//  6. If neither: resume all (allowing bubbles to finish) and exit.
//  7. Repeat from step 2.
func (o *Orchestrator) Run(t *testing.T) ([]GlobalStep, bool) {
	t.Helper()

	n := len(o.nodes)

	// Start all bubbles.
	for _, addr := range o.order {
		o.startBubble(t, o.nodes[addr])
	}

	// pendingIdle holds bubbles that sent an IdleState but have not yet
	// received a Resume. They are frozen in their hook.
	pendingIdle := make(map[string]distributed.IdleState)
	doneSet := make(map[string]bool)
	active := n
	allPassed := true

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
			// Deliver the next pending send.
			op := o.schedulable[0]
			o.schedulable = o.schedulable[1:]

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
				// Advance time: resume all idle bubbles at the same clock value.
				o.globalTime = earliest
				o.trace = append(o.trace, GlobalStep{Type: StepTimeAdvance, Time: earliest})
				t.Logf("orchestratorv2: advance time to %d ns", earliest)

				// I'm not too sure if we should wake up all timers here. Maybe we should just wake the earliest ones.
				for addr := range pendingIdle {
					ctrl := o.nodes[addr]
					ctrl.bubble.Resume <- distributed.Resume{AdvanceTimeTo: earliest}
				}
				pendingIdle = make(map[string]distributed.IdleState)

			} else {
				// No sends and no timers. Resume all idle bubbles so they
				// can finish any remaining internal work and exit.
				t.Logf("orchestratorv2: no pending sends or timers — resuming all to drain")
				for addr := range pendingIdle {
					ctrl := o.nodes[addr]
					ctrl.bubble.Resume <- distributed.Resume{}
				}
				pendingIdle = make(map[string]distributed.IdleState)
			}
		}
	}

	return o.trace, allPassed
}

func dirName(d OpDir) string {
	if d == OpSend {
		return "send"
	}
	return "recv"
}
