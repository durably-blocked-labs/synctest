// Package orchestratorv2 coordinates multiple synctest bubbles acting as
// distributed nodes that communicate via controlled message delivery.
// Each bubble runs in its own goroutine with an isolated fake clock.
// The orchestrator receives PendingOps from node transports and decides
// which to execute next, giving full control over cross-node message ordering.
//
// Core invariant: sends are always immediately schedulable; recvs are only
// schedulable once a delivered-but-not-yet-consumed message is waiting.
package orchestratorv2

import (
	"reflect"
	"testing"
	"testing/synctest"
)

// OpDir classifies the direction of a pending operation.
type OpDir int

const (
	OpSend OpDir = iota
	OpRecv
)

// PendingOp is the unit of work submitted by a transport to the orchestrator.
// Both sends and recvs carry an Execute closure.
// For sends, Execute delivers the message to the target's mailbox.
// For recvs, Execute grants permission to the waiting bridge goroutine.
type PendingOp struct {
	Dir     OpDir
	From    string  // OpSend: sender addr; OpRecv: waiting node addr
	To      string  // OpSend: target addr; OpRecv: unused
	Type    string  // RPC type name for tracing ("AppendEntries", etc.)
	Execute func()  // non-nil for both OpSend and OpRecv
}

// NodeTransport is the interface transport implementations must satisfy.
// A single Outbox carries both OpSend and OpRecv ops.
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
	// blockedCh is a stub — not yet implemented in synctest.
	// Full impl waits for all N bubbles to signal before scheduling.
	blockedCh chan struct{}
	done      chan struct{}
}

// Orchestrator manages nodes and controls cross-node message delivery.
type Orchestrator struct {
	nodes      map[string]*nodeCtrl
	order      []string // insertion order for deterministic iteration
	globalTime int64

	schedulable  []*PendingOp            // ops ready to execute (FIFO)
	blockedRecvs map[string][]*PendingOp // recv ops waiting for a delivered message; keyed by From
	deliveredTo  map[string]int          // count of delivered-but-not-consumed messages per node

	trace []GlobalStep
}

// New creates an empty orchestrator.
func New() *Orchestrator {
	return &Orchestrator{
		nodes:        make(map[string]*nodeCtrl),
		blockedRecvs: make(map[string][]*PendingOp),
		deliveredTo:  make(map[string]int),
	}
}

// AddNode registers a node transport and its bubble test function.
func (o *Orchestrator) AddNode(transport NodeTransport, f func(t *testing.T)) {
	addr := transport.Addr()
	ctrl := &nodeCtrl{
		transport: transport,
		testFunc:  f,
		blockedCh: make(chan struct{}, 1),
		done:      make(chan struct{}),
	}
	o.nodes[addr] = ctrl
	o.order = append(o.order, addr)
}

// enqueue adds an op to the schedulable queue or defers it if blocked.
func (o *Orchestrator) enqueue(op *PendingOp) {
	switch op.Dir {
	case OpSend:
		// Sends are always immediately schedulable.
		o.schedulable = append(o.schedulable, op)

	case OpRecv:
		// Only schedulable if there is already a delivered-but-unconsumed message.
		if o.deliveredTo[op.From] > 0 {
			o.deliveredTo[op.From]--
			o.schedulable = append(o.schedulable, op)
		} else {
			o.blockedRecvs[op.From] = append(o.blockedRecvs[op.From], op)
		}
	}
}

// deliverNext executes the next schedulable op.
func (o *Orchestrator) deliverNext(t *testing.T) {
	op := o.schedulable[0]
	o.schedulable = o.schedulable[1:]

	go op.Execute() // must not block orchestrator goroutine

	if op.Dir == OpSend {
		// Track that a message is now sitting in the target's mailbox.
		o.deliveredTo[op.To]++
		// If any recv was blocked waiting for this, unblock it now.
		if recvs := o.blockedRecvs[op.To]; len(recvs) > 0 {
			recv := recvs[0]
			o.blockedRecvs[op.To] = recvs[1:]
			o.deliveredTo[op.To]-- // recv will consume this slot
			o.schedulable = append(o.schedulable, recv)
		}
	}

	o.trace = append(o.trace, GlobalStep{
		Type:   StepDeliver,
		Dir:    op.Dir,
		From:   op.From,
		To:     op.To,
		OpType: op.Type,
		Time:   o.globalTime,
	})
	t.Logf("orchestratorv2: deliver %s %s→%s (%s)", dirName(op.Dir), op.From, op.To, op.Type)
}

// startBubble launches a node's test function inside a synctest bubble.
func (o *Orchestrator) startBubble(t *testing.T, ctrl *nodeCtrl) {
	go func() {
		synctest.Test(t, func(t *testing.T) {
			ctrl.testFunc(t)
		})
		close(ctrl.done)
	}()
}

// Run starts all registered nodes and routes messages between them
// until all bubbles complete. Returns the full event trace and whether
// all bubbles passed.
//
// TODO: Full impl waits for all N bubbles to signal via blockedCh before
// scheduling. This stub delivers eagerly as ops arrive.
func (o *Orchestrator) Run(t *testing.T) ([]GlobalStep, bool) {
	t.Helper()

	n := len(o.nodes)

	// Start all bubbles.
	for _, addr := range o.order {
		o.startBubble(t, o.nodes[addr])
	}

	active := n

	// Build reflect.Select cases:
	//   [0..n-1]     outbox channels (one per node)
	//   [n..2n-1]    done channels (one per node)
	//   [2n]         default (non-blocking drain)
	outboxCases := make([]reflect.SelectCase, n)
	doneCases := make([]reflect.SelectCase, n)
	for i, addr := range o.order {
		ctrl := o.nodes[addr]
		outboxCases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ctrl.transport.Outbox()),
		}
		doneCases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ctrl.done),
		}
	}
	defaultCase := reflect.SelectCase{Dir: reflect.SelectDefault}

	allPassed := true

	disableIdx := func(idx int) {
		outboxCases[idx] = reflect.SelectCase{Dir: reflect.SelectRecv} // nil chan blocks forever
		doneCases[idx] = reflect.SelectCase{Dir: reflect.SelectRecv}
	}

	buildCases := func(withDefault bool) []reflect.SelectCase {
		cases := make([]reflect.SelectCase, 0, 2*n+1)
		cases = append(cases, outboxCases...)
		cases = append(cases, doneCases...)
		if withDefault {
			cases = append(cases, defaultCase)
		}
		return cases
	}

	for active > 0 {
		// Non-blocking drain: collect all immediately-available ops.
		for {
			cases := buildCases(true)
			chosen, value, _ := reflect.Select(cases)
			if chosen == 2*n {
				// default: nothing immediately available
				break
			}
			if chosen < n {
				// outbox
				op := value.Interface().(*PendingOp)
				o.enqueue(op)
			} else {
				// done
				idx := chosen - n
				addr := o.order[idx]
				active--
				disableIdx(idx)
				o.trace = append(o.trace, GlobalStep{Type: StepDone, From: addr})
				t.Logf("orchestratorv2: node %q bubble done", addr)
			}
		}

		if len(o.schedulable) > 0 {
			o.deliverNext(t)
			continue
		}

		if active == 0 {
			break
		}

		// Nothing schedulable. Block until next event.
		cases := buildCases(false)
		chosen, value, _ := reflect.Select(cases)
		if chosen < n {
			// outbox
			op := value.Interface().(*PendingOp)
			o.enqueue(op)
		} else {
			// done
			idx := chosen - n
			addr := o.order[idx]
			active--
			disableIdx(idx)
			o.trace = append(o.trace, GlobalStep{Type: StepDone, From: addr})
			t.Logf("orchestratorv2: node %q bubble done", addr)
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
