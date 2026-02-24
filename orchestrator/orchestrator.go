// Package orchestrator coordinates multiple synctest bubbles acting as
// distributed nodes that communicate via message passing. Each bubble
// runs in its own goroutine with an isolated fake clock, and the
// orchestrator routes messages between them using reflect.Select.
//
// For MVP: turn-based execution, FIFO intra-bubble scheduling,
// channel-based RPC routing.
package orchestrator

import (
	"reflect"
	"testing"
	"testing/synctest"
)

// Message is the unit of cross-bubble communication.
type Message struct {
	From    string
	To      string
	Payload any
}

// Node is the bubble-side handle for cross-bubble communication.
// All methods use synctest.CallExternal to wrap channel ops so that
// the bubble's fake clock and durable-blocking tracker see these
// as external operations.
type Node struct {
	name string
	send chan<- Message  // bubble → orchestrator
	recv <-chan Message  // orchestrator → bubble
}

// Send sends a message to the named target bubble. The call blocks
// (via CallExternal) until the orchestrator reads the message.
func (n *Node) Send(to string, payload any) {
	msg := Message{From: n.name, To: to, Payload: payload}
	synctest.CallExternal(func() {
		n.send <- msg
	})
}

// Recv blocks until a message arrives from the orchestrator.
func (n *Node) Recv() Message {
	for {
		var msg Message
		got := false
		synctest.CallExternal(func() {
			select {
			case msg = <-n.recv:
				got = true
			default:
			}
		})
		if got {
			return msg
		}
	}
}

// RPC sends a message and waits for a response. Convenience wrapper
// around Send followed by Recv.
func (n *Node) RPC(to string, payload any) Message {
	n.Send(to, payload)
	return n.Recv()
}

// BubbleFunc is the test function signature for a bubble.
type BubbleFunc func(t *testing.T, node *Node)

// bubbleCtrl is the orchestrator's per-bubble state.
type bubbleCtrl struct {
	name     string
	testFunc BubbleFunc
	send     chan Message      // bubble → orchestrator (unbuffered)
	recv     chan Message      // orchestrator → bubble (buffered 1)
	done      chan bubbleResult        // signals bubble completion (buffered 1)
	hookState chan synctest.BubbleState // hook → orchestrator (unbuffered)
	hookReply chan int32               // orchestrator → hook (unbuffered)
}

type bubbleResult struct {
	trace []synctest.Decision
	ok    bool
}

// Orchestrator manages bubbles and routes messages between them.
type Orchestrator struct {
	bubbles map[string]*bubbleCtrl
	order   []string // insertion order for deterministic iteration
}

// New creates an empty orchestrator.
func New() *Orchestrator {
	return &Orchestrator{
		bubbles: make(map[string]*bubbleCtrl),
	}
}

// AddBubble registers a bubble with the given name and test function.
func (o *Orchestrator) AddBubble(name string, f BubbleFunc) {
	ctrl := &bubbleCtrl{
		name:     name,
		testFunc: f,
		send:     make(chan Message),
		recv:     make(chan Message, 1),
		done:      make(chan bubbleResult, 1),
		hookState: make(chan synctest.BubbleState),
		hookReply: make(chan int32),
	}
	o.bubbles[name] = ctrl
	o.order = append(o.order, name)
}

// startBubble launches a bubble in a goroutine. The bubble runs inside
// synctest.Explore with a decision hook that blocks, sending state to
// the orchestrator and waiting for a scheduling reply.
func (o *Orchestrator) startBubble(t *testing.T, ctrl *bubbleCtrl) {
	node := &Node{name: ctrl.name, send: ctrl.send, recv: ctrl.recv}
	go func() {
		trace, ok := synctest.Explore(t, func(t *testing.T) {
			synctest.SetDecisionHook(func(state synctest.BubbleState) int32 {
				ctrl.hookState <- state
				return <-ctrl.hookReply
			})
			ctrl.testFunc(t, node)
		}, nil)
		ctrl.done <- bubbleResult{trace, ok}
	}()
}

// EventType classifies what happened in one orchestrator loop iteration.
type EventType int

const (
	EventOutbox EventType = iota // bubble sent a message via outbox
	EventHook                    // bubble's decision hook fired
	EventDone                    // bubble completed
)

// GlobalStep records one orchestrator event loop iteration.
type GlobalStep struct {
	Bubble    string    // which bubble this event came from
	Event     EventType // what kind of event
	HookReply int32     // reply sent to decision hook (only for EventHook)
}

// bubbleStatus tracks per-bubble state for turn-based orchestration.
type bubbleStatus struct {
	hookPending      bool   // hookState received, hookReply not yet sent
	waitingFor       string // name of bubble we expect a response from ("" = none)
	pendingHookReply int32  // reply to send when hook is released
}

// Run starts all registered bubbles and routes messages between them
// until all bubbles complete. It delegates to RunExplore with a nil prefix.
func (o *Orchestrator) Run(t *testing.T) {
	t.Helper()
	_, _ = o.RunExplore(t, nil)
}

// RunExplore starts all bubbles and runs the event loop with record/replay support.
//
// In record mode (len(prefix) == 0): uses reflect.Select as normal, appending
// each event as a GlobalStep to the trace.
//
// In replay mode (len(prefix) > 0): uses reflect.Select identically to record
// mode, but buffers outbox/done events that arrive out of prefix order and
// processes them in the recorded sequence. Hook events are processed immediately
// with the same hold/release logic as record mode, preserving the turn-based
// invariant: bubbles waiting for a response have their hooks held until the
// response arrives.
//
// Returns the full trace and whether all bubbles passed.
func (o *Orchestrator) RunExplore(t *testing.T, prefix []GlobalStep) ([]GlobalStep, bool) {
	t.Helper()

	n := len(o.bubbles)

	// Per-bubble status tracking.
	status := make(map[string]*bubbleStatus, n)
	for _, name := range o.order {
		status[name] = &bubbleStatus{}
	}

	// Start all bubbles.
	for _, name := range o.order {
		o.startBubble(t, o.bubbles[name])
	}

	active := n
	results := make(map[string]bubbleResult)
	var trace []GlobalStep
	allPassed := true

	// processOutbox handles a message from a bubble's outbox channel.
	processOutbox := func(bubbleName string, msg Message) {
		msg.From = bubbleName

		target, ok := o.bubbles[msg.To]
		if !ok {
			t.Fatalf("orchestrator: bubble %q sent message to unknown bubble %q", bubbleName, msg.To)
		}

		trace = append(trace, GlobalStep{Bubble: bubbleName, Event: EventOutbox})

		recipientStatus := status[msg.To]
		isResponse := recipientStatus.waitingFor == bubbleName
		if isResponse {
			recipientStatus.waitingFor = ""
			t.Logf("orchestrator: bubble %q received response from %q, unblocked", msg.To, bubbleName)
		}
		if !isResponse {
			senderStatus := status[bubbleName]
			senderStatus.waitingFor = msg.To
		}

		target.recv <- msg

		if recipientStatus.hookPending && recipientStatus.waitingFor == "" {
			o.bubbles[msg.To].hookReply <- recipientStatus.pendingHookReply
			recipientStatus.hookPending = false
			t.Logf("orchestrator: releasing held hook for bubble %q", msg.To)
		}
	}

	// processHook handles a scheduling decision from a bubble.
	// 
	// Is it possible for hook to be received before the RPC?
	processHook := func(bubbleName string, state synctest.BubbleState, hookReply int32) {
		t.Logf("orchestrator: bubble %q decision (step=%d, runnable=%d)",
			bubbleName, state.Step, state.RunnableN)

		st := status[bubbleName]
		st.hookPending = true
		st.pendingHookReply = hookReply

		trace = append(trace, GlobalStep{Bubble: bubbleName, Event: EventHook, HookReply: hookReply})

		if st.waitingFor == "" {
			o.bubbles[bubbleName].hookReply <- st.pendingHookReply
			st.hookPending = false
		} else {
			t.Logf("orchestrator: holding bubble %q (waiting for %q)", bubbleName, st.waitingFor)
		}
	}

	// processDone handles a bubble completion.
	processDone := func(bubbleName string, result bubbleResult) {
		results[bubbleName] = result
		trace = append(trace, GlobalStep{Bubble: bubbleName, Event: EventDone})
		active--
		t.Logf("orchestrator: bubble %q finished (ok=%v, decisions=%d)", bubbleName, result.ok, len(result.trace))
	}

	if len(prefix) > 0 {
		// Replay mode.
		//
		// Uses reflect.Select directly (same as record mode) to receive events
		// from all bubble channels. Hook events are processed immediately with
		// the same hold/release logic as record mode, preserving the turn-based
		// invariant. Outbox and done events are buffered and processed in the
		// order recorded in the prefix, ensuring deterministic message routing
		// regardless of reflect.Select's non-deterministic channel selection.
		cases := make([]reflect.SelectCase, 3*n)
		for i, name := range o.order {
			ctrl := o.bubbles[name]
			cases[i] = reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(ctrl.send),
			}
			cases[n+i] = reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(ctrl.hookState),
			}
			cases[2*n+i] = reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(ctrl.done),
			}
		}

		// Build replay schedule from prefix: outbox and done events only.
		type eventKey struct {
			bubble string
			event  EventType
		}
		pending := make(map[eventKey][]any)

		var replaySteps []GlobalStep
		for _, step := range prefix {
			if step.Event == EventOutbox || step.Event == EventDone {
				replaySteps = append(replaySteps, step)
			}
		}
		replayIdx := 0

		// flushPending processes buffered events that match the next prefix steps.
		flushPending := func() {
			for replayIdx < len(replaySteps) {
				step := replaySteps[replayIdx]
				key := eventKey{step.Bubble, step.Event}
				if len(pending[key]) == 0 {
					return
				}
				val := pending[key][0]
				pending[key] = pending[key][1:]
				switch step.Event {
				case EventOutbox:
					processOutbox(step.Bubble, val.(Message))
				case EventDone:
					processDone(step.Bubble, val.(bubbleResult))
				}
				replayIdx++
			}
		}

		for active > 0 {
			chosen, value, _ := reflect.Select(cases)

			switch {
			case chosen < n:
				name := o.order[chosen]
				if replayIdx < len(replaySteps) {
					key := eventKey{name, EventOutbox}
					pending[key] = append(pending[key], value.Interface().(Message))
					flushPending()
				} else {
					processOutbox(name, value.Interface().(Message))
				}
			case chosen < 2*n:
				processHook(o.order[chosen-n], value.Interface().(synctest.BubbleState), 0)
			default:
				idx := chosen - 2*n
				name := o.order[idx]
				cases[idx] = reflect.SelectCase{Dir: reflect.SelectRecv}
				cases[n+idx] = reflect.SelectCase{Dir: reflect.SelectRecv}
				cases[2*n+idx] = reflect.SelectCase{Dir: reflect.SelectRecv}
				if replayIdx < len(replaySteps) {
					key := eventKey{name, EventDone}
					pending[key] = append(pending[key], value.Interface().(bubbleResult))
					flushPending()
				} else {
					processDone(name, value.Interface().(bubbleResult))
				}
			}
		}
	} else {
		// Record mode: use reflect.Select directly.
		cases := make([]reflect.SelectCase, 3*n)
		for i, name := range o.order {
			ctrl := o.bubbles[name]
			cases[i] = reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(ctrl.send),
			}
			cases[n+i] = reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(ctrl.hookState),
			}
			cases[2*n+i] = reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(ctrl.done),
			}
		}

		for active > 0 {
			chosen, value, _ := reflect.Select(cases)

			switch {
			case chosen < n:
				processOutbox(o.order[chosen], value.Interface().(Message))
			case chosen < 2*n:
				processHook(o.order[chosen-n], value.Interface().(synctest.BubbleState), 0)
			default:
				idx := chosen - 2*n
				processDone(o.order[idx], value.Interface().(bubbleResult))
				cases[idx] = reflect.SelectCase{Dir: reflect.SelectRecv}
				cases[n+idx] = reflect.SelectCase{Dir: reflect.SelectRecv}
				cases[2*n+idx] = reflect.SelectCase{Dir: reflect.SelectRecv}
			}
		}
	}

	// Check if any bubble failed.
	for _, name := range o.order {
		if r, ok := results[name]; ok && !r.ok {
			t.Errorf("orchestrator: bubble %q failed", name)
			allPassed = false
		}
	}

	return trace, allPassed
}
