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

// bubbleStatus tracks per-bubble state for turn-based orchestration.
type bubbleStatus struct {
	hookPending bool   // hookState received, hookReply not yet sent
	waitingFor  string // name of bubble we expect a response from ("" = none)
}

// Run starts all registered bubbles and routes messages between them
// until all bubbles complete. Each bubble's decision hook blocks,
// sending BubbleState to the orchestrator and waiting for a scheduling reply.
//
// Turn-based invariant: when bubble A sends an RPC to bubble B, A stays
// frozen (hookReply withheld) until B sends a response back. The orchestrator
// detects this by tracking waitingFor per bubble.
//
// The reflect.Select cases are laid out as:
//
//	[outbox_0..n-1, hookState_0..n-1, done_0..n-1]
//
// so the chosen index determines whether it was a message, decision, or
// completion event.
func (o *Orchestrator) Run(t *testing.T) {
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

	// Build reflect.SelectCase slice.
	// Layout: [outbox_0..n-1, hookState_0..n-1, done_0..n-1]
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

	active := n
	results := make(map[string]bubbleResult)

	for active > 0 {
		chosen, value, _ := reflect.Select(cases)

		switch {
		case chosen < n:
			// Message from bubble outbox[chosen].
			msg := value.Interface().(Message)
			senderName := o.order[chosen]
			msg.From = senderName

			target, ok := o.bubbles[msg.To]
			if !ok {
				t.Fatalf("orchestrator: bubble %q sent message to unknown bubble %q", senderName, msg.To)
			}

			// Check if this message is a response that unblocks the recipient.
			// If recipient X has waitingFor == sender, this is the response.
			recipientStatus := status[msg.To]
			isResponse := recipientStatus.waitingFor == senderName
			if isResponse {
				recipientStatus.waitingFor = ""
				t.Logf("orchestrator: bubble %q received response from %q, unblocked", msg.To, senderName)
			}

			// Only mark sender as waiting if this is a new request, not a response.
			// A bubble sending a response should remain free to make progress.
			if !isResponse {
				senderStatus := status[senderName]
				senderStatus.waitingFor = msg.To
			}

			// Route message to target inbox.
			target.recv <- msg

			// If the recipient has a pending hook and is now unblocked, release it.
			if recipientStatus.hookPending && recipientStatus.waitingFor == "" {
				o.bubbles[msg.To].hookReply <- 0
				recipientStatus.hookPending = false
				t.Logf("orchestrator: releasing held hook for bubble %q", msg.To)
			}

		case chosen < 2*n:
			// Decision point: bubble needs a scheduling decision.
			idx := chosen - n
			name := o.order[idx]
			state := value.Interface().(synctest.BubbleState)
			t.Logf("orchestrator: bubble %q decision (step=%d, runnable=%d)",
				name, state.Step, state.RunnableN)

			st := status[name]
			st.hookPending = true

			if st.waitingFor == "" {
				// Not waiting for anyone — reply immediately.
				o.bubbles[name].hookReply <- 0
				st.hookPending = false
			} else {
				// Waiting for a response — hold the hook.
				t.Logf("orchestrator: holding bubble %q (waiting for %q)", name, st.waitingFor)
			}

		default:
			// Done signal from bubble done[chosen-2n].
			idx := chosen - 2*n
			name := o.order[idx]
			result := value.Interface().(bubbleResult)
			results[name] = result

			// Disable all cases for this bubble.
			cases[idx] = reflect.SelectCase{Dir: reflect.SelectRecv}
			cases[n+idx] = reflect.SelectCase{Dir: reflect.SelectRecv}
			cases[2*n+idx] = reflect.SelectCase{Dir: reflect.SelectRecv}
			active--

			t.Logf("orchestrator: bubble %q finished (ok=%v, decisions=%d)", name, result.ok, len(result.trace))
		}
	}

	// Fail if any bubble failed.
	for _, name := range o.order {
		if r, ok := results[name]; ok && !r.ok {
			t.Errorf("orchestrator: bubble %q failed", name)
		}
	}
}
