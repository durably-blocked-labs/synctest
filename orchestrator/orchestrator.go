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
	"runtime"
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
	var msg Message
	synctest.CallExternal(func() {
		msg = <-n.recv
	})
	return msg
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
	done     chan bubbleResult // signals bubble completion (buffered 1)
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
		done:     make(chan bubbleResult, 1),
	}
	o.bubbles[name] = ctrl
	o.order = append(o.order, name)
}

// startBubble launches a bubble in a goroutine. The bubble runs inside
// synctest.Explore with FIFO scheduling (decision hook returns 0).
func (o *Orchestrator) startBubble(t *testing.T, ctrl *bubbleCtrl) {
	node := &Node{name: ctrl.name, send: ctrl.send, recv: ctrl.recv}
	go func() {
		trace, ok := synctest.Explore(t, func(t *testing.T) {
			synctest.SetDecisionHook(func(state synctest.BubbleState) int32 {
				return 0 // FIFO for MVP
			})
			ctrl.testFunc(t, node)
		}, nil)
		ctrl.done <- bubbleResult{trace, ok}
	}()
}

// Run starts all registered bubbles and routes messages between them
// until all bubbles complete. Uses reflect.Select for N-bubble support.
//
// The select cases are laid out as:
//
//	[outbox_0, outbox_1, ..., done_0, done_1, ...]
//
// so the chosen index determines whether it was a message or completion event.
func (o *Orchestrator) Run(t *testing.T) {
	t.Helper()

	n := len(o.bubbles)
	runtime.GOMAXPROCS(n + 1)

	// Start all bubbles.
	for _, name := range o.order {
		o.startBubble(t, o.bubbles[name])
	}

	// Build reflect.SelectCase slice.
	// First n cases: recv from each bubble's outbox (send channel).
	// Next n cases: recv from each bubble's done channel.
	cases := make([]reflect.SelectCase, 2*n)
	for i, name := range o.order {
		ctrl := o.bubbles[name]
		cases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ctrl.send),
		}
		cases[n+i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ctrl.done),
		}
	}

	active := n
	results := make(map[string]bubbleResult)

	for active > 0 {
		chosen, value, _ := reflect.Select(cases)

		if chosen < n {
			// Message from bubble outbox[chosen].
			msg := value.Interface().(Message)
			senderName := o.order[chosen]
			msg.From = senderName

			target, ok := o.bubbles[msg.To]
			if !ok {
				t.Fatalf("orchestrator: bubble %q sent message to unknown bubble %q", senderName, msg.To)
			}
			target.recv <- msg
		} else {
			// Done signal from bubble done[chosen-n].
			idx := chosen - n
			name := o.order[idx]
			result := value.Interface().(bubbleResult)
			results[name] = result

			// Disable this bubble's cases by setting channels to nil.
			cases[idx] = reflect.SelectCase{Dir: reflect.SelectRecv}
			cases[n+idx] = reflect.SelectCase{Dir: reflect.SelectRecv}
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
