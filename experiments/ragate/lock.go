// Package ragate contains a synthetic Ricart-Agrawala-style lock with an
// injected notification-before-ownership race. It is a protocol fixture, not
// a complete Bubbles transport or GC experiment.
package ragate

import (
	"fmt"
	"sync"
)

type nodeState int

const (
	Released nodeState = iota
	Wanted
	Held
)

// GateRANode implements a Ricart-Agrawala lock with a close(gate) pattern.
//
// Three goroutines run concurrently after AcquireLock is called:
//   - Handler:         reads messages from transport, processes REQUESTs and REPLYs
//   - App (caller):    blocks on <-gate, then sets state=Held (enters CS)
//   - DeferredFlusher: blocks on <-gate, then sends deferred REPLYs
//
// The last REPLY closes gate while state remains Wanted. App publishes Held
// after waking. The flusher incorrectly treats any state other than Held as
// permission to flush, exposing a notification-before-ownership race.
type GateRANode struct {
	id        string
	transport nodeTransport
	peers     []string

	mu       sync.Mutex
	state    nodeState
	clock    int      // Lamport clock
	reqClock int      // clock value in our REQUEST
	deferred []string // peers whose REPLYs are deferred

	repliesReceived int
	gate            chan struct{} // closed when all REPLYs collected

	stopCh chan struct{}
	done   chan struct{}

	// OnViolation reports a flusher about to grant deferred permission while
	// acquisition is still Wanted. It does not establish observed CS overlap.
	OnViolation func(msg string)
}

func NewGateRANode(addr string, transport nodeTransport, peers []string) *GateRANode {
	return &GateRANode{
		id:        addr,
		transport: transport,
		peers:     peers,
		stopCh:    make(chan struct{}),
		done:      make(chan struct{}),
	}
}

// Start launches the handler goroutine.
func (n *GateRANode) Start() {
	go func() {
		defer close(n.done)
		for {
			select {
			case msg, ok := <-n.transport.Mailbox():
				if !ok {
					return // channel closed — transport shut down
				}
				n.handleMessage(msg)
			case <-n.stopCh:
				return
			}
		}
	}()
}

func (n *GateRANode) Stop() {
	close(n.stopCh)
	<-n.done
}

// AcquireLock broadcasts REQUEST to all peers, creates a gate channel,
// launches the DeferredFlusher, and blocks until the gate is closed
// (all REPLYs received). Then sets state=Held.
//
// The DeferredFlusher goroutine also waits on the gate. When the gate
// closes, it flushes any deferred REPLYs. Under non-FIFO local scheduling,
// the flusher may run before state=Held is set.
func (n *GateRANode) AcquireLock() {
	n.mu.Lock()
	n.state = Wanted
	n.clock++
	n.reqClock = n.clock
	n.repliesReceived = 0
	n.gate = make(chan struct{})
	n.deferred = nil
	n.mu.Unlock()

	// Broadcast REQUEST to all peers.
	for _, peer := range n.peers {
		n.transport.Send(peer, Message{
			Kind:      MsgRequest,
			From:      n.id,
			Timestamp: n.reqClock,
		})
	}

	if len(n.peers) == 0 {
		n.mu.Lock()
		n.state = Held
		n.mu.Unlock()
		return
	}

	// Launch the DeferredFlusher: it waits on the gate, then flushes
	// deferred REPLYs. This goroutine and the App goroutine (below)
	// both wake when close(gate) fires, creating a local scheduling
	// decision point.
	go n.deferredFlusher()

	// App goroutine: block until all REPLYs are collected.
	<-n.gate

	// BUG WINDOW: the gate is closed but state is still Wanted. A flusher
	// scheduled before this ownership write passes its incomplete Held check.
	n.mu.Lock()
	n.state = Held
	n.mu.Unlock()
}

// ReleaseLock transitions to Released and sends deferred REPLYs.
func (n *GateRANode) ReleaseLock() {
	n.mu.Lock()
	n.state = Released
	deferred := n.deferred
	n.deferred = nil
	ts := n.clock
	n.mu.Unlock()

	for _, peer := range deferred {
		n.transport.Send(peer, Message{
			Kind:      MsgReply,
			From:      n.id,
			Timestamp: ts,
		})
	}
}

// deferredFlusher waits on the gate, then flushes any deferred REPLYs.
//
// The injected bug is the incomplete Held-only guard: Wanted also means
// deferred replies must wait, but the helper mistakes it for a free lock.
func (n *GateRANode) deferredFlusher() {
	<-n.gate

	n.mu.Lock()
	if n.state == Held {
		n.mu.Unlock()
		return
	}
	if n.state == Wanted && n.OnViolation != nil && len(n.deferred) > 0 {
		n.OnViolation(fmt.Sprintf("node %s: deferredFlusher saw state=Wanted, granting %d premature replies", n.id, len(n.deferred)))
	}
	deferred := n.deferred
	n.deferred = nil
	ts := n.clock
	n.mu.Unlock()

	for _, peer := range deferred {
		n.transport.Send(peer, Message{
			Kind:      MsgReply,
			From:      n.id,
			Timestamp: ts,
		})
	}
}

// handleMessage processes incoming RA protocol messages.
func (n *GateRANode) handleMessage(msg Message) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Update Lamport clock.
	if msg.Timestamp > n.clock {
		n.clock = msg.Timestamp
	}
	n.clock++

	switch msg.Kind {
	case MsgReply:
		if n.state != Wanted {
			return
		}
		n.repliesReceived++
		if n.repliesReceived == len(n.peers) {
			// Notification precedes ownership publication in this faulty variant.
			close(n.gate)
		}

	case MsgRequest:
		shouldDefer := n.state == Held ||
			(n.state == Wanted && (n.reqClock < msg.Timestamp ||
				(n.reqClock == msg.Timestamp && n.id < msg.From)))
		if shouldDefer {
			n.deferred = append(n.deferred, msg.From)
		} else {
			ts := n.clock
			n.mu.Unlock()
			n.transport.Send(msg.From, Message{
				Kind:      MsgReply,
				From:      n.id,
				Timestamp: ts,
			})
			n.mu.Lock()
		}
	}
}
