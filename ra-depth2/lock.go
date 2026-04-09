// Package radepth2 demonstrates a depth-2 G+L bug in a Ricart-Agrawala
// distributed lock variant that uses close(gate) to wake multiple goroutines.
//
// The close(gate) pattern: when all REPLY messages are collected, the handler
// closes a gate channel that wakes both an App goroutine (which enters the CS)
// and a DeferredFlusher goroutine (which processes deferred REQUESTs). Under
// FIFO local scheduling the App goroutine runs first and sets state=Held before
// the DeferredFlusher checks state. Under non-FIFO local scheduling the
// DeferredFlusher may run first, see state=Released (not Held, not Wanted), and
// grant a deferred REQUEST — violating mutual exclusion.
//
// Finding this bug requires two non-FIFO decisions:
//
//  1. Global (G): reorder message delivery so a REQUEST from another node
//     arrives while the winner is still collecting REPLYs, causing a deferred
//     reply to be queued.
//  2. Local (L): after close(gate) wakes both goroutines, schedule the
//     DeferredFlusher before the App goroutine.
//
// Neither decision alone triggers the bug: without (1) the deferred queue is
// empty so the flusher is a no-op; without (2) the App sets state=Held before
// the flusher runs. Only ExploreAll (combined G+L search) finds it.
package radepth2

import "sync"

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
// When the handler collects all N-1 REPLYs, it sets state=Released and
// closes the gate channel. Both App and DeferredFlusher wake simultaneously.
// The scheduling order between them is a local decision.
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
			case msg := <-n.transport.Mailbox():
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
		// No peers: enter CS immediately.
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

	// BUG WINDOW: between close(gate) and this line, state is Released
	// (set by the handler when it closed the gate). If DeferredFlusher
	// runs before us, it sees state=Released and grants deferred REPLYs.
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
// This is where the bug manifests: if it runs before the App goroutine
// sets state=Held, it sees state=Released and sends REPLYs to nodes
// that should have been deferred.
func (n *GateRANode) deferredFlusher() {
	<-n.gate

	n.mu.Lock()
	// BUG: check state to decide whether to send deferred REPLYs.
	// If App hasn't run yet, state is Released (not Held), so we
	// think we're not in the CS and grant all deferred requests.
	//
	// Correct behavior: deferred requests should only be sent after
	// ReleaseLock, not after gate opens. But we're checking state here
	// as a "fast path" to send them early if we're not in CS.
	if n.state == Held || n.state == Wanted {
		// In CS or still collecting — don't flush yet.
		// The deferred list stays for ReleaseLock to handle.
		n.mu.Unlock()
		return
	}
	// state == Released: we think we're done. Flush deferred REPLYs.
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
			// All REPLYs collected. Transition to Released (clearing Wanted)
			// and close the gate. Both App and DeferredFlusher will wake.
			n.state = Released
			close(n.gate)
		}

	case MsgRequest:
		// Standard RA: defer if we're in CS or have higher priority.
		shouldDefer := n.state == Held ||
			(n.state == Wanted && (n.reqClock < msg.Timestamp ||
				(n.reqClock == msg.Timestamp && n.id < msg.From)))
		if shouldDefer {
			n.deferred = append(n.deferred, msg.From)
		} else {
			ts := n.clock
			// Must unlock before Send (Send calls ExternalWait).
			n.mu.Unlock()
			n.transport.Send(msg.From, Message{
				Kind:      MsgReply,
				From:      n.id,
				Timestamp: ts,
			})
			// Re-lock so defer Unlock is valid. Use a different pattern:
			// we already unlocked, so lock again for the deferred unlock.
			n.mu.Lock()
		}
	}
}
