// Package radeferredstorm demonstrates a depth-4 G+L bug using the
// close(gate) pattern across two acquisition rounds with four contending nodes.
//
// The defect is the same as ra-gate — close(gate) wakes both an App goroutine
// and a DeferredFlusher, and under non-FIFO local scheduling the flusher can
// see state=Released and send premature replies. The difference: node A acquires
// the lock TWICE, and four nodes contend simultaneously. Each round independently
// requires one global reorder (to populate the deferred queue) plus one local
// reorder (flusher before app). Two rounds = depth 4.
//
// With 4 nodes × 3 peers = 12 REQUEST messages plus REPLYs and KV operations,
// the state space is dramatically larger than the 3-node ra-gate variant. CHESS
// needs bound ≥ 4 to explore both rounds, and the branching factor at each
// decision point is higher due to the extra messages.
//
// Finding this bug requires exactly four non-FIFO decisions:
//
//  1. Global (G1): reorder delivery so a REQUEST from B arrives at A while A
//     is collecting REPLYs in round 1 → deferred queue populated.
//  2. Local  (L1): after close(gate) in round 1, schedule DeferredFlusher
//     before App → premature REPLY to B, B enters CS alongside A.
//  3. Global (G2): in round 2, reorder delivery so a REQUEST from C or D
//     arrives at A while collecting REPLYs → deferred queue populated again.
//  4. Local  (L2): after close(gate) in round 2, schedule DeferredFlusher
//     before App → premature REPLY, another node enters CS alongside A.
//
// Neither round's bug alone requires depth 4. The depth comes from needing
// BOTH rounds to fire. CHESS at bound=2 finds the single-round violation;
// bound=4 is needed for the double violation (counter off by 2).
package radeferredstorm

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

// StormRANode implements a Ricart-Agrawala lock with the close(gate) pattern.
// Identical mechanism to GateRANode — the depth-4 behavior comes from the
// 4-node, 2-round scenario, not from a different code defect.
type StormRANode struct {
	id        string
	transport nodeTransport
	peers     []string

	mu       sync.Mutex
	state    nodeState
	clock    int
	reqClock int
	deferred []string

	repliesReceived int
	gate            chan struct{}

	stopCh chan struct{}
	done   chan struct{}
}

func NewStormRANode(addr string, transport nodeTransport, peers []string) *StormRANode {
	return &StormRANode{
		id:        addr,
		transport: transport,
		peers:     peers,
		stopCh:    make(chan struct{}),
		done:      make(chan struct{}),
	}
}

func (n *StormRANode) Start() {
	go func() {
		defer close(n.done)
		for {
			select {
			case msg, ok := <-n.transport.Mailbox():
				if !ok {
					return
				}
				n.handleMessage(msg)
			case <-n.stopCh:
				return
			}
		}
	}()
}

func (n *StormRANode) Stop() {
	close(n.stopCh)
	<-n.done
}

// AcquireLock broadcasts REQUEST, launches DeferredFlusher, blocks on gate.
// The DeferredFlusher and App goroutine both wake on close(gate), creating
// a local scheduling decision that is the L component of the G+L bug.
func (n *StormRANode) AcquireLock() {
	n.mu.Lock()
	n.state = Wanted
	n.clock++
	n.reqClock = n.clock
	n.repliesReceived = 0
	n.gate = make(chan struct{})
	n.deferred = nil
	n.mu.Unlock()

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

	go n.deferredFlusher()
	<-n.gate

	n.mu.Lock()
	n.state = Held
	n.mu.Unlock()
}

func (n *StormRANode) ReleaseLock() {
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

// deferredFlusher waits on the gate, then sends deferred REPLYs.
// If it runs before App sets state=Held, it sees state=Released
// and grants premature replies — the core protocol violation.
func (n *StormRANode) deferredFlusher() {
	<-n.gate

	n.mu.Lock()
	if n.state == Held || n.state == Wanted {
		n.mu.Unlock()
		return
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

func (n *StormRANode) handleMessage(msg Message) {
	n.mu.Lock()
	defer n.mu.Unlock()

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
			n.state = Released
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

// @stance: creative
// logViolation is available for diagnostic use but the primary detection
// mechanism is the end-state counter assertion — pure black-box.
func logViolation(node, msg string) string {
	return fmt.Sprintf("node %s: %s", node, msg)
}
