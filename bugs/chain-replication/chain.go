// Package chainreplication implements a chain replication protocol with a
// failure-recovery bug modeled after the P# benchmark from the PCTCP paper
// (Ozkan et al., OOPSLA 2018).
//
// Chain replication arranges servers in a linear chain: HEAD → MID → TAIL.
// Writes enter at HEAD, propagate forward to TAIL. When TAIL commits, it
// sends BackwardAck upstream. Reads go directly to TAIL.
//
// The bug is in the failure recovery path. When MID fails, the master tells
// HEAD its new successor is TAIL and provides the last sequence number MID
// acknowledged to TAIL. HEAD must forward its pending updates (SentHistory
// entries after that sequence) to TAIL. The bug: an off-by-one in the
// forwarding loop skips one entry, creating a gap in TAIL's history.
//
// Finding this bug requires the orchestrator to deliver messages in a
// specific order: enough writes must be in-flight when the failure occurs,
// the recovery messages must interleave correctly with pending writes, and
// the skipped entry must not be "fixed" by other message paths.
package chainreplication

import (
	"sync"
)

// HistEntry is one committed update in a server's history.
type HistEntry struct {
	Seq   int
	Key   string
	Value int
}

// ChainNode is a server in the chain replication protocol.
type ChainNode struct {
	id        string
	transport nodeTransport

	mu          sync.Mutex
	predecessor string
	successor   string
	isHead      bool
	isTail      bool

	// history holds committed entries (tail commits on receipt,
	// others commit on BackwardAck).
	history []HistEntry

	// sentHistory holds entries forwarded to successor but not yet
	// acknowledged via BackwardAck.
	sentHistory []HistEntry

	nextSeq        int            // HEAD-only: monotonic sequence counter
	pendingClients map[int]string // HEAD-only: seq → client addr for write acks

	stopCh chan struct{}
	done   chan struct{}
}

func NewChainNode(id string, transport nodeTransport, predecessor, successor string) *ChainNode {
	return &ChainNode{
		id:          id,
		transport:   transport,
		predecessor: predecessor,
		successor:   successor,
		isHead:      predecessor == "",
		isTail:      successor == "",
		pendingClients: make(map[int]string),
		stopCh:         make(chan struct{}),
		done:           make(chan struct{}),
	}
}

func (n *ChainNode) Start() {
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

func (n *ChainNode) handleMessage(msg Message) {
	switch msg.Kind {
	case MsgClientWrite:
		n.handleClientWrite(msg)
	case MsgForwardUpdate:
		n.handleForwardUpdate(msg)
	case MsgBackwardAck:
		n.handleBackwardAck(msg)
	case MsgNewSuccessor:
		n.handleNewSuccessor(msg)
	case MsgNewPredecessor:
		n.handleNewPredecessor(msg)
	case MsgReadRequest:
		n.handleReadRequest(msg)
	case MsgStop:
		close(n.stopCh)
	}
}

// handleClientWrite processes a write at HEAD. Assigns a sequence number,
// stores in sentHistory, and forwards to successor. Tracks the client
// address so BackwardAck can trigger a WriteAck to the client.
func (n *ChainNode) handleClientWrite(msg Message) {
	n.mu.Lock()
	n.nextSeq++
	entry := HistEntry{Seq: n.nextSeq, Key: msg.Key, Value: msg.Value}
	n.sentHistory = append(n.sentHistory, entry)
	n.pendingClients[n.nextSeq] = msg.From
	succ := n.successor
	n.mu.Unlock()

	if succ != "" {
		n.transport.Send(succ, Message{
			Kind:  MsgForwardUpdate,
			From:  n.id,
			Seq:   entry.Seq,
			Key:   entry.Key,
			Value: entry.Value,
		})
	}
}

// handleForwardUpdate propagates a write down the chain. Non-tail nodes
// store and forward. TAIL commits immediately and sends BackwardAck.
func (n *ChainNode) handleForwardUpdate(msg Message) {
	n.mu.Lock()
	entry := HistEntry{Seq: msg.Seq, Key: msg.Key, Value: msg.Value}

	if n.isTail {
		// TAIL: commit and ack backward.
		n.history = append(n.history, entry)
		pred := n.predecessor
		n.mu.Unlock()

		if pred != "" {
			n.transport.Send(pred, Message{
				Kind: MsgBackwardAck,
				From: n.id,
				Seq:  msg.Seq,
			})
		}
		return
	}

	// Intermediate or HEAD: store in sent, forward to successor.
	n.sentHistory = append(n.sentHistory, entry)
	succ := n.successor
	n.mu.Unlock()

	if succ != "" {
		n.transport.Send(succ, Message{
			Kind:  MsgForwardUpdate,
			From:  n.id,
			Seq:   entry.Seq,
			Key:   entry.Key,
			Value: entry.Value,
		})
	}
}

// handleBackwardAck propagates a commit acknowledgment up the chain.
// Each node moves the entry from sentHistory to history.
func (n *ChainNode) handleBackwardAck(msg Message) {
	n.mu.Lock()
	for i, e := range n.sentHistory {
		if e.Seq == msg.Seq {
			n.history = append(n.history, e)
			n.sentHistory = append(n.sentHistory[:i], n.sentHistory[i+1:]...)
			break
		}
	}
	pred := n.predecessor
	// HEAD: notify client that write is committed.
	clientAddr := ""
	if n.isHead {
		if addr, ok := n.pendingClients[msg.Seq]; ok {
			clientAddr = addr
			delete(n.pendingClients, msg.Seq)
		}
	}
	n.mu.Unlock()

	if clientAddr != "" {
		n.transport.Send(clientAddr, Message{
			Kind: MsgWriteAck,
			From: n.id,
			Seq:  msg.Seq,
		})
	}
	if pred != "" {
		n.transport.Send(pred, Message{
			Kind: MsgBackwardAck,
			From: n.id,
			Seq:  msg.Seq,
		})
	}
}

// handleNewSuccessor is called during failure recovery when the master
// tells this node its successor has changed. The node must forward any
// pending entries (in sentHistory) that the new successor hasn't seen.
//
// BUG: off-by-one. The master provides LastAckSeq — the last sequence
// the dead middle node acknowledged to the new successor (TAIL). HEAD
// should forward all entries with Seq > LastAckSeq. Instead, it uses
// Seq > LastAckSeq+1, skipping exactly one entry. Under FIFO delivery
// this is harmless (the entry was already forwarded normally). Under
// message reordering where the entry was in-flight to the dead node
// and never reached TAIL, the entry is permanently lost.
func (n *ChainNode) handleNewSuccessor(msg Message) {
	n.mu.Lock()
	n.successor = msg.NewPeer
	n.isTail = (msg.NewPeer == "")

	// Forward pending entries that the new successor hasn't seen.
	var toForward []HistEntry
	for _, e := range n.sentHistory {
		if e.Seq > msg.LastAckSeq+1 { // BUG: should be > msg.LastAckSeq
			toForward = append(toForward, e)
		}
	}
	succ := n.successor
	n.mu.Unlock()

	for _, e := range toForward {
		n.transport.Send(succ, Message{
			Kind:  MsgForwardUpdate,
			From:  n.id,
			Seq:   e.Seq,
			Key:   e.Key,
			Value: e.Value,
		})
	}
}

// handleNewPredecessor is called during failure recovery to update
// this node's predecessor pointer.
func (n *ChainNode) handleNewPredecessor(msg Message) {
	n.mu.Lock()
	n.predecessor = msg.NewPeer
	n.isHead = (msg.NewPeer == "")
	n.mu.Unlock()
}

// handleReadRequest responds with the committed value for a key.
// Only TAIL should receive reads in correct chain replication.
func (n *ChainNode) handleReadRequest(msg Message) {
	n.mu.Lock()
	// Find the latest committed value for the key.
	val := 0
	for _, e := range n.history {
		if e.Key == msg.Key {
			val = e.Value
		}
	}
	n.mu.Unlock()

	n.transport.Send(msg.From, Message{
		Kind:  MsgReadResponse,
		From:  n.id,
		Key:   msg.Key,
		Value: val,
	})
}

// CommittedHistory returns a copy of the committed history for testing.
func (n *ChainNode) CommittedHistory() []HistEntry {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]HistEntry, len(n.history))
	copy(out, n.history)
	return out
}

// @stance: creative
