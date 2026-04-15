// Package raquorumcascade demonstrates a pure global-ordering bug in a
// Ricart-Agrawala lock variant.
//
// The node follows the ordinary RA rules for Request and Reply handling, but
// it contains one bug: when the final quorum Reply arrives, it closes the
// acquire gate and flushes deferred Requesters immediately, before the node
// has actually transitioned out of Wanted.
package raquorumcascade

import "sync"

type nodeState int

const (
	Released nodeState = iota
	Wanted
	Held
)

// QuorumCascadeNode implements Ricart-Agrawala with a premature-defer bug.
type QuorumCascadeNode struct {
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

	OnReplySent     func(to string)
	OnReplyReceived func(from string, count int)

	stopCh chan struct{}
	done   chan struct{}
}

func NewQuorumCascadeNode(addr string, transport nodeTransport, peers []string) *QuorumCascadeNode {
	return &QuorumCascadeNode{
		id:        addr,
		transport: transport,
		peers:     peers,
		stopCh:    make(chan struct{}),
		done:      make(chan struct{}),
	}
}

func (n *QuorumCascadeNode) Start() {
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

func (n *QuorumCascadeNode) Stop() {
	close(n.stopCh)
	<-n.done
}

func (n *QuorumCascadeNode) AcquireLock() {
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

	<-n.gate

	n.mu.Lock()
	n.state = Held
	n.mu.Unlock()
}

func (n *QuorumCascadeNode) ReleaseLock() {
	n.mu.Lock()
	n.state = Released
	deferred := n.deferred
	n.deferred = nil
	ts := n.clock
	n.mu.Unlock()

	for _, peer := range deferred {
		n.sendReply(peer, ts)
	}
}

func (n *QuorumCascadeNode) handleMessage(msg Message) {
	n.mu.Lock()
	if msg.Timestamp > n.clock {
		n.clock = msg.Timestamp
	}
	n.clock++

	switch msg.Kind {
	case MsgReply:
		if n.state != Wanted {
			n.mu.Unlock()
			return
		}

		n.repliesReceived++
		if n.OnReplyReceived != nil {
			n.OnReplyReceived(msg.From, n.repliesReceived)
		}
		if n.repliesReceived != len(n.peers) {
			n.mu.Unlock()
			return
		}

		// BUG: quorum is enough to flush deferred REQUESTs, even though the
		// node is still only Wanted and has not released the critical section.
		deferred := n.deferred
		n.deferred = nil
		ts := n.clock
		close(n.gate)
		n.mu.Unlock()

		for _, peer := range deferred {
			n.sendReply(peer, ts)
		}
		return

	case MsgRequest:
		shouldDefer := n.state == Held ||
			(n.state == Wanted && (n.reqClock < msg.Timestamp ||
				(n.reqClock == msg.Timestamp && n.id < msg.From)))
		if shouldDefer {
			n.deferred = append(n.deferred, msg.From)
			n.mu.Unlock()
			return
		}

		ts := n.clock
		n.mu.Unlock()
		n.sendReply(msg.From, ts)
		return
	}

	n.mu.Unlock()
}

func (n *QuorumCascadeNode) sendReply(to string, ts int) {
	n.transport.Send(to, Message{
		Kind:      MsgReply,
		From:      n.id,
		Timestamp: ts,
	})
	if n.OnReplySent != nil {
		n.OnReplySent(to)
	}
}
