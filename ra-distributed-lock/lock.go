package lock

import "sync"

// nodeTransport is the interface RANode uses to send and receive messages.
// Both *Transport (direct) and *OrchestratorTransport (controlled) satisfy it.
type nodeTransport interface {
	Send(to string, msg Message)
	Mailbox() <-chan Message
}

type nodeState int

const (
	Released nodeState = iota
	Wanted
	Held
)

// RANode implements the Ricart-Agrawala distributed mutual exclusion algorithm.
type RANode struct {
	id        string
	transport nodeTransport
	peers     []string

	mu              sync.Mutex
	state           nodeState
	timestamp       int // Lamport clock
	reqTimestamp    int // timestamp sent in our pending REQUEST
	deferredReplies []string

	repliesCh       chan struct{}
	repliesNeeded   int
	repliesReceived int

	stopCh chan struct{}
	done   chan struct{}
}

func NewRANode(addr string, transport nodeTransport, peers []string) *RANode {
	return &RANode{
		id:        addr,
		transport: transport,
		peers:     peers,
		stopCh:    make(chan struct{}),
		done:      make(chan struct{}),
	}
}

func (n *RANode) Start() {
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

func (n *RANode) Stop() {
	close(n.stopCh)
	<-n.done
}

func (n *RANode) AcquireLock() {
	n.mu.Lock()
	n.state = Wanted
	n.timestamp++
	ts := n.timestamp
	n.reqTimestamp = ts
	repliesCh := make(chan struct{})
	n.repliesCh = repliesCh
	n.repliesNeeded = len(n.peers)
	n.repliesReceived = 0
	n.mu.Unlock()

	for _, peer := range n.peers {
		n.transport.Send(peer, Message{
			Kind:      MsgRequest,
			From:      n.id,
			Timestamp: ts,
		})
	}

	if len(n.peers) > 0 {
		<-repliesCh
	}

	n.mu.Lock()
	n.repliesCh = nil
	n.state = Held
	n.mu.Unlock()
}

func (n *RANode) ReleaseLock() {
	n.mu.Lock()
	n.state = Released
	deferred := n.deferredReplies
	n.deferredReplies = nil
	n.mu.Unlock()

	n.mu.Lock()
	ts := n.timestamp
	n.mu.Unlock()
	for _, peer := range deferred {
		n.transport.Send(peer, Message{
			Kind:      MsgReply,
			From:      n.id,
			Timestamp: ts,
		})
	}
}

func (n *RANode) handleMessage(msg Message) {
	n.mu.Lock()

	switch msg.Kind {
	case MsgReply:
		// Update Lamport clock.
		// MUTATION: clock update moved here (was before the switch, applied to all
		// message types). Now only REPLYs advance n.timestamp. Under FIFO delivery
		// no REPLY arrives before its corresponding REQUEST, so n.timestamp equals
		// n.reqTimestamp when a REQUEST is processed — the bug in shouldDefer is
		// latent. Under reordered delivery a REPLY can precede a REQUEST from the
		// same sender, bumping n.timestamp above n.reqTimestamp before the REQUEST
		// arrives, which triggers the shouldDefer bug below.
		if msg.Timestamp > n.timestamp {
			n.timestamp = msg.Timestamp
		}
		n.timestamp++

		if n.state != Wanted {
			n.mu.Unlock()
			return
		}
		n.repliesReceived++
		if n.repliesReceived == n.repliesNeeded {
			ch := n.repliesCh
			n.mu.Unlock()
			close(ch)
			return
		}
		n.mu.Unlock()

	case MsgRequest:
		// MUTATION: uses n.timestamp instead of n.reqTimestamp. Because the clock
		// update was moved to MsgReply, n.timestamp here equals n.reqTimestamp
		// under FIFO (no REPLY received yet). Under reordering n.timestamp may have
		// been bumped by a prior REPLY, making it > msg.Timestamp and skipping the
		// deferral that should protect mutual exclusion.
		shouldDefer := n.state == Held ||
			(n.state == Wanted && (n.timestamp < msg.Timestamp ||
				(n.timestamp == msg.Timestamp && n.id < msg.From)))
		if shouldDefer {
			n.deferredReplies = append(n.deferredReplies, msg.From)
			n.mu.Unlock()
		} else {
			ts := n.timestamp
			n.mu.Unlock()
			n.transport.Send(msg.From, Message{
				Kind:      MsgReply,
				From:      n.id,
				Timestamp: ts,
			})
		}
	}
}
