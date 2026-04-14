// Package raduplicaterequest demonstrates a Ricart-Agrawala bug triggered by a
// duplicated normal Request. One node records the same deferred requester twice
// and later sends two replies to that peer; the requester counts replies by raw
// counter rather than unique sender and can enter without hearing from every
// peer.
package raduplicaterequest

import "sync"

type nodeState int

const (
	Released nodeState = iota
	Wanted
	Held
)

type DuplicateRequestNode struct {
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

func NewDuplicateRequestNode(addr string, transport nodeTransport, peers []string) *DuplicateRequestNode {
	return &DuplicateRequestNode{
		id:        addr,
		transport: transport,
		peers:     peers,
		stopCh:    make(chan struct{}),
		done:      make(chan struct{}),
	}
}

func (n *DuplicateRequestNode) Start() {
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

func (n *DuplicateRequestNode) Stop() {
	close(n.stopCh)
	<-n.done
}

func (n *DuplicateRequestNode) AcquireLock() {
	n.mu.Lock()
	n.state = Wanted
	n.clock++
	n.reqClock = n.clock
	n.repliesReceived = 0
	n.gate = make(chan struct{})
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

func (n *DuplicateRequestNode) ReleaseLock() {
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

func (n *DuplicateRequestNode) handleMessage(msg Message) {
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
		// BUG: replies are counted by raw total, not by unique sender.
		n.repliesReceived++
		if n.repliesReceived == len(n.peers) {
			close(n.gate)
		}
		n.mu.Unlock()
		return

	case MsgRequest:
		shouldDefer := n.state == Held ||
			(n.state == Wanted && (n.reqClock < msg.Timestamp ||
				(n.reqClock == msg.Timestamp && n.id < msg.From)))
		if shouldDefer {
			// BUG: duplicate requests from the same peer are recorded twice.
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

func (n *DuplicateRequestNode) sendReply(to string, ts int) {
	n.transport.Send(to, Message{
		Kind:      MsgReply,
		From:      n.id,
		Timestamp: ts,
	})
}
