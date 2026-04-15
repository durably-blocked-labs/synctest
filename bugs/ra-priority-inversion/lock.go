// Package rapriorityinversion demonstrates a Ricart-Agrawala bug where
// deferred requesters are released in arrival order instead of timestamp
// priority order.
package rapriorityinversion

import "sync"

type nodeState int

const (
	Released nodeState = iota
	Wanted
	Held
)

type deferredRequest struct {
	from string
	ts   int
}

type PriorityInversionNode struct {
	id        string
	transport nodeTransport
	peers     []string

	mu       sync.Mutex
	state    nodeState
	clock    int
	reqClock int
	deferred []deferredRequest

	repliesReceived int
	gate            chan struct{}

	stopCh chan struct{}
	done   chan struct{}

	announcePeer  string
	announceLabel string

	OnReplySent func(to string)
}

func NewPriorityInversionNode(addr string, transport nodeTransport, peers []string) *PriorityInversionNode {
	return &PriorityInversionNode{
		id:        addr,
		transport: transport,
		peers:     peers,
		stopCh:    make(chan struct{}),
		done:      make(chan struct{}),
	}
}

func (n *PriorityInversionNode) Start() {
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

func (n *PriorityInversionNode) Stop() {
	close(n.stopCh)
	<-n.done
}

func (n *PriorityInversionNode) SetAcquireAnnounce(peer, label string) {
	n.mu.Lock()
	n.announcePeer = peer
	n.announceLabel = label
	n.mu.Unlock()
}

func (n *PriorityInversionNode) AcquireLock() {
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
	if n.announcePeer != "" && n.announceLabel != "" {
		n.transport.Send(n.announcePeer, Message{
			Kind:    MsgControl,
			From:    n.id,
			Control: n.announceLabel,
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

func (n *PriorityInversionNode) ReleaseLock() {
	n.mu.Lock()
	n.state = Released
	deferred := n.deferred
	n.deferred = nil
	ts := n.clock
	n.mu.Unlock()

	// BUG: deferred requesters are drained in slice order, not in timestamp
	// priority order. A lower-priority contender that arrived first can get the
	// first reply on release.
	for _, req := range deferred {
		n.sendReply(req.from, ts)
	}
}

func (n *PriorityInversionNode) handleMessage(msg Message) {
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
			n.deferred = append(n.deferred, deferredRequest{
				from: msg.From,
				ts:   msg.Timestamp,
			})
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

func (n *PriorityInversionNode) sendReply(to string, ts int) {
	n.transport.Send(to, Message{
		Kind:      MsgReply,
		From:      n.id,
		Timestamp: ts,
	})
	if n.OnReplySent != nil {
		n.OnReplySent(to)
	}
}
