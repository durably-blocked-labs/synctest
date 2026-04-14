// Package rastalereply demonstrates a pure global-ordering bug in a
// Ricart-Agrawala lock variant that accepts stale replies from earlier rounds.
//
// The implementation deduplicates replies by sender within one AcquireLock
// call, but it does not validate whether an arriving reply belongs to the
// node's current request round. If the network duplicates and delays a reply
// from round 1 until round 2, the node can count that old reply toward its new
// quorum and enter the critical section too early.
package rastalereply

import "sync"

type nodeState int

const (
	Released nodeState = iota
	Wanted
	Held
)

type StaleReplyNode struct {
	id        string
	transport nodeTransport
	peers     []string

	mu       sync.Mutex
	state    nodeState
	clock    int
	reqClock int
	deferred []string

	repliesReceived int
	repliedBy       map[string]bool
	gate            chan struct{}

	stopCh chan struct{}
	done   chan struct{}
}

func NewStaleReplyNode(addr string, transport nodeTransport, peers []string) *StaleReplyNode {
	return &StaleReplyNode{
		id:        addr,
		transport: transport,
		peers:     peers,
		stopCh:    make(chan struct{}),
		done:      make(chan struct{}),
	}
}

func (n *StaleReplyNode) Start() {
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

func (n *StaleReplyNode) Stop() {
	close(n.stopCh)
	<-n.done
}

func (n *StaleReplyNode) AcquireLock() {
	n.mu.Lock()
	n.state = Wanted
	n.clock++
	n.reqClock = n.clock
	n.repliesReceived = 0
	n.repliedBy = make(map[string]bool, len(n.peers))
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

func (n *StaleReplyNode) ReleaseLock() {
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

func (n *StaleReplyNode) handleMessage(msg Message) {
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
		// BUG: replies are deduplicated by sender within one acquisition, but
		// they are not validated against the current request round. A delayed
		// reply from an earlier round can therefore satisfy a later AcquireLock.
		if n.repliedBy[msg.From] {
			n.mu.Unlock()
			return
		}
		n.repliedBy[msg.From] = true
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

func (n *StaleReplyNode) sendReply(to string, ts int) {
	n.transport.Send(to, Message{
		Kind:      MsgReply,
		From:      n.id,
		Timestamp: ts,
	})
}
