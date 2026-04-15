package ragate

import (
	"sync"
)

type nodeState int

const (
	Released nodeState = iota
	Wanted
	Held
)

type GateRANode struct {
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

func NewGateRANode(addr string, transport nodeTransport, peers []string) *GateRANode {
	return &GateRANode{
		id:        addr,
		transport: transport,
		peers:     peers,
		stopCh:    make(chan struct{}),
		done:      make(chan struct{}),
	}
}

func (n *GateRANode) Start() {
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

func (n *GateRANode) Stop() {
	close(n.stopCh)
	<-n.done
}

func (n *GateRANode) AcquireLock() {
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

func (n *GateRANode) deferredFlusher() {
	<-n.gate

	// state is Wanted here (not Released), so this guard fires and we return.
	// Deferred replies are sent only from ReleaseLock, preserving mutual exclusion.
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

func (n *GateRANode) handleMessage(msg Message) {
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
			// Fix: do NOT set state=Released here.
			// State remains Wanted until AcquireLock sets it to Held.
			close(n.gate)
		}

	case MsgRequest:
		shouldDefer := n.state == Held ||
			(n.state == Wanted && (n.clock < msg.Timestamp ||
				(n.clock == msg.Timestamp && n.id < msg.From)))
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