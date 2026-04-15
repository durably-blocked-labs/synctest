package raleaseexpiry

import (
	"sync"
	"time"
)

type nodeState int

const (
	Released nodeState = iota
	Wanted
	Held
)

const leaseTTL = 15 * time.Millisecond

type LeaseExpiryNode struct {
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
	leaseEpoch      int

	stopCh chan struct{}
	done   chan struct{}
}

func NewLeaseExpiryNode(addr string, transport nodeTransport, peers []string) *LeaseExpiryNode {
	return &LeaseExpiryNode{
		id:        addr,
		transport: transport,
		peers:     peers,
		stopCh:    make(chan struct{}),
		done:      make(chan struct{}),
	}
}

func (n *LeaseExpiryNode) Start() {
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

func (n *LeaseExpiryNode) Stop() {
	close(n.stopCh)
	<-n.done
}

func (n *LeaseExpiryNode) AcquireLock() {
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
		epoch := n.leaseEpoch + 1
		n.leaseEpoch = epoch
		n.mu.Unlock()
		go n.leaseWatchdog(epoch)
		return
	}

	<-n.gate

	n.mu.Lock()
	n.state = Held
	epoch := n.leaseEpoch + 1
	n.leaseEpoch = epoch
	n.mu.Unlock()
	go n.leaseWatchdog(epoch)
}

func (n *LeaseExpiryNode) ReleaseLock() {
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

func (n *LeaseExpiryNode) leaseWatchdog(epoch int) {
	<-time.After(leaseTTL)

	n.mu.Lock()
	if n.state != Held || n.leaseEpoch != epoch {
		n.mu.Unlock()
		return
	}

	n.state = Released
	deferred := n.deferred
	n.deferred = nil
	ts := n.clock
	n.mu.Unlock()

	for _, peer := range deferred {
		n.sendReply(peer, ts)
	}
}

func (n *LeaseExpiryNode) handleMessage(msg Message) {
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

func (n *LeaseExpiryNode) sendReply(to string, ts int) {
	n.transport.Send(to, Message{
		Kind:      MsgReply,
		From:      n.id,
		Timestamp: ts,
	})
}
