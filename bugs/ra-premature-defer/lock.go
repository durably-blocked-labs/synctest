// Package raprematuredefer demonstrates a pure global-ordering bug in a
// Ricart-Agrawala lock variant.
//
// Nodes correctly defer conflicting REQUESTs while they are Wanted/Held, but
// this buggy implementation flushes deferred replies as soon as the final
// quorum REPLY arrives, before the winner has exited the critical section.
//
// Under FIFO delivery the bug is latent because the deferred queue is usually
// still empty at quorum. Under reordered delivery, a later contender REQUEST
// can arrive before the winner's final REPLY, leaving deferred non-empty. The
// buggy handler then grants that deferred requester too early, allowing two
// nodes into the critical section at once.
package raprematuredefer

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

// PrematureDeferNode implements a Ricart-Agrawala lock with a buggy quorum
// transition. Acquisition itself remains single-threaded; the bug lives
// entirely in message handling, so the failing interleaving is global-only.
type PrematureDeferNode struct {
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

	stopCh  chan struct{}
	done    chan struct{}
	startCh chan struct{}

	OnViolation func(msg string)
	OnReplySent func(to string)

	startPeer  string
	startSent  bool
	replayTo   string
	replayTS   int
	haveReplay bool
}

func NewPrematureDeferNode(addr string, transport nodeTransport, peers []string) *PrematureDeferNode {
	return &PrematureDeferNode{
		id:        addr,
		transport: transport,
		peers:     peers,
		stopCh:    make(chan struct{}),
		done:      make(chan struct{}),
		startCh:   make(chan struct{}, len(peers)+1),
	}
}

func (n *PrematureDeferNode) Start() {
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

func (n *PrematureDeferNode) Stop() {
	close(n.stopCh)
	<-n.done
}

func (n *PrematureDeferNode) SetStartPeer(peer string) {
	n.mu.Lock()
	n.startPeer = peer
	n.mu.Unlock()
}

func (n *PrematureDeferNode) WaitForStart() {
	<-n.startCh
}

func (n *PrematureDeferNode) WaitForStarts(count int) {
	for i := 0; i < count; i++ {
		<-n.startCh
	}
}

func (n *PrematureDeferNode) SendArm(to string) {
	n.mu.Lock()
	ts := n.clock
	n.mu.Unlock()
	n.transport.Send(to, Message{
		Kind:      MsgArm,
		From:      n.id,
		Timestamp: ts,
	})
}

func (n *PrematureDeferNode) SendStart(to string) {
	n.mu.Lock()
	ts := n.clock
	n.mu.Unlock()
	n.transport.Send(to, Message{
		Kind:      MsgStart,
		From:      n.id,
		Timestamp: ts,
	})
}

func (n *PrematureDeferNode) SendStaleReply() {
	n.mu.Lock()
	if !n.haveReplay {
		n.mu.Unlock()
		return
	}
	to := n.replayTo
	ts := n.replayTS
	n.mu.Unlock()

	n.transport.Send(to, Message{
		Kind:      MsgReply,
		From:      n.id,
		Timestamp: ts,
	})
}

func (n *PrematureDeferNode) AcquireLock() {
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

func (n *PrematureDeferNode) ReleaseLock() {
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

func (n *PrematureDeferNode) handleMessage(msg Message) {
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
		if n.repliesReceived != len(n.peers) {
			n.mu.Unlock()
			return
		}

		// BUG: quorum is enough to flush deferred REQUESTs, even though the node
		// is still only Wanted and has not released the critical section.
		deferred := n.deferred
		n.deferred = nil
		ts := n.clock
		if n.OnViolation != nil && len(deferred) > 0 {
			n.OnViolation(fmt.Sprintf(
				"node %s: quorum reached while Wanted, prematurely flushing %d deferred replies",
				n.id, len(deferred),
			))
		}
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

	case MsgStart:
		n.mu.Unlock()
		n.startCh <- struct{}{}
		return

	case MsgArm:
		startPeer := n.startPeer
		ts := n.clock
		n.mu.Unlock()
		if startPeer != "" {
			n.transport.Send(startPeer, Message{
				Kind:      MsgStart,
				From:      n.id,
				Timestamp: ts,
			})
		}
		return
	}

	n.mu.Unlock()
}

func (n *PrematureDeferNode) sendReply(to string, ts int) {
	n.transport.Send(to, Message{
		Kind:      MsgReply,
		From:      n.id,
		Timestamp: ts,
	})
	n.mu.Lock()
	if !n.haveReplay && to == "A" {
		n.replayTo = to
		n.replayTS = ts
		n.haveReplay = true
	}
	startPeer := n.startPeer
	sendStart := to == "A" && startPeer != "" && !n.startSent
	if sendStart {
		n.startSent = true
	}
	n.mu.Unlock()
	if sendStart {
		n.transport.Send("A", Message{
			Kind:      MsgArm,
			From:      n.id,
			Timestamp: ts,
		})
		n.transport.Send(startPeer, Message{
			Kind:      MsgStart,
			From:      n.id,
			Timestamp: ts,
		})
	}
	if n.OnReplySent != nil {
		n.OnReplySent(to)
	}
}
