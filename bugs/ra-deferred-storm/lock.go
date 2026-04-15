// Package radeferredstorm demonstrates a Ricart-Agrawala bug in a
// Roucairol-Carvalho style lock that tries to re-enter eagerly while draining
// deferred requests.
//
// The node caches permissions it still holds from peers. On exit it drains the
// deferred queue in sorted order; after the first drained reply it launches
// `go EnterCS()` for the next round, but it does so before the drain completes
// and before it clears `inCS`.
//
// Under FIFO local scheduling the eager goroutine typically runs after the full
// drain, sees all invalidated cached permissions, and re-requests them. Under a
// non-FIFO local decision it can run mid-drain, snapshot a partially-invalidated
// cache, and request too small a quorum. With global message reordering, old
// replies and new requests cross and another node can enter while the eager
// round is already collecting permissions.
package radeferredstorm

import (
	"sort"
	"sync"
)

type requestWaiter struct {
	count int
	ch    chan struct{}
}

type nodeState int

const (
	Released nodeState = iota
	Wanted
	Held
)

type DeferredStormNode struct {
	id        string
	transport nodeTransport
	peers     []string

	mu        sync.Mutex
	state     nodeState
	inCS      bool
	clock     int
	reqClock  int
	deferred  []string
	grantedBy map[string]bool
	waiting   map[string]bool
	gate      chan struct{}

	stopCh chan struct{}
	done   chan struct{}

	eagerReady     chan struct{}
	startCh        chan struct{}
	releaseCh      chan struct{}
	releasePeer    string
	requestCounts  map[string]int
	requestWaiters map[string][]requestWaiter
}

func NewDeferredStormNode(addr string, transport nodeTransport, peers []string) *DeferredStormNode {
	return &DeferredStormNode{
		id:             addr,
		transport:      transport,
		peers:          peers,
		stopCh:         make(chan struct{}),
		done:           make(chan struct{}),
		grantedBy:      make(map[string]bool, len(peers)),
		startCh:        make(chan struct{}, len(peers)+1),
		releaseCh:      make(chan struct{}, len(peers)+1),
		requestCounts:  make(map[string]int, len(peers)),
		requestWaiters: make(map[string][]requestWaiter, len(peers)),
	}
}

func (n *DeferredStormNode) Start() {
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

func (n *DeferredStormNode) Stop() {
	close(n.stopCh)
	<-n.done
}

func (n *DeferredStormNode) EnterCS() {
	n.enterCS(nil)
}

func (n *DeferredStormNode) WaitForEagerEntry() {
	n.mu.Lock()
	ready := n.eagerReady
	n.mu.Unlock()
	if ready != nil {
		<-ready
	}
}

func (n *DeferredStormNode) SetReleasePeer(peer string) {
	n.mu.Lock()
	n.releasePeer = peer
	n.mu.Unlock()
}

func (n *DeferredStormNode) WaitForStart() {
	<-n.startCh
}

func (n *DeferredStormNode) WaitForRelease() {
	<-n.releaseCh
}

func (n *DeferredStormNode) WaitForRequest(peer string, count int) {
	n.mu.Lock()
	if n.requestCounts[peer] >= count {
		n.mu.Unlock()
		return
	}
	ch := make(chan struct{})
	n.requestWaiters[peer] = append(n.requestWaiters[peer], requestWaiter{
		count: count,
		ch:    ch,
	})
	n.mu.Unlock()
	<-ch
}

func (n *DeferredStormNode) SendStart(to string) {
	n.sendControl(to, MsgStart)
}

func (n *DeferredStormNode) SendArm(to string) {
	n.sendControl(to, MsgArm)
}

func (n *DeferredStormNode) enterCS(ready chan struct{}) {
	n.mu.Lock()
	n.clock++
	n.reqClock = n.clock
	n.state = Wanted
	n.waiting = make(map[string]bool, len(n.peers))
	n.gate = make(chan struct{})

	missing := make([]string, 0, len(n.peers))
	for _, peer := range n.peers {
		if n.grantedBy[peer] {
			continue
		}
		n.waiting[peer] = true
		missing = append(missing, peer)
	}
	if len(missing) == 0 {
		n.waiting = nil
		n.gate = nil
		n.state = Held
		n.inCS = true
		n.mu.Unlock()
		if ready != nil {
			close(ready)
		}
		return
	}
	reqTS := n.reqClock
	gate := n.gate
	n.mu.Unlock()

	for _, peer := range missing {
		n.transport.Send(peer, Message{
			Kind:      MsgRequest,
			From:      n.id,
			Timestamp: reqTS,
		})
	}

	<-gate

	n.mu.Lock()
	n.waiting = nil
	n.gate = nil
	n.state = Held
	n.inCS = true
	n.mu.Unlock()
	if ready != nil {
		close(ready)
	}
}

func (n *DeferredStormNode) ExitCS(reenter bool) {
	n.mu.Lock()
	deferred := append([]string(nil), n.deferred...)
	n.deferred = nil
	sort.Strings(deferred)
	if reenter {
		n.eagerReady = make(chan struct{})
	} else {
		n.eagerReady = nil
	}
	ready := n.eagerReady
	n.mu.Unlock()

	if !reenter || len(deferred) == 0 {
		n.finishDrain(deferred)
		return
	}

	n.sendReply(deferred[0])
	rest := deferred[1:]
	if len(rest) > 0 {
		go n.finishDrain(rest)
	} else {
		go n.finishDrain(nil)
	}
	go n.enterCS(ready)
}

func (n *DeferredStormNode) finishDrain(deferred []string) {
	for _, peer := range deferred {
		n.sendReply(peer)
	}
	n.mu.Lock()
	n.inCS = false
	if n.state == Held {
		n.state = Released
	}
	n.mu.Unlock()
}

func (n *DeferredStormNode) handleMessage(msg Message) {
	n.mu.Lock()
	if msg.Timestamp > n.clock {
		n.clock = msg.Timestamp
	}
	n.clock++

	switch msg.Kind {
	case MsgReply:
		if n.waiting == nil || !n.waiting[msg.From] {
			n.mu.Unlock()
			return
		}
		delete(n.waiting, msg.From)
		n.grantedBy[msg.From] = true
		gate := n.gate
		ready := len(n.waiting) == 0 && gate != nil
		if ready {
			n.gate = nil
		}
		n.mu.Unlock()
		if ready {
			close(gate)
		}
		return

	case MsgRequest:
		n.noteRequestLocked(msg.From)
		shouldDefer := n.inCS ||
			(n.state == Wanted && (n.reqClock < msg.Timestamp ||
				(n.reqClock == msg.Timestamp && n.id < msg.From)))
		if shouldDefer {
			n.deferPeerLocked(msg.From)
			n.mu.Unlock()
			return
		}
		n.mu.Unlock()
		n.sendReply(msg.From)
		return

	case MsgStart:
		startCh := n.startCh
		n.mu.Unlock()
		startCh <- struct{}{}
		return

	case MsgRelease:
		releaseCh := n.releaseCh
		n.mu.Unlock()
		releaseCh <- struct{}{}
		return

	case MsgArm:
		releasePeer := n.releasePeer
		ts := n.clock
		n.mu.Unlock()
		if releasePeer != "" {
			n.transport.Send(releasePeer, Message{
				Kind:      MsgRelease,
				From:      n.id,
				Timestamp: ts,
			})
		}
		return
	}

	n.mu.Unlock()
}

func (n *DeferredStormNode) deferPeerLocked(peer string) {
	for _, existing := range n.deferred {
		if existing == peer {
			return
		}
	}
	n.deferred = append(n.deferred, peer)
}

func (n *DeferredStormNode) sendReply(to string) {
	n.mu.Lock()
	ts := n.clock
	n.grantedBy[to] = false
	n.mu.Unlock()

	n.transport.Send(to, Message{
		Kind:      MsgReply,
		From:      n.id,
		Timestamp: ts,
	})
}

func (n *DeferredStormNode) sendControl(to string, kind MsgKind) {
	n.mu.Lock()
	ts := n.clock
	n.mu.Unlock()

	n.transport.Send(to, Message{
		Kind:      kind,
		From:      n.id,
		Timestamp: ts,
	})
}

func (n *DeferredStormNode) noteRequestLocked(peer string) {
	n.requestCounts[peer]++
	got := n.requestCounts[peer]
	waiters := n.requestWaiters[peer]
	if len(waiters) == 0 {
		return
	}

	keep := waiters[:0]
	for _, waiter := range waiters {
		if got >= waiter.count {
			close(waiter.ch)
			continue
		}
		keep = append(keep, waiter)
	}
	if len(keep) == 0 {
		delete(n.requestWaiters, peer)
		return
	}
	n.requestWaiters[peer] = keep
}
