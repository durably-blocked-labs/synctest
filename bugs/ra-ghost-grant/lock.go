// Package raghostgrant demonstrates a composition bug between two individually
// reasonable ideas:
//   - Roucairol-Carvalho reply caching, which lets a node re-enter without
//     re-requesting permission from peers whose grants it still holds.
//   - Late join / dynamic membership, where a joining node announces itself via
//     JOIN and old members update their peer sets asynchronously.
//
// Node A re-enters using cached grants from B and D before it has processed C's
// JOIN, so A's current peer set omits C. Later, while A is in the critical
// section, C sends a Request. Because C is still unknown from A's perspective,
// A replies immediately instead of treating C as a competing peer. C can then
// enter concurrently, producing a lost update in the workload.
package raghostgrant

import (
	"slices"
	"sync"
)

type nodeState int

const (
	Released nodeState = iota
	Wanted
	Held
)

type GhostGrantNode struct {
	id        string
	transport nodeTransport

	mu            sync.Mutex
	peers         map[string]bool
	state         nodeState
	clock         int
	reqClock      int
	cachedReplies map[string]bool
	waitingFor    map[string]bool
	repliedBy     map[string]bool
	deferred      []string
	gate          chan struct{}

	stopCh chan struct{}
	done   chan struct{}
}

func NewGhostGrantNode(addr string, transport nodeTransport, peers []string) *GhostGrantNode {
	known := make(map[string]bool, len(peers))
	cached := make(map[string]bool, len(peers))
	for _, peer := range peers {
		known[peer] = true
		cached[peer] = false
	}
	return &GhostGrantNode{
		id:            addr,
		transport:     transport,
		peers:         known,
		cachedReplies: cached,
		stopCh:        make(chan struct{}),
		done:          make(chan struct{}),
	}
}

func (n *GhostGrantNode) Start() {
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

func (n *GhostGrantNode) Stop() {
	close(n.stopCh)
	<-n.done
}

func (n *GhostGrantNode) JoinCluster() {
	for _, peer := range n.snapshotPeers() {
		n.transport.Send(peer, Message{
			Kind: MsgJoin,
			From: n.id,
		})
	}
}

func (n *GhostGrantNode) AcquireLock() {
	n.mu.Lock()
	n.state = Wanted
	n.clock++
	n.reqClock = n.clock
	n.waitingFor = make(map[string]bool)
	n.repliedBy = make(map[string]bool)
	n.deferred = nil

	requests := make([]string, 0, len(n.peers))
	for _, peer := range n.sortedPeersLocked() {
		if n.cachedReplies[peer] {
			continue
		}
		n.waitingFor[peer] = true
		requests = append(requests, peer)
	}
	if len(requests) == 0 {
		n.state = Held
		n.mu.Unlock()
		return
	}

	n.gate = make(chan struct{})
	reqTS := n.reqClock
	gate := n.gate
	n.mu.Unlock()

	for _, peer := range requests {
		n.transport.Send(peer, Message{
			Kind:      MsgRequest,
			From:      n.id,
			Timestamp: reqTS,
		})
	}

	<-gate

	n.mu.Lock()
	n.state = Held
	n.mu.Unlock()
}

func (n *GhostGrantNode) ReleaseLock() {
	n.mu.Lock()
	n.state = Released
	deferred := append([]string(nil), n.deferred...)
	n.deferred = nil
	ts := n.clock
	n.mu.Unlock()

	for _, peer := range deferred {
		n.replyToPeer(peer, ts)
	}
}

func (n *GhostGrantNode) handleMessage(msg Message) {
	n.mu.Lock()
	if msg.Timestamp > n.clock {
		n.clock = msg.Timestamp
	}
	n.clock++

	switch msg.Kind {
	case MsgJoin:
		if msg.From != n.id && !n.peers[msg.From] {
			n.peers[msg.From] = true
			n.cachedReplies[msg.From] = false
		}
		n.mu.Unlock()
		return

	case MsgReply:
		if n.state != Wanted || !n.waitingFor[msg.From] || n.repliedBy[msg.From] {
			n.mu.Unlock()
			return
		}
		n.repliedBy[msg.From] = true
		n.cachedReplies[msg.From] = true
		if len(n.repliedBy) == len(n.waitingFor) {
			close(n.gate)
		}
		n.mu.Unlock()
		return

	case MsgRequest:
		if !n.peers[msg.From] {
			// BUG: to avoid blocking an unrecognized joiner, the node treats an
			// unknown requester as immediately grantable instead of first
			// reconciling membership. Combined with reply caching, this lets A
			// ignore C in its own quorum while still granting C permission.
			ts := n.clock
			n.mu.Unlock()
			n.replyToPeer(msg.From, ts)
			return
		}

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
		n.replyToPeer(msg.From, ts)
		return
	}

	n.mu.Unlock()
}

func (n *GhostGrantNode) replyToPeer(peer string, ts int) {
	n.mu.Lock()
	n.cachedReplies[peer] = false
	n.mu.Unlock()
	n.transport.Send(peer, Message{
		Kind:      MsgReply,
		From:      n.id,
		Timestamp: ts,
	})
}

func (n *GhostGrantNode) snapshotPeers() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.sortedPeersLocked()
}

func (n *GhostGrantNode) sortedPeersLocked() []string {
	peers := make([]string, 0, len(n.peers))
	for peer := range n.peers {
		peers = append(peers, peer)
	}
	slices.Sort(peers)
	return peers
}
