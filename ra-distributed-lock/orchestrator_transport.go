package lock

import (
	"testing/synctest"

	"github.com/shubhaankar/synctest/orchestratorv2"
)

// OrchestratorTransport routes RA lock messages through the orchestratorv2
// global orchestrator, enabling controlled message delivery ordering for
// deterministic distributed systems testing.
//
// Usage inside a bubble test function:
//
//	tr.StartBridge()                      // creates internal mailbox, starts bridge
//	node := NewRANode(addr, tr, peers)
//	node.Start()
//	// ... acquire/release lock ...
//	node.Stop()
//	tr.Close()                            // shuts down bridge goroutine
//
// The bridge goroutine uses ExternalWait to block on the external mailbox.
// When the orchestrator delivers a message (via Execute), it writes to the
// external mailbox; the bridge wakes, re-enters the bubble, and forwards
// the message to the internal mailbox that RANode reads from.
type OrchestratorTransport struct {
	addr    string
	outbox  chan *orchestratorv2.PendingOp // buffered; to orchestrator
	mailbox chan Message                   // buffered external; orchestrator writes via Execute
	closeCh chan struct{}
	peers   map[string]*OrchestratorTransport

	// Created inside the bubble by StartBridge; RANode receives from this.
	internalMailbox chan Message
	bridgeDone      chan struct{}
}

// NewOrchestratorTransport creates a transport for the given address.
// internalMailbox is nil until StartBridge is called inside the bubble.
func NewOrchestratorTransport(addr string) *OrchestratorTransport {
	return &OrchestratorTransport{
		addr:    addr,
		outbox:  make(chan *orchestratorv2.PendingOp, 64),
		mailbox: make(chan Message, 64),
		closeCh: make(chan struct{}),
		peers:   make(map[string]*OrchestratorTransport),
	}
}

// Connect registers a peer transport. Must be called before StartBridge.
func (t *OrchestratorTransport) Connect(peer *OrchestratorTransport) {
	t.peers[peer.addr] = peer
}

// Addr implements orchestratorv2.NodeTransport.
func (t *OrchestratorTransport) Addr() string { return t.addr }

// Outbox implements orchestratorv2.NodeTransport.
func (t *OrchestratorTransport) Outbox() <-chan *orchestratorv2.PendingOp { return t.outbox }

// Mailbox implements nodeTransport. Returns the internal (bubble) channel.
// Panics if called before StartBridge.
func (t *OrchestratorTransport) Mailbox() <-chan Message { return t.internalMailbox }

// StartBridge creates the internal mailbox channel inside the bubble and
// launches the bridge goroutine that forwards orchestrator-delivered messages
// from the external mailbox to the internal one. Must be called inside the
// bubble before NewRANode and RANode.Start().
func (t *OrchestratorTransport) StartBridge() {
	// Created inside the bubble so RANode's goroutine blocking on it is durable.
	t.internalMailbox = make(chan Message, 64)
	t.bridgeDone = make(chan struct{})

	go func() {
		defer close(t.bridgeDone)
		for {
			var msg Message
			var closed bool
			synctest.ExternalWait(func() {
				select {
				case msg = <-t.mailbox:
				case <-t.closeCh:
					closed = true
				}
			})
			if closed {
				return
			}
			t.internalMailbox <- msg
		}
	}()
}

// Send implements nodeTransport. Routes the message through the orchestrator
// by submitting a PendingOp to the outbox. The orchestrator calls Execute to
// write the message to the target's external mailbox when it decides to deliver it.
//
// Uses ExternalWait so the bubble can go idle while waiting for the outbox send
// (which completes immediately given the buffered outbox).
func (t *OrchestratorTransport) Send(to string, msg Message) {
	peer, ok := t.peers[to]
	if !ok {
		return
	}
	synctest.ExternalWait(func() {
		select {
		case t.outbox <- &orchestratorv2.PendingOp{
			Dir:  orchestratorv2.OpSend,
			From: t.addr,
			To:   to,
			Type: msgKindName(msg.Kind),
			Execute: func() {
				select {
				case peer.mailbox <- msg:
				case <-peer.closeCh:
				}
			},
		}:
		case <-t.closeCh:
		}
	})
}

// Close shuts down the bridge goroutine and waits for it to exit.
// After the bridge exits, internalMailbox is closed so any goroutine
// ranging over Mailbox() (e.g. a dispatcher) terminates cleanly.
// Must be called inside the bubble after RANode.Stop().
func (t *OrchestratorTransport) Close() {
	select {
	case <-t.closeCh:
		// already closed; bridgeDone and internalMailbox already closed
		<-t.bridgeDone
		return
	default:
		close(t.closeCh)
	}
	<-t.bridgeDone
	close(t.internalMailbox)
}

func msgKindName(k MsgKind) string {
	switch k {
	case MsgRequest:
		return "Request"
	case MsgReply:
		return "Reply"
	case MsgKVGet:
		return "KVGet"
	case MsgKVPut:
		return "KVPut"
	case MsgKVDone:
		return "KVDone"
	case MsgKVReply:
		return "KVReply"
	default:
		return "Unknown"
	}
}
