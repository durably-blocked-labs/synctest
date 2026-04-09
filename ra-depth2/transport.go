package radepth2

import (
	"testing/synctest"

	"github.com/shubhaankar/synctest/orchestrator"
)

// MsgKind classifies RA lock protocol messages.
type MsgKind int

const (
	MsgRequest MsgKind = iota
	MsgReply
	MsgKVGet
	MsgKVPut
	MsgKVDone
	MsgKVReply
)

// Message is the wire format for RA lock protocol messages.
type Message struct {
	Kind      MsgKind
	From      string
	Timestamp int
	Key       string // KV: key for Get/Put
	Value     int    // KV: value for Put/Reply
}

// nodeTransport is the interface RANode uses to send and receive messages.
type nodeTransport interface {
	Send(to string, msg Message)
	Mailbox() <-chan Message
}

// OrchestratorTransport routes messages through the orchestrator
// global orchestrator, enabling controlled delivery ordering.
type OrchestratorTransport struct {
	addr    string
	outbox  chan *orchestrator.PendingOp
	mailbox chan Message // external; orchestrator writes via Execute
	closeCh chan struct{}
	peers   map[string]*OrchestratorTransport

	internalMailbox chan Message
	bridgeDone      chan struct{}
}

func NewOrchestratorTransport(addr string) *OrchestratorTransport {
	return &OrchestratorTransport{
		addr:    addr,
		outbox:  make(chan *orchestrator.PendingOp, 64),
		mailbox: make(chan Message, 64),
		closeCh: make(chan struct{}),
		peers:   make(map[string]*OrchestratorTransport),
	}
}

func (t *OrchestratorTransport) Connect(peer *OrchestratorTransport) {
	t.peers[peer.addr] = peer
}

func (t *OrchestratorTransport) Addr() string                           { return t.addr }
func (t *OrchestratorTransport) Outbox() <-chan *orchestrator.PendingOp { return t.outbox }
func (t *OrchestratorTransport) Mailbox() <-chan Message                { return t.internalMailbox }

func (t *OrchestratorTransport) StartBridge() {
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
			select {
			case t.internalMailbox <- msg:
			case <-t.closeCh:
				return
			}
		}
	}()
}

func (t *OrchestratorTransport) Send(to string, msg Message) {
	peer, ok := t.peers[to]
	if !ok {
		return
	}
	synctest.ExternalWait(func() {
		select {
		case t.outbox <- &orchestrator.PendingOp{
			Dir:  orchestrator.OpSend,
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

func (t *OrchestratorTransport) Close() {
	select {
	case <-t.closeCh:
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
