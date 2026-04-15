package raprematuredefer

import (
	"testing/synctest"

	"github.com/shubhaankar/synctest/orchestrator"
)

// MsgKind classifies RA protocol messages.
type MsgKind int

const (
	MsgRequest MsgKind = iota
	MsgReply
	MsgStart
	MsgArm
	MsgKVGet
	MsgKVPut
	MsgKVGetReply
	MsgKVPutReply
)

// Message is the wire format for RA protocol and KV messages.
type Message struct {
	Kind      MsgKind
	From      string
	Timestamp int
	Key       string // KV ops
	Value     int    // KV ops
}

type nodeTransport interface {
	Send(to string, msg Message)
	Mailbox() <-chan Message
}

// OrchestratorTransport routes messages through the global orchestrator.
type OrchestratorTransport struct {
	addr    string
	outbox  chan *orchestrator.PendingOp
	mailbox chan Message
	closeCh chan struct{}
	peers   map[string]*OrchestratorTransport

	internalMailbox   chan Message
	internalKVMailbox chan Message
	bridgeDone        chan struct{}
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
func (t *OrchestratorTransport) KVMailbox() <-chan Message              { return t.internalKVMailbox }

func (t *OrchestratorTransport) StartBridge() {
	t.internalMailbox = make(chan Message, 64)
	t.internalKVMailbox = make(chan Message, 64)
	t.bridgeDone = make(chan struct{})

	go func() {
		defer close(t.bridgeDone)
		defer close(t.internalMailbox)
		defer close(t.internalKVMailbox)
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
			switch msg.Kind {
			case MsgKVGetReply, MsgKVPutReply:
				select {
				case t.internalKVMailbox <- msg:
				case <-t.closeCh:
					return
				}
			default:
				select {
				case t.internalMailbox <- msg:
				case <-t.closeCh:
					return
				}
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

func (t *OrchestratorTransport) Shutdown() {
	select {
	case <-t.closeCh:
		return
	default:
		close(t.closeCh)
	}
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
}

func msgKindName(k MsgKind) string {
	switch k {
	case MsgRequest:
		return "Request"
	case MsgReply:
		return "Reply"
	case MsgStart:
		return "Start"
	case MsgArm:
		return "Arm"
	case MsgKVGet:
		return "KVGet"
	case MsgKVPut:
		return "KVPut"
	case MsgKVGetReply:
		return "KVGetReply"
	case MsgKVPutReply:
		return "KVPutReply"
	default:
		return "Unknown"
	}
}
