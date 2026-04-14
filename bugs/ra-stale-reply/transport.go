package rastalereply

import (
	"testing/synctest"

	"github.com/shubhaankar/synctest/orchestrator"
)

type MsgKind int

const (
	MsgRequest MsgKind = iota
	MsgReply
)

type Message struct {
	Kind      MsgKind
	From      string
	Timestamp int
}

type nodeTransport interface {
	Send(to string, msg Message)
	Mailbox() <-chan Message
}

type duplicateRule struct {
	to   string
	kind MsgKind
	used bool
}

type OrchestratorTransport struct {
	addr    string
	outbox  chan *orchestrator.PendingOp
	mailbox chan Message
	closeCh chan struct{}
	peers   map[string]*OrchestratorTransport

	internalMailbox chan Message
	bridgeDone      chan struct{}

	duplicate *duplicateRule
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

func (t *OrchestratorTransport) DuplicateNext(to string, kind MsgKind) {
	t.duplicate = &duplicateRule{to: to, kind: kind}
}

func (t *OrchestratorTransport) Addr() string                           { return t.addr }
func (t *OrchestratorTransport) Outbox() <-chan *orchestrator.PendingOp { return t.outbox }
func (t *OrchestratorTransport) Mailbox() <-chan Message                { return t.internalMailbox }

func (t *OrchestratorTransport) StartBridge() {
	t.internalMailbox = make(chan Message, 64)
	t.bridgeDone = make(chan struct{})

	go func() {
		defer close(t.bridgeDone)
		defer close(t.internalMailbox)
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
	t.enqueueSend(peer, to, msg)

	if t.duplicate != nil && !t.duplicate.used && t.duplicate.to == to && t.duplicate.kind == msg.Kind {
		t.duplicate.used = true
		t.enqueueSend(peer, to, msg)
	}
}

func (t *OrchestratorTransport) enqueueSend(peer *OrchestratorTransport, to string, msg Message) {
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
	default:
		return "Unknown"
	}
}
