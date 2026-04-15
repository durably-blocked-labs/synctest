package raleaseexpiry

import (
	"testing/synctest"
	"time"

	"github.com/shubhaankar/synctest/orchestrator"
)

type MsgKind int

const (
	MsgRequest MsgKind = iota
	MsgReply
	MsgKVGet
	MsgKVPut
	MsgKVGetReply
	MsgKVPutReply
	MsgControl
)

type Message struct {
	Kind      MsgKind
	From      string
	Timestamp int
	Key       string
	Value     int
	Control   string
}

type nodeTransport interface {
	Send(to string, msg Message)
	Mailbox() <-chan Message
}

type delayRule struct {
	kind  MsgKind
	delay time.Duration
	used  bool
}

type OrchestratorTransport struct {
	addr    string
	outbox  chan *orchestrator.PendingOp
	mailbox chan Message
	closeCh chan struct{}
	peers   map[string]*OrchestratorTransport

	internalMailbox   chan Message
	internalKVMailbox chan Message
	internalControl   chan Message
	bridgeDone        chan struct{}

	delay *delayRule
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

func (t *OrchestratorTransport) DelayNext(kind MsgKind, delay time.Duration) {
	t.delay = &delayRule{kind: kind, delay: delay}
}

func (t *OrchestratorTransport) Addr() string                           { return t.addr }
func (t *OrchestratorTransport) Outbox() <-chan *orchestrator.PendingOp { return t.outbox }
func (t *OrchestratorTransport) Mailbox() <-chan Message                { return t.internalMailbox }
func (t *OrchestratorTransport) KVMailbox() <-chan Message              { return t.internalKVMailbox }
func (t *OrchestratorTransport) ControlMailbox() <-chan Message         { return t.internalControl }

func (t *OrchestratorTransport) StartBridge() {
	t.internalMailbox = make(chan Message, 64)
	t.internalKVMailbox = make(chan Message, 64)
	t.internalControl = make(chan Message, 64)
	t.bridgeDone = make(chan struct{})

	go func() {
		defer close(t.bridgeDone)
		defer close(t.internalMailbox)
		defer close(t.internalKVMailbox)
		defer close(t.internalControl)
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

			if t.delay != nil && !t.delay.used && t.delay.kind == msg.Kind {
				t.delay.used = true
				go t.forwardAfterDelay(msg, t.delay.delay)
				continue
			}

			t.forward(msg)
		}
	}()
}

func (t *OrchestratorTransport) forwardAfterDelay(msg Message, delay time.Duration) {
	<-time.After(delay)
	select {
	case <-t.closeCh:
		return
	default:
	}
	t.forward(msg)
}

func (t *OrchestratorTransport) forward(msg Message) {
	switch msg.Kind {
	case MsgKVGetReply, MsgKVPutReply:
		select {
		case t.internalKVMailbox <- msg:
		case <-t.closeCh:
		}
	case MsgControl:
		select {
		case t.internalControl <- msg:
		case <-t.closeCh:
		}
	default:
		select {
		case t.internalMailbox <- msg:
		case <-t.closeCh:
		}
	}
}

func (t *OrchestratorTransport) Send(to string, msg Message) {
	peer, ok := t.peers[to]
	if !ok {
		return
	}
	t.enqueueSend(peer, to, msg)
}

func (t *OrchestratorTransport) SendControl(to, label string) {
	t.Send(to, Message{
		Kind:    MsgControl,
		From:    t.addr,
		Control: label,
	})
}

func (t *OrchestratorTransport) WaitControl(label string) {
	for msg := range t.ControlMailbox() {
		if msg.Control == label {
			return
		}
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
	case MsgKVGet:
		return "KVGet"
	case MsgKVPut:
		return "KVPut"
	case MsgKVGetReply:
		return "KVGetReply"
	case MsgKVPutReply:
		return "KVPutReply"
	case MsgControl:
		return "Control"
	default:
		return "Unknown"
	}
}
