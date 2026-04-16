package quorumreadrepair

import (
	"testing/synctest"

	"github.com/shubhaankar/synctest/orchestrator"
)

type MsgKind int

const (
	MsgPut MsgKind = iota
	MsgPutAck
	MsgGet
	MsgGetResp
	MsgRepair
	MsgRepairAck
	MsgControl
)

type Clock map[string]int

type VersionedValue struct {
	Value string
	Clock Clock
}

type Message struct {
	Kind      MsgKind
	From      string
	RequestID string
	Key       string
	Values    []VersionedValue
	Control   string
}

type OrchestratorTransport struct {
	addr    string
	outbox  chan *orchestrator.PendingOp
	mailbox chan Message
	closeCh chan struct{}
	peers   map[string]*OrchestratorTransport

	internalMailbox chan Message
	internalControl chan Message
	bridgeDone      chan struct{}
}

func NewOrchestratorTransport(addr string) *OrchestratorTransport {
	return &OrchestratorTransport{
		addr:    addr,
		outbox:  make(chan *orchestrator.PendingOp, 128),
		mailbox: make(chan Message, 128),
		closeCh: make(chan struct{}),
		peers:   make(map[string]*OrchestratorTransport),
	}
}

func (t *OrchestratorTransport) Connect(peer *OrchestratorTransport) {
	t.peers[peer.addr] = peer
}

func (t *OrchestratorTransport) Addr() string { return t.addr }

func (t *OrchestratorTransport) Outbox() <-chan *orchestrator.PendingOp { return t.outbox }

func (t *OrchestratorTransport) Mailbox() <-chan Message { return t.internalMailbox }

func (t *OrchestratorTransport) ControlMailbox() <-chan Message { return t.internalControl }

func (t *OrchestratorTransport) StartBridge() {
	t.internalMailbox = make(chan Message, 128)
	t.internalControl = make(chan Message, 32)
	t.bridgeDone = make(chan struct{})

	go func() {
		defer close(t.bridgeDone)
		defer close(t.internalMailbox)
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
			switch msg.Kind {
			case MsgControl:
				select {
				case t.internalControl <- msg:
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
	if msg.From == "" {
		msg.From = t.addr
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
				case peer.mailbox <- cloneMessage(msg):
				case <-peer.closeCh:
				}
			},
		}:
		case <-t.closeCh:
		}
	})
}

func (t *OrchestratorTransport) SendControl(to, control string) {
	t.Send(to, Message{Kind: MsgControl, Control: control})
}

func (t *OrchestratorTransport) WaitControl(control string) {
	for msg := range t.ControlMailbox() {
		if msg.Control == control {
			return
		}
	}
}

func (t *OrchestratorTransport) Shutdown() {
	select {
	case <-t.closeCh:
	default:
		close(t.closeCh)
	}
}

func (t *OrchestratorTransport) Close() {
	t.Shutdown()
	if t.bridgeDone != nil {
		<-t.bridgeDone
	}
}

func cloneMessage(msg Message) Message {
	msg.Values = cloneValues(msg.Values)
	return msg
}

func cloneValues(values []VersionedValue) []VersionedValue {
	if values == nil {
		return nil
	}
	cloned := make([]VersionedValue, len(values))
	for i, value := range values {
		cloned[i] = VersionedValue{
			Value: value.Value,
			Clock: cloneClock(value.Clock),
		}
	}
	return cloned
}

func cloneClock(clock Clock) Clock {
	if clock == nil {
		return nil
	}
	cloned := make(Clock, len(clock))
	for k, v := range clock {
		cloned[k] = v
	}
	return cloned
}

func msgKindName(kind MsgKind) string {
	switch kind {
	case MsgPut:
		return "Put"
	case MsgPutAck:
		return "PutAck"
	case MsgGet:
		return "Get"
	case MsgGetResp:
		return "GetResp"
	case MsgRepair:
		return "Repair"
	case MsgRepairAck:
		return "RepairAck"
	case MsgControl:
		return "Control"
	default:
		return "Unknown"
	}
}
