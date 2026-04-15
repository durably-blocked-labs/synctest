package chainreplication

import (
	"testing/synctest"

	"github.com/shubhaankar/synctest/orchestrator"
)

type MsgKind int

const (
	MsgClientWrite   MsgKind = iota // client → head
	MsgForwardUpdate                // chain forward propagation
	MsgBackwardAck                  // chain backward commit
	MsgNewSuccessor                 // master → server (failure recovery)
	MsgNewPredecessor               // master → server (failure recovery)
	MsgWriteAck                     // head → client (write committed)
	MsgReadRequest                  // client → tail
	MsgReadResponse                 // tail → client
	MsgStop                         // master → server (graceful shutdown)
)

type Message struct {
	Kind       MsgKind
	From       string
	Seq        int
	Key        string
	Value      int
	NewPeer    string // for NewSuccessor/NewPredecessor
	LastAckSeq int    // for NewSuccessor: last seq acked by dead node to new successor
}

type nodeTransport interface {
	Send(to string, msg Message)
	Mailbox() <-chan Message
}

type OrchestratorTransport struct {
	addr    string
	outbox  chan *orchestrator.PendingOp
	mailbox chan Message
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
	case MsgClientWrite:
		return "ClientWrite"
	case MsgForwardUpdate:
		return "FwdUpdate"
	case MsgBackwardAck:
		return "BckAck"
	case MsgNewSuccessor:
		return "NewSucc"
	case MsgNewPredecessor:
		return "NewPred"
	case MsgWriteAck:
		return "WriteAck"
	case MsgReadRequest:
		return "Read"
	case MsgReadResponse:
		return "ReadResp"
	case MsgStop:
		return "Stop"
	default:
		return "Unknown"
	}
}

// @stance: creative
