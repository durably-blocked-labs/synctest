// Package rafttest provides a Raft transport that routes cross-node RPCs
// through the orchestratorv2 Orchestrator, enabling controlled message
// delivery ordering for deterministic distributed systems testing.
package rafttest

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing/synctest"

	"github.com/hashicorp/raft"
	"github.com/shubhaankar/synctest/orchestratorv2"
)

type envelopeKind int

const (
	envelopeRequest envelopeKind = iota
	envelopeResponse
)

type envelope struct {
	kind    envelopeKind
	reqID   uint64
	from    raft.ServerAddress
	to      raft.ServerAddress
	rpcType string

	command interface{}
	reader  io.Reader
	resp    raft.RPCResponse
}

// RaftTransport implements raft.Transport (and raft.WithPreVote, raft.WithClose)
// using an orchestrator-routed mailbox. All cross-node request and response
// messages go through the orchestratorv2 outbox so delivery order is controlled.
type RaftTransport struct {
	localAddr        raft.ServerAddress
	outbox           chan *orchestratorv2.PendingOp // unbuffered; to orchestrator
	internalConsumer chan raft.RPC                  // intra-bubble; Raft reads this
	mailbox          chan envelope                  // buffered; orchestrator writes here
	recvPermit       chan struct{}                  // unbuffered; recv Execute grants permit
	peers            map[raft.ServerAddress]*RaftTransport
	closeCh          chan struct{}

	nextReqID uint64
	pendingMu sync.Mutex
	pending   map[uint64]chan raft.RPCResponse
}

// Compile-time interface checks.
var _ raft.Transport = (*RaftTransport)(nil)
var _ raft.WithPreVote = (*RaftTransport)(nil)
var _ raft.WithClose = (*RaftTransport)(nil)
var _ orchestratorv2.NodeTransport = (*RaftTransport)(nil)

// NewRaftTransport creates a transport for the given address.
func NewRaftTransport(addr raft.ServerAddress) *RaftTransport {
	return &RaftTransport{
		localAddr:        addr,
		outbox:           make(chan *orchestratorv2.PendingOp),
		internalConsumer: make(chan raft.RPC, 16),
		mailbox:          make(chan envelope, 16),
		recvPermit:       make(chan struct{}),
		peers:            make(map[raft.ServerAddress]*RaftTransport),
		closeCh:          make(chan struct{}),
		pending:          make(map[uint64]chan raft.RPCResponse),
	}
}

// Connect registers a peer transport. Must be called before Run().
func (t *RaftTransport) Connect(peer *RaftTransport) {
	t.peers[peer.localAddr] = peer
}

// Addr implements orchestratorv2.NodeTransport.
func (t *RaftTransport) Addr() string {
	return string(t.localAddr)
}

// Outbox implements orchestratorv2.NodeTransport.
func (t *RaftTransport) Outbox() <-chan *orchestratorv2.PendingOp {
	return t.outbox
}

// StartBridge spawns the bridge goroutine that connects the orchestrator's
// delivery decisions to Raft's internal consumer channel and response waiters.
// Must be called inside the bubble.
func (t *RaftTransport) StartBridge() {
	go func() {
		for {
			select {
			case <-t.closeCh:
				return
			default:
			}

			synctest.CallExternal(func() {
				select {
				case t.outbox <- &orchestratorv2.PendingOp{
					Dir:  orchestratorv2.OpRecv,
					From: string(t.localAddr),
					Type: "recv",
					Execute: func() {
						t.recvPermit <- struct{}{}
					},
				}:
				case <-t.closeCh:
					return
				}

				select {
				case <-t.recvPermit:
				case <-t.closeCh:
					return
				}

				msg := <-t.mailbox
				t.handleEnvelope(msg)
			})
		}
	}()
}

func (t *RaftTransport) handleEnvelope(msg envelope) {
	switch msg.kind {
	case envelopeRequest:
		proxyRespCh := make(chan raft.RPCResponse, 1)
		t.internalConsumer <- raft.RPC{Command: msg.command, Reader: msg.reader, RespChan: proxyRespCh}
		go t.forwardResponse(msg, proxyRespCh)

	case envelopeResponse:
		t.pendingMu.Lock()
		respCh := t.pending[msg.reqID]
		t.pendingMu.Unlock()
		if respCh == nil {
			return
		}
		select {
		case respCh <- msg.resp:
		case <-t.closeCh:
		}
	}
}

func (t *RaftTransport) forwardResponse(req envelope, proxyRespCh <-chan raft.RPCResponse) {
	var resp raft.RPCResponse
	select {
	case resp = <-proxyRespCh:
	case <-t.closeCh:
		return
	}

	peer, ok := t.peers[req.from]
	if !ok {
		return
	}

	synctest.CallExternal(func() {
		select {
		case t.outbox <- &orchestratorv2.PendingOp{
			Dir:  orchestratorv2.OpSend,
			From: string(t.localAddr),
			To:   string(req.from),
			Type: req.rpcType + "Response",
			Execute: func() {
				peer.mailbox <- envelope{
					kind:  envelopeResponse,
					reqID: req.reqID,
					from:  t.localAddr,
					to:    req.from,
					resp:  resp,
				}
			},
		}:
		case <-t.closeCh:
		}
	})
}

// makeRPC sends an RPC to the target and waits for the orchestrator-delivered response.
func (t *RaftTransport) makeRPC(target raft.ServerAddress, cmd interface{}, r io.Reader) (raft.RPCResponse, error) {
	peer, ok := t.peers[target]
	if !ok {
		return raft.RPCResponse{}, fmt.Errorf("rafttest: no peer for %q", target)
	}

	reqID := atomic.AddUint64(&t.nextReqID, 1)
	respCh := make(chan raft.RPCResponse, 1)

	t.pendingMu.Lock()
	t.pending[reqID] = respCh
	t.pendingMu.Unlock()
	defer func() {
		t.pendingMu.Lock()
		delete(t.pending, reqID)
		t.pendingMu.Unlock()
	}()

	rpcType := rpcTypeName(cmd)
	synctest.CallExternal(func() {
		select {
		case t.outbox <- &orchestratorv2.PendingOp{
			Dir:  orchestratorv2.OpSend,
			From: string(t.localAddr),
			To:   string(target),
			Type: rpcType,
			Execute: func() {
				peer.mailbox <- envelope{
					kind:    envelopeRequest,
					reqID:   reqID,
					from:    t.localAddr,
					to:      target,
					rpcType: rpcType,
					command: cmd,
					reader:  r,
				}
			},
		}:
		case <-t.closeCh:
		}
	})

	var resp raft.RPCResponse
	synctest.CallExternal(func() {
		select {
		case resp = <-respCh:
		case <-t.closeCh:
			resp = raft.RPCResponse{Error: fmt.Errorf("rafttest: transport %q closed", t.localAddr)}
		}
	})
	if resp.Error != nil {
		return resp, resp.Error
	}
	return resp, nil
}

// AppendEntries implements raft.Transport.
func (t *RaftTransport) AppendEntries(id raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	rpcResp, err := t.makeRPC(target, args, nil)
	if err != nil {
		return err
	}
	*resp = *rpcResp.Response.(*raft.AppendEntriesResponse)
	return nil
}

// RequestVote implements raft.Transport.
func (t *RaftTransport) RequestVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	rpcResp, err := t.makeRPC(target, args, nil)
	if err != nil {
		return err
	}
	*resp = *rpcResp.Response.(*raft.RequestVoteResponse)
	return nil
}

// RequestPreVote implements raft.WithPreVote.
func (t *RaftTransport) RequestPreVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestPreVoteRequest, resp *raft.RequestPreVoteResponse) error {
	rpcResp, err := t.makeRPC(target, args, nil)
	if err != nil {
		return err
	}
	*resp = *rpcResp.Response.(*raft.RequestPreVoteResponse)
	return nil
}

// InstallSnapshot implements raft.Transport.
func (t *RaftTransport) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, args *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader) error {
	rpcResp, err := t.makeRPC(target, args, data)
	if err != nil {
		return err
	}
	*resp = *rpcResp.Response.(*raft.InstallSnapshotResponse)
	return nil
}

// TimeoutNow implements raft.Transport.
func (t *RaftTransport) TimeoutNow(id raft.ServerID, target raft.ServerAddress, args *raft.TimeoutNowRequest, resp *raft.TimeoutNowResponse) error {
	rpcResp, err := t.makeRPC(target, args, nil)
	if err != nil {
		return err
	}
	*resp = *rpcResp.Response.(*raft.TimeoutNowResponse)
	return nil
}

// AppendEntriesPipeline implements raft.Transport.
// Returns ErrPipelineReplicationNotSupported - the orchestrator does not support pipelining.
func (t *RaftTransport) AppendEntriesPipeline(id raft.ServerID, target raft.ServerAddress) (raft.AppendPipeline, error) {
	return nil, raft.ErrPipelineReplicationNotSupported
}

// Consumer implements raft.Transport. Returns the consumer channel that Raft
// reads from to process incoming RPCs.
func (t *RaftTransport) Consumer() <-chan raft.RPC {
	return t.internalConsumer
}

// LocalAddr implements raft.Transport.
func (t *RaftTransport) LocalAddr() raft.ServerAddress {
	return t.localAddr
}

// EncodePeer implements raft.Transport.
func (t *RaftTransport) EncodePeer(id raft.ServerID, addr raft.ServerAddress) []byte {
	return []byte(addr)
}

// DecodePeer implements raft.Transport.
func (t *RaftTransport) DecodePeer(buf []byte) raft.ServerAddress {
	return raft.ServerAddress(buf)
}

// SetHeartbeatHandler implements raft.Transport. No-op: heartbeats go through
// the normal consumer channel.
func (t *RaftTransport) SetHeartbeatHandler(cb func(rpc raft.RPC)) {}

// Close implements raft.WithClose.
func (t *RaftTransport) Close() error {
	select {
	case <-t.closeCh:
		// already closed
	default:
		close(t.closeCh)
	}
	return nil
}

// rpcTypeName returns a human-readable name for an RPC command type.
func rpcTypeName(cmd interface{}) string {
	switch cmd.(type) {
	case *raft.AppendEntriesRequest:
		return "AppendEntries"
	case *raft.RequestVoteRequest:
		return "RequestVote"
	case *raft.RequestPreVoteRequest:
		return "RequestPreVote"
	case *raft.InstallSnapshotRequest:
		return "InstallSnapshot"
	case *raft.TimeoutNowRequest:
		return "TimeoutNow"
	default:
		return fmt.Sprintf("%T", cmd)
	}
}
