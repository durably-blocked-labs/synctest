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
	envelopeClose // sentinel: transport closed
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
//
// Bridge goroutines use ExternalWait to signal to the bubble that they are
// waiting on orchestrator-controlled channels. When all goroutines in the
// bubble are in ExternalWait, the bubble goes idle and notifies the global
// orchestrator, which then decides what to deliver next.
//
// internalConsumer MUST be created inside the bubble (via StartBridge) so that
// Raft goroutines blocking on it are durably blocked and the bubble can go idle.
type RaftTransport struct {
	localAddr        raft.ServerAddress
	outbox           chan *orchestratorv2.PendingOp // unbuffered; to orchestrator
	internalConsumer chan raft.RPC                  // created inside bubble by StartBridge; Raft reads this
	mailbox          chan envelope                  // buffered; orchestrator writes here
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
// internalConsumer is intentionally left nil here; it is created inside the
// bubble by StartBridge() so that Raft goroutines receive from a bubble channel.
func NewRaftTransport(addr raft.ServerAddress) *RaftTransport {
	return &RaftTransport{
		localAddr: addr,
		// Buffered to avoid deadlock when a bubble must report idle before the
		// orchestrator starts draining outboxes.
		outbox:    make(chan *orchestratorv2.PendingOp, 64),
		mailbox:   make(chan envelope, 16),
		peers:     make(map[raft.ServerAddress]*RaftTransport),
		closeCh:   make(chan struct{}),
		pending:   make(map[uint64]chan raft.RPCResponse),
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

// StartBridge creates the internalConsumer channel (inside the bubble so Raft
// goroutines are durably blocked on it) and spawns the bridge goroutine that
// connects the orchestrator's delivery decisions to Raft's consumer channel.
//
// The bridge uses ExternalWait to block on the mailbox. When all goroutines
// in the bubble are in ExternalWait (including this one), the bubble goes idle
// and the global orchestrator decides which message to deliver next. The
// orchestrator writes directly to the mailbox, then sends Resume — at which
// point the bridge goroutine wakes and processes the envelope.
//
// Must be called inside the bubble, before raft.NewRaft().
func (t *RaftTransport) StartBridge() {
	// Create inside the bubble so raft goroutines blocking on Consumer() are
	// durably blocked (bubble channel), enabling idle detection.
	t.internalConsumer = make(chan raft.RPC, 16)

	go func() {
		for {
			var msg envelope
			synctest.ExternalWait(func() {
				select {
				case msg = <-t.mailbox:
				case <-t.closeCh:
					msg = envelope{kind: envelopeClose}
				}
			})
			if msg.kind == envelopeClose {
				return
			}
			t.handleEnvelope(msg)
		}
	}()
}

func (t *RaftTransport) handleEnvelope(msg envelope) {
	switch msg.kind {
	case envelopeRequest:
		proxyRespCh := make(chan raft.RPCResponse, 1)
		select {
		case t.internalConsumer <- raft.RPC{Command: msg.command, Reader: msg.reader, RespChan: proxyRespCh}:
			go t.forwardResponse(msg, proxyRespCh)
		case <-t.closeCh:
			return
		}

	case envelopeResponse:
		t.pendingMu.Lock()
		respCh := t.pending[msg.reqID]
		t.pendingMu.Unlock()
		if respCh == nil {
			return
		}
		// respCh is buffered(1) and created inside the bubble — durable send.
		respCh <- msg.resp
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

	synctest.ExternalWait(func() {
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
//
// Phase 1: ExternalWait on the outbox send. The orchestrator reads the PendingOp,
// calls Execute (delivering the envelope to the target's mailbox), then resumes
// the target bubble. The sender's bubble stays frozen until the response arrives.
//
// Phase 2: ExternalWait on the response channel. The orchestrator delivers the
// response envelope to this node's mailbox; the bridge goroutine processes it
// and writes to respCh, completing this ExternalWait.
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

	// Phase 1: deliver request to target via orchestrator.
	synctest.ExternalWait(func() {
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

	// Phase 2: wait for response delivered by orchestrator.
	// respCh is created inside the bubble, so this must remain a durable
	// in-bubble receive (not ExternalWait).
	resp := <-respCh
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
