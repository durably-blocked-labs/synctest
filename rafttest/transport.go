// Package rafttest provides a Raft transport that routes cross-node RPCs
// through the orchestrator Orchestrator, enabling controlled message
// delivery ordering for deterministic distributed systems testing.
//
// Qualified profile: attached-bounded/v1, schedule-only unified replay.
// Select control over host-owned outbox/control channels is unsupported and
// fails closed; bubble-owned selects can be qualified separately. Peer closure
// after RPC publication is an unsupported whole-trial infrastructure stop, not
// a modeled terminal response. Cancel the trial before ordinary teardown.
package rafttest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing/synctest"

	"github.com/hashicorp/raft"
	"github.com/shubhaankar/synctest/orchestrator"
)

var ErrTransportClosed = errors.New("rafttest: transport closed")

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

	command   interface{}
	reader    io.Reader
	resp      raft.RPCResponse
	completed chan error
}

// RaftTransport implements raft.Transport (and raft.WithPreVote, raft.WithClose)
// using an orchestrator-routed mailbox. All cross-node request and response
// messages go through the orchestrator outbox so delivery order is controlled.
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
	outbox           chan *orchestrator.PendingOp // unbuffered; to orchestrator
	internalConsumer chan raft.RPC                // created inside bubble by StartBridge; Raft reads this
	mailbox          chan envelope                // buffered; orchestrator writes here
	peers            map[raft.ServerAddress]*RaftTransport
	closeCh          chan struct{}
	closeOnce        sync.Once
	publications     orchestrator.Publications
	bridgeReady      atomic.Pointer[synctest.Ready]
	localStop        chan struct{} // created and closed only while attached
	trialCtx         context.Context
	failTrial        func(error)
	closeMu          sync.Mutex
	closeWatchers    map[string]func()

	nextReqID uint64
	pendingMu sync.Mutex
	pending   map[uint64]chan raft.RPCResponse
}

// Compile-time interface checks.
var _ raft.Transport = (*RaftTransport)(nil)
var _ raft.WithPreVote = (*RaftTransport)(nil)
var _ raft.WithClose = (*RaftTransport)(nil)
var _ orchestrator.NodeTransport = (*RaftTransport)(nil)
var _ orchestrator.AcknowledgedTransport = (*RaftTransport)(nil)

// NewRaftTransport creates a transport for the given address.
// internalConsumer is intentionally left nil here; it is created inside the
// bubble by StartBridge() so that Raft goroutines receive from a bubble channel.
func NewRaftTransport(addr raft.ServerAddress) *RaftTransport {
	return &RaftTransport{
		localAddr: addr,
		// Buffered to avoid deadlock when a bubble must report idle before the
		// orchestrator starts draining outboxes.
		outbox:        make(chan *orchestrator.PendingOp, 64),
		mailbox:       make(chan envelope, 16),
		peers:         make(map[raft.ServerAddress]*RaftTransport),
		closeCh:       make(chan struct{}),
		pending:       make(map[uint64]chan raft.RPCResponse),
		closeWatchers: make(map[string]func()),
	}
}

// Connect registers a peer transport. Must be called before Run().
func (t *RaftTransport) Connect(peer *RaftTransport) {
	t.peers[peer.localAddr] = peer
}

// Addr implements orchestrator.NodeTransport.
func (t *RaftTransport) Addr() string {
	return string(t.localAddr)
}

// Outbox implements orchestrator.NodeTransport.
func (t *RaftTransport) Outbox() <-chan *orchestrator.PendingOp {
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
	t.localStop = make(chan struct{})

	go func() {
		for {
			var msg envelope
			ready := synctest.NewReady()
			t.bridgeReady.Store(ready)
			synctest.ExternalWaitReady(func() {
				select {
				case msg = <-t.mailbox:
				case <-t.closeCh:
					msg = envelope{kind: envelopeClose}
				}
			}, ready)
			t.bridgeReady.Store(nil)
			if msg.kind == envelopeClose {
				close(t.localStop)
				return
			}
			err := t.handleEnvelope(msg)
			if msg.completed != nil {
				msg.completed <- err
			}
		}
	}()
}

func (t *RaftTransport) handleEnvelope(msg envelope) error {
	switch msg.kind {
	case envelopeRequest:
		proxyRespCh := make(chan raft.RPCResponse, 1)
		select {
		case t.internalConsumer <- raft.RPC{Command: msg.command, Reader: msg.reader, RespChan: proxyRespCh}:
			go t.forwardResponse(msg, proxyRespCh)
		case <-t.localStop:
			return ErrTransportClosed
		default:
			return orchestrator.ResourceLimitError{Resource: "internal consumer", Limit: cap(t.internalConsumer)}
		}

	case envelopeResponse:
		t.pendingMu.Lock()
		respCh := t.pending[msg.reqID]
		t.pendingMu.Unlock()
		if respCh == nil {
			return nil
		}
		// respCh is buffered(1) and created inside the bubble — durable send.
		select {
		case respCh <- msg.resp:
		case <-t.localStop:
			return ErrTransportClosed
		default:
			return fmt.Errorf("rafttest: duplicate/full response channel for request %d", msg.reqID)
		}
	}
	return nil
}

func (t *RaftTransport) forwardResponse(req envelope, proxyRespCh <-chan raft.RPCResponse) {
	var resp raft.RPCResponse
	select {
	case resp = <-proxyRespCh:
	case <-t.localStop:
		return
	}

	peer, ok := t.peers[req.from]
	if !ok {
		panic(fmt.Errorf("rafttest: no response peer for %q", req.from))
	}

	op := &orchestrator.PendingOp{Dir: orchestrator.OpSend, From: string(t.localAddr), To: string(req.from), Type: req.rpcType + "Response"}
	op.Deliver = func(ctx context.Context) (orchestrator.DeliveryReceipt, error) {
		return peer.deliver(ctx, envelope{kind: envelopeResponse, reqID: req.reqID, from: t.localAddr, to: req.from, resp: resp})
	}
	if err := t.publish(op); err != nil {
		if errors.Is(err, ErrTransportClosed) {
			return
		}
		panic(err)
	}

}

// makeRPC publishes while attached using a bounded nonblocking outbox, then
// waits on a bubble-owned response or bridge-owned local cancellation.
// A full outbox is model exhaustion, never a silently dropped request.
func (t *RaftTransport) makeRPC(target raft.ServerAddress, cmd interface{}, r io.Reader) (raft.RPCResponse, error) {
	peer, ok := t.peers[target]
	if !ok {
		return raft.RPCResponse{}, fmt.Errorf("rafttest: no peer for %q", target)
	}

	// Early exit if this transport or the peer is already closed.
	// Checking peer.closeCh here avoids the expensive ExternalWait cycle
	// for RPCs to peers that have already shut down.
	select {
	case <-t.closeCh:
		return raft.RPCResponse{}, fmt.Errorf("rafttest: transport closed")
	default:
	}
	select {
	case <-peer.closeCh:
		return raft.RPCResponse{}, fmt.Errorf("rafttest: peer %q closed", target)
	default:
	}

	reqID := atomic.AddUint64(&t.nextReqID, 1)
	respCh := make(chan raft.RPCResponse, 1)

	t.pendingMu.Lock()
	if len(t.pending) >= 256 {
		t.pendingMu.Unlock()
		panic(orchestrator.ResourceLimitError{Resource: "pending RPCs", Limit: 256})
	}
	t.pending[reqID] = respCh
	t.pendingMu.Unlock()
	defer func() {
		t.pendingMu.Lock()
		delete(t.pending, reqID)
		t.pendingMu.Unlock()
	}()

	rpcType := rpcTypeName(cmd)

	op := &orchestrator.PendingOp{Dir: orchestrator.OpSend, From: string(t.localAddr), To: string(target), Type: rpcType}
	op.Deliver = func(ctx context.Context) (orchestrator.DeliveryReceipt, error) {
		return peer.deliver(ctx, envelope{kind: envelopeRequest, reqID: reqID, from: t.localAddr, to: target, rpcType: rpcType, command: cmd, reader: r})
	}
	// Register before publication so peer closure cannot be lost.
	key := fmt.Sprintf("%s:%d", t.localAddr, reqID)
	peer.closeMu.Lock()
	select {
	case <-peer.closeCh:
		peer.closeMu.Unlock()
		return raft.RPCResponse{}, fmt.Errorf("rafttest: peer %q closed", target)
	default:
	}
	peer.closeWatchers[key] = func() {
		if t.trialCtx != nil && t.trialCtx.Err() == nil && t.failTrial != nil {
			t.failTrial(fmt.Errorf("rafttest: peer %q closed after publication (unsupported whole-trial infrastructure stop)", target))
		}
	}
	peer.closeMu.Unlock()
	defer func() { peer.closeMu.Lock(); delete(peer.closeWatchers, key); peer.closeMu.Unlock() }()
	if err := t.publish(op); err != nil {
		if errors.Is(err, ErrTransportClosed) {
			return raft.RPCResponse{}, err
		}
		panic(err)
	}
	var resp raft.RPCResponse
	select {
	case resp = <-respCh:
	case <-t.localStop:
		return raft.RPCResponse{}, ErrTransportClosed
	}

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

// BindTrial installs the coordinator's causal infrastructure-stop boundary.
// Called before bubbles start; teardown cancels the context before Close.
func (t *RaftTransport) BindTrial(ctx context.Context, fail func(error)) {
	t.trialCtx, t.failTrial = ctx, fail
}

// Close touches only host-owned state. The reattached bridge closes localStop.
func (t *RaftTransport) Close() error {
	t.closeOnce.Do(func() {
		t.closeMu.Lock()
		defer t.closeMu.Unlock()
		for _, fail := range t.closeWatchers {
			fail()
		}
		close(t.closeCh)
	})
	return nil
}

func (t *RaftTransport) PublicationPolicy() string { return orchestrator.AttachedPublicationV1 }
func (t *RaftTransport) PendingPublications() int  { return t.publications.Pending() }

// WaitPublications is bookkeeping for callers; qualification uses a zero
// PendingPublications snapshot and the versioned attached publication policy.
func (t *RaftTransport) WaitPublications(ctx context.Context) error { return t.publications.Wait(ctx) }

func (t *RaftTransport) publish(op *orchestrator.PendingOp) error {
	id, complete := t.publications.Begin(string(t.localAddr))
	op.Occurrence = id
	defer complete()
	select {
	case <-t.closeCh:
		return ErrTransportClosed
	default:
	}
	select {
	case t.outbox <- op:
		return nil
	case <-t.closeCh:
		return ErrTransportClosed
	default:
		return orchestrator.ResourceLimitError{Resource: "outbox", Limit: cap(t.outbox)}
	}

}

func (t *RaftTransport) deliver(ctx context.Context, msg envelope) (orchestrator.DeliveryReceipt, error) {
	if err := ctx.Err(); err != nil {
		return orchestrator.DeliveryReceipt{}, err
	}
	select {
	case <-t.closeCh:
		return orchestrator.DeliveryReceipt{}, ErrTransportClosed
	default:
	}
	ready := t.bridgeReady.Load()
	if ready == nil {
		return orchestrator.DeliveryReceipt{}, fmt.Errorf("rafttest: %s bridge not receiving", t.localAddr)
	}
	msg.completed = make(chan error, 1)
	select {
	case <-t.closeCh:
		return orchestrator.DeliveryReceipt{}, ErrTransportClosed
	case t.mailbox <- msg:
		return orchestrator.DeliveryReceipt{WaitReady: ready.Wait, Completed: msg.completed, Cancelled: t.closeCh}, nil
	default:
		return orchestrator.DeliveryReceipt{}, fmt.Errorf("rafttest: %s mailbox full", t.localAddr)
	}
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
