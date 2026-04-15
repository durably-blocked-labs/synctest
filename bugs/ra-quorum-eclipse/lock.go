package raquorumeclipse

import (
	"slices"
	"sync"
)

type contenderState int

const (
	contenderIdle contenderState = iota
	contenderRequesting
	contenderHeld
)

type contenderNode struct {
	id         string
	transport  nodeTransport
	quorum     []string
	quorumNeed int
	priority   int

	mu           sync.Mutex
	state        contenderState
	round        int
	grants       map[string]bool
	responded    map[string]bool
	relinquished map[string]bool
	sawFailed    bool
	gate         chan struct{}
	gateClosed   bool

	contentionReadyFrom string
	contentionReadySent bool
	contentionReadyHook func()
}

func newContenderNode(id string, transport nodeTransport, quorum []string, quorumNeed int, priority int) *contenderNode {
	return &contenderNode{
		id:         id,
		transport:  transport,
		quorum:     quorum,
		quorumNeed: quorumNeed,
		priority:   priority,
	}
}

func (n *contenderNode) Start() {
	go func() {
		for msg := range n.transport.Mailbox() {
			n.handleMessage(msg)
		}
	}()
}

func (n *contenderNode) SetContentionReadyHook(grantFrom string, hook func()) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.contentionReadyFrom = grantFrom
	n.contentionReadyHook = hook
}

func (n *contenderNode) AcquireLock() {
	n.mu.Lock()
	n.state = contenderRequesting
	n.round++
	round := n.round
	n.grants = make(map[string]bool, len(n.quorum))
	n.responded = make(map[string]bool, len(n.quorum))
	n.relinquished = make(map[string]bool, len(n.quorum))
	n.sawFailed = false
	n.gate = make(chan struct{})
	n.gateClosed = false
	n.contentionReadySent = false
	n.mu.Unlock()

	for _, voter := range n.quorum {
		n.transport.Send(voter, Message{
			Kind:     MsgRequest,
			From:     n.id,
			Round:    round,
			Priority: n.priority,
		})
	}

	<-n.gate

	n.mu.Lock()
	n.state = contenderHeld
	n.mu.Unlock()
}

func (n *contenderNode) ReleaseLock() {
	n.mu.Lock()
	round := n.round
	n.state = contenderIdle
	n.mu.Unlock()

	for _, voter := range n.quorum {
		n.transport.Send(voter, Message{
			Kind:  MsgRelease,
			From:  n.id,
			Round: round,
		})
	}
}

func (n *contenderNode) handleMessage(msg Message) {
	n.mu.Lock()
	if msg.Round != n.round && msg.Kind != MsgFailed {
		n.mu.Unlock()
		return
	}

	var notify func()
	switch msg.Kind {
	case MsgGrant:
		if n.state != contenderRequesting {
			n.mu.Unlock()
			return
		}
		n.grants[msg.From] = true
		n.responded[msg.From] = true
		notify = n.maybeContentionReadyLocked()
		n.maybeOpenGateLocked()

	case MsgFailed:
		if n.state != contenderRequesting {
			n.mu.Unlock()
			return
		}
		n.sawFailed = true
		delete(n.grants, msg.From)
		n.responded[msg.From] = true
		notify = n.maybeContentionReadyLocked()

	case MsgInquire:
		if n.state != contenderRequesting {
			n.mu.Unlock()
			return
		}
		if !n.sawFailed || !n.grants[msg.From] || n.relinquished[msg.From] {
			n.mu.Unlock()
			return
		}
		n.relinquished[msg.From] = true
		round := n.round
		n.mu.Unlock()
		n.transport.Send(msg.From, Message{
			Kind:  MsgRelinquish,
			From:  n.id,
			Round: round,
		})
		return
	}

	n.mu.Unlock()
	if notify != nil {
		notify()
	}
}

func (n *contenderNode) maybeOpenGateLocked() {
	if n.gateClosed {
		return
	}
	if len(n.responded) != len(n.quorum) {
		return
	}
	if len(n.grants) < n.quorumNeed {
		return
	}
	close(n.gate)
	n.gateClosed = true
}

func (n *contenderNode) maybeContentionReadyLocked() func() {
	if n.contentionReadyHook == nil || n.contentionReadySent {
		return nil
	}
	if n.contentionReadyFrom == "" || !n.sawFailed || !n.grants[n.contentionReadyFrom] {
		return nil
	}
	n.contentionReadySent = true
	return n.contentionReadyHook
}

type queuedRequest struct {
	from     string
	round    int
	priority int
}

type voterMode int

const (
	modeDelayedGrant voterMode = iota
	modeRevokerBug
	modeFailThenGrant
)

type voterNode struct {
	id        string
	transport nodeTransport
	mode      voterMode

	mu             sync.Mutex
	holder         queuedRequest
	hasHolder      bool
	queue          []queuedRequest
	pendingDelayed map[string]queuedRequest
}

func newVoterNode(id string, transport nodeTransport, mode voterMode) *voterNode {
	return &voterNode{
		id:             id,
		transport:      transport,
		mode:           mode,
		pendingDelayed: make(map[string]queuedRequest),
	}
}

func (n *voterNode) Start() {
	go func() {
		for msg := range n.transport.Mailbox() {
			n.handleMessage(msg)
		}
	}()
}

func (n *voterNode) handleMessage(msg Message) {
	switch n.mode {
	case modeDelayedGrant:
		n.handleDelayedGrant(msg)
	case modeRevokerBug:
		n.handleRevokerBug(msg)
	case modeFailThenGrant:
		n.handleFailThenGrant(msg)
	}
}

func (n *voterNode) handleDelayedGrant(msg Message) {
	switch msg.Kind {
	case MsgRequest:
		switch msg.From {
		case "A":
			n.mu.Lock()
			n.pendingDelayed[msg.From] = queuedRequest{from: msg.From, round: msg.Round, priority: msg.Priority}
			n.mu.Unlock()
		case "B":
			n.sendFailed("B", msg.Round)
			n.mu.Lock()
			req, ok := n.pendingDelayed["A"]
			if ok {
				delete(n.pendingDelayed, "A")
			}
			n.mu.Unlock()
			if ok {
				n.sendGrant(req.from, req.round)
			}
		default:
			n.sendFailed(msg.From, msg.Round)
		}
	}
}

func (n *voterNode) handleFailThenGrant(msg Message) {
	if msg.Kind != MsgRequest {
		return
	}
	if msg.From == "A" {
		n.sendFailed(msg.From, msg.Round)
		return
	}
	n.sendGrant(msg.From, msg.Round)
}

func (n *voterNode) handleRevokerBug(msg Message) {
	switch msg.Kind {
	case MsgRequest:
		req := queuedRequest{from: msg.From, round: msg.Round, priority: msg.Priority}

		n.mu.Lock()
		defer n.mu.Unlock()

		if !n.hasHolder {
			n.hasHolder = true
			n.holder = req
			n.mu.Unlock()
			n.sendGrant(req.from, req.round)
			n.mu.Lock()
			return
		}

		n.enqueueLocked(req)
		if req.priority < n.holder.priority {
			holder := n.holder
			n.mu.Unlock()
			n.sendInquire(holder.from, holder.round)
			n.mu.Lock()
		}

	case MsgRelinquish:
		n.mu.Lock()
		if !n.hasHolder || n.holder.from != msg.From {
			n.mu.Unlock()
			return
		}

		old := n.holder
		n.hasHolder = false
		n.enqueueLocked(old)
		next, ok := n.popBestLocked()
		if ok {
			n.hasHolder = true
			n.holder = next
		}
		n.mu.Unlock()

		if ok {
			// BUG: the vote is re-granted before the old holder is told its
			// previous grant is no longer valid.
			n.sendGrant(next.from, next.round)
		}
		n.sendFailed(old.from, old.round)

	case MsgRelease:
		n.mu.Lock()
		if n.hasHolder && n.holder.from == msg.From {
			n.hasHolder = false
		}
		next, ok := n.popBestLocked()
		if ok {
			n.hasHolder = true
			n.holder = next
		}
		n.mu.Unlock()

		if ok {
			n.sendGrant(next.from, next.round)
		}
	}
}

func (n *voterNode) enqueueLocked(req queuedRequest) {
	for _, queued := range n.queue {
		if queued.from == req.from && queued.round == req.round {
			return
		}
	}
	n.queue = append(n.queue, req)
}

func (n *voterNode) popBestLocked() (queuedRequest, bool) {
	if len(n.queue) == 0 {
		return queuedRequest{}, false
	}
	bestIndex := 0
	for i := 1; i < len(n.queue); i++ {
		if n.queue[i].priority < n.queue[bestIndex].priority {
			bestIndex = i
		}
	}
	best := n.queue[bestIndex]
	n.queue = slices.Delete(n.queue, bestIndex, bestIndex+1)
	return best, true
}

func (n *voterNode) sendGrant(to string, round int) {
	n.transport.Send(to, Message{
		Kind:  MsgGrant,
		From:  n.id,
		Round: round,
	})
}

func (n *voterNode) sendFailed(to string, round int) {
	n.transport.Send(to, Message{
		Kind:  MsgFailed,
		From:  n.id,
		Round: round,
	})
}

func (n *voterNode) sendInquire(to string, round int) {
	n.transport.Send(to, Message{
		Kind:  MsgInquire,
		From:  n.id,
		Round: round,
	})
}
