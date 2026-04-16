package quorumreadrepair

import (
	"sort"
	"sync"
)

type Replica struct {
	addr     string
	tr       *OrchestratorTransport
	applyCh  chan applyReq
	repairCh chan applyReq
	closeCh  chan struct{}

	mu    sync.Mutex
	store map[string][]VersionedValue
}

type applyReq struct {
	msg Message
	ack string
}

func compareClock(a, b Clock) int {
	aGreater := false
	bGreater := false
	keys := make(map[string]struct{}, len(a)+len(b))
	for k := range a {
		keys[k] = struct{}{}
	}
	for k := range b {
		keys[k] = struct{}{}
	}
	for k := range keys {
		av := a[k]
		bv := b[k]
		if av > bv {
			aGreater = true
		}
		if bv > av {
			bGreater = true
		}
	}
	switch {
	case aGreater && !bGreater:
		return 1
	case bGreater && !aGreater:
		return -1
	default:
		return 0
	}
}

func mergeSiblings(existing, incoming []VersionedValue) []VersionedValue {
	all := append(cloneValues(existing), cloneValues(incoming)...)
	var kept []VersionedValue
	for i, candidate := range all {
		dominated := false
		duplicate := false
		for j, other := range all {
			if i == j {
				continue
			}
			if compareClock(candidate.Clock, other.Clock) < 0 {
				dominated = true
				break
			}
			if candidate.Value == other.Value && clockEqual(candidate.Clock, other.Clock) && j < i {
				duplicate = true
				break
			}
		}
		if !dominated && !duplicate {
			kept = append(kept, VersionedValue{Value: candidate.Value, Clock: cloneClock(candidate.Clock)})
		}
	}
	sortValues(kept)
	return kept
}

func buggyRepairMerge(existing, incoming []VersionedValue) []VersionedValue {
	all := mergeSiblings(existing, incoming)
	if len(all) <= 1 {
		return all
	}
	winner := all[0]
	for _, v := range all[1:] {
		if clockScore(v.Clock) >= clockScore(winner.Clock) {
			winner = v
		}
	}
	return []VersionedValue{{Value: winner.Value, Clock: cloneClock(winner.Clock)}}
}

func clockScore(c Clock) int {
	var score int
	for _, v := range c {
		score += v
	}
	return score
}

func clockEqual(a, b Clock) bool {
	if len(a) != len(b) {
		return false
	}
	for k, av := range a {
		if b[k] != av {
			return false
		}
	}
	return true
}

func sortValues(values []VersionedValue) {
	sort.Slice(values, func(i, j int) bool {
		left := values[i]
		right := values[j]
		if left.Value != right.Value {
			return left.Value < right.Value
		}
		return clockLess(left.Clock, right.Clock)
	})
}

func clockLess(a, b Clock) bool {
	keysA := make([]string, 0, len(a))
	for k := range a {
		keysA = append(keysA, k)
	}
	keysB := make([]string, 0, len(b))
	for k := range b {
		keysB = append(keysB, k)
	}
	sort.Strings(keysA)
	sort.Strings(keysB)

	for i := 0; i < len(keysA) && i < len(keysB); i++ {
		if keysA[i] != keysB[i] {
			return keysA[i] < keysB[i]
		}
		av := a[keysA[i]]
		bv := b[keysB[i]]
		if av != bv {
			return av < bv
		}
	}
	return len(keysA) < len(keysB)
}

func NewReplica(addr string, tr *OrchestratorTransport) *Replica {
	return &Replica{
		addr:     addr,
		tr:       tr,
		applyCh:  make(chan applyReq, 64),
		repairCh: make(chan applyReq, 64),
		closeCh:  make(chan struct{}),
		store:    make(map[string][]VersionedValue),
	}
}

func (r *Replica) Start() {
	go r.router()
	go r.applyLoop()
	go r.repairLoop()
}

func (r *Replica) router() {
	defer close(r.applyCh)
	defer close(r.repairCh)

	for msg := range r.tr.Mailbox() {
		switch msg.Kind {
		case MsgPut:
			req := applyReq{msg: msg, ack: msg.From}
			select {
			case r.applyCh <- req:
			case <-r.tr.closeCh:
				return
			}
		case MsgRepair:
			req := applyReq{msg: msg, ack: msg.From}
			select {
			case r.repairCh <- req:
			case <-r.tr.closeCh:
				return
			}
		case MsgGet:
			r.tr.Send(msg.From, Message{
				Kind:      MsgGetResp,
				From:      r.addr,
				RequestID: msg.RequestID,
				Key:       msg.Key,
				Values:    r.Snapshot(msg.Key),
			})
		}
	}
}

func (r *Replica) applyLoop() {
	for req := range r.applyCh {
		r.mu.Lock()
		r.store[req.msg.Key] = mergeSiblings(r.store[req.msg.Key], req.msg.Values)
		r.mu.Unlock()

		r.tr.Send(req.ack, Message{
			Kind:      MsgPutAck,
			From:      r.addr,
			RequestID: req.msg.RequestID,
			Key:       req.msg.Key,
		})
	}
}

func (r *Replica) repairLoop() {
	for req := range r.repairCh {
		r.mu.Lock()
		r.store[req.msg.Key] = buggyRepairMerge(r.store[req.msg.Key], req.msg.Values)
		r.mu.Unlock()

		r.tr.Send(req.ack, Message{
			Kind:      MsgRepairAck,
			From:      r.addr,
			RequestID: req.msg.RequestID,
			Key:       req.msg.Key,
		})
	}
}

func (r *Replica) Snapshot(key string) []VersionedValue {
	r.mu.Lock()
	defer r.mu.Unlock()
	return cloneValues(r.store[key])
}
