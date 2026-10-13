// serving#2137 — Breaker Mutex + Channel Deadlock (3 goroutines)
// https://github.com/knative/serving/issues/2137
//
// G1 (main) calls unlockAll, unlocks first request's mutex, waits on accepted.
// G2 enters Maybe(), acquires activeRequests slot, calls thunk which blocks on mutex.
// G3 enters Maybe(), blocks on activeRequests (capacity 1, G2 holds it).
// G1 blocks on accepted (G2 can't finish), G2 blocks on mutex, G3 blocks on channel.
// Deadlock: circular wait across lock + channel + channel.
package serving2137

import (
	"sync"
	"testing"
)

type token struct{}

type request struct {
	lock     *sync.Mutex
	accepted chan bool
}

type Breaker struct {
	pendingRequests chan token
	activeRequests  chan token
}

func (b *Breaker) Maybe(thunk func()) bool {
	var t token
	select {
	default:
		return false
	case b.pendingRequests <- t:
		b.activeRequests <- t
		defer func() { <-b.activeRequests; <-b.pendingRequests }()
		thunk()
		return true
	}
}

func (b *Breaker) concurrentRequest() request {
	r := request{lock: &sync.Mutex{}, accepted: make(chan bool, 1)}
	r.lock.Lock()
	var start sync.WaitGroup
	start.Add(1)
	go func() {
		start.Done()
		ok := b.Maybe(func() {
			r.lock.Lock()
			r.lock.Unlock()
		})
		r.accepted <- ok
	}()
	start.Wait()
	return r
}

func (b *Breaker) concurrentRequests(n int) []request {
	requests := make([]request, n)
	for i := range requests {
		requests[i] = b.concurrentRequest()
	}
	return requests
}

func newBreaker(queueDepth, maxConcurrency int32) *Breaker {
	return &Breaker{
		pendingRequests: make(chan token, queueDepth+maxConcurrency),
		activeRequests:  make(chan token, maxConcurrency),
	}
}

func unlock(req request) {
	req.lock.Unlock()
	ok := <-req.accepted
	req.accepted <- ok
}

func unlockAll(requests []request) {
	for _, lc := range requests {
		unlock(lc)
	}
}

// Workload is the raw test function for benchmark use.
func Workload(t *testing.T) {
	b := newBreaker(1, 1)
	locks := b.concurrentRequests(2)
	unlockAll(locks)
}
