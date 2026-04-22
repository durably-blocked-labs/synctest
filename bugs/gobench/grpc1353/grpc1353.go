// grpc#1353 — Channel + Mutex Circular Wait (3 goroutines)
// https://github.com/grpc/grpc-go/pull/1353
//
// G2 holds mu and blocks sending on unbuffered channel.
// G3 reads from channel then tries to acquire mu.
// Fix: use a buffered channel and drain before sending.
package grpc1353

import (
	"sync"
	"testing"
)

type Balancer interface {
	Start()
	Up() func()
	Notify() <-chan bool
	Close()
	Done() <-chan struct{}
}

type roundRobin struct {
	mu     sync.Mutex
	addrCh chan bool
	done   chan struct{}
}

func (rr *roundRobin) Done() <-chan struct{} { return rr.done }

func (rr *roundRobin) Start() {
	rr.addrCh = make(chan bool)
	rr.done = make(chan struct{})
	go func() {
		for i := 0; i < 3; i++ {
			rr.watchAddrUpdates()
		}
		close(rr.done)
	}()
}

func (rr *roundRobin) Up() func() {
	return func() {
		rr.down()
	}
}

func (rr *roundRobin) Notify() <-chan bool {
	return rr.addrCh
}

func (rr *roundRobin) Close() {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	if rr.addrCh != nil {
		close(rr.addrCh)
	}
}

func (rr *roundRobin) watchAddrUpdates() {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	rr.addrCh <- true
}

func (rr *roundRobin) down() {
	rr.mu.Lock()
	defer rr.mu.Unlock()
}

type addrConn struct {
	mu   sync.Mutex
	down func()
}

func (ac *addrConn) tearDown() {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	if ac.down != nil {
		ac.down()
	}
}

type dialOptions struct {
	balancer Balancer
}

type ClientConn struct {
	dopts dialOptions
	conns []*addrConn
}

func (cc *ClientConn) lbWatcher() {
	for addr := range cc.dopts.balancer.Notify() {
		_ = addr
		var del []*addrConn
		for _, a := range cc.conns {
			del = append(del, a)
		}
		for _, c := range del {
			c.tearDown()
		}
	}
}

func newClientConn() *ClientConn {
	cc := &ClientConn{
		dopts: dialOptions{
			&roundRobin{},
		},
	}
	ac1 := &addrConn{down: cc.dopts.balancer.Up()}
	ac2 := &addrConn{down: cc.dopts.balancer.Up()}
	cc.conns = append(cc.conns, ac1, ac2)
	return cc
}

// Workload is the raw test function for benchmark use.
func Workload(t *testing.T) {
	cc := newClientConn()
	cc.dopts.balancer.Start()
	go cc.lbWatcher()
	go func() {
		<-cc.dopts.balancer.Done()
		cc.dopts.balancer.Close()
	}()
}
