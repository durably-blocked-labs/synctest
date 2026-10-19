// kubernetes#26980 — RWMutex + Cond + Channel Deadlock (2 goroutines)
// https://github.com/kubernetes/kubernetes/pull/26980
//
// G1 (pop) acquires lock, finds pendingNotifications non-empty, enters select
// that blocks on stopCh — while still holding the lock.
// G2 tries to acquire lock → blocked.
// Main goroutine waits on resultCh from G2 → deadlock.
// The sync.Cond is used in the inner loop but not triggered in the deadlock path.
package k8s26980

import (
	"sync"
	"testing"
)

type processorListener struct {
	lock sync.RWMutex
	cond sync.Cond

	pendingNotifications []interface{}
}

func (p *processorListener) add(notification interface{}) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.pendingNotifications = append(p.pendingNotifications, notification)
	p.cond.Broadcast()
}

func (p *processorListener) pop(stopCh <-chan struct{}) {
	p.lock.Lock()
	defer p.lock.Unlock()
	for {
		for len(p.pendingNotifications) == 0 {
			select {
			case <-stopCh:
				return
			default:
			}
			p.cond.Wait()
		}
		select {
		case <-stopCh:
			return
		}
	}
}

func newProcessListener() *processorListener {
	ret := &processorListener{
		pendingNotifications: []interface{}{},
	}
	ret.cond.L = &ret.lock
	return ret
}

// Workload is the raw test function for benchmark use.
func Workload(t *testing.T) {
	pl := newProcessListener()
	stopCh := make(chan struct{})
	defer close(stopCh)
	pl.add(1)
	go pl.pop(stopCh)

	resultCh := make(chan struct{})
	go func() {
		pl.lock.Lock()
		close(resultCh)
	}()
	<-resultCh
	pl.lock.Unlock()
}
