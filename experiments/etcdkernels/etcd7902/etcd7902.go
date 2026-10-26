// etcd#7902 — Lock Around Channel Wait (deadlock)
// https://github.com/coreos/etcd/pull/7902
//
// Follower holds mutex, blocks on <-rcNextc.
// Leader needs mutex to close(nextc).
// Fix: remove the lock around rc.release().
package etcd7902

import (
	"sync"
	"testing"
)

type roundClient struct {
	progress int
	acquire  func()
	validate func()
	release  func()
}

func runElectionFunc() {
	rcs := make([]roundClient, 3)
	nextc := make(chan bool)
	for i := range rcs {
		var rcNextc chan bool
		setRcNextc := func() {
			rcNextc = nextc
		}
		rcs[i].acquire = func() {}
		rcs[i].validate = func() {
			setRcNextc()
		}
		rcs[i].release = func() {
			if i == 0 { // leader
				close(nextc)
				nextc = make(chan bool)
			}
			<-rcNextc // followers block here
		}
	}
	doRounds(rcs, 2)
}

func doRounds(rcs []roundClient, rounds int) {
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(len(rcs))
	for i := range rcs {
		go func(rc *roundClient) {
			defer wg.Done()
			for rc.progress < rounds || rounds <= 0 {
				rc.acquire()
				mu.Lock()
				rc.validate()
				mu.Unlock()
				rc.progress++
				mu.Lock() // leader blocks here
				rc.release()
				mu.Unlock()
			}
		}(&rcs[i])
	}
	wg.Wait()
}

// Workload is the raw test function for benchmark use.
func Workload(t *testing.T) {
	runElectionFunc()
}
