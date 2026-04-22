// kubernetes#10182 — RWMutex + Channel Deadlock (3 goroutines)
// https://github.com/kubernetes/kubernetes/pull/10182
//
// G1 receives from podStatusChannel in syncBatch, then tries to acquire lock.
// G2/G3 acquire lock in SetPodStatus, then try to send on podStatusChannel.
// Deadlock: G1 waiting for lock held by G3, G3 waiting to send on channel.
// Flaky: 15/100
package k8s10182

import (
	"sync"
	"testing"
)

type statusManager struct {
	podStatusesLock  sync.RWMutex
	podStatusChannel chan bool
}

func (s *statusManager) Start() {
	go func() {
		for i := 0; i < 2; i++ {
			s.syncBatch()
		}
	}()
}

func (s *statusManager) syncBatch() {
	<-s.podStatusChannel
	s.DeletePodStatus()
}

func (s *statusManager) DeletePodStatus() {
	s.podStatusesLock.Lock()
	defer s.podStatusesLock.Unlock()
}

func (s *statusManager) SetPodStatus() {
	s.podStatusesLock.Lock()
	defer s.podStatusesLock.Unlock()
	s.podStatusChannel <- true
}

func newStatusManager() *statusManager {
	return &statusManager{
		podStatusChannel: make(chan bool),
	}
}

// Workload is the raw test function for benchmark use.
func Workload(t *testing.T) {
	s := newStatusManager()
	go s.Start()
	go s.SetPodStatus()
	go s.SetPodStatus()
}
