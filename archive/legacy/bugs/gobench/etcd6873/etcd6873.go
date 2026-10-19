// etcd#6873 — Channel + Mutex Deadlock (3 goroutines)
// https://github.com/etcd-io/etcd/commit/7618fdd1d642e47cac70c03f637b0fd798a53a6e
//
// G2 holds updatec channel reader loop and calls coalesce() which needs mu.
// G3 holds mu in stop(), closes updatec, then blocks on <-donec.
// Deadlock: G2 blocked on mu (held by G3), G3 blocked on donec (closed by G2).
// Flaky: 9/100
package etcd6873

import (
	"sync"
	"testing"
)

type watchBroadcast struct{}

type watchBroadcasts struct {
	mu      sync.Mutex
	updatec chan *watchBroadcast
	donec   chan struct{}
}

func newWatchBroadcasts() *watchBroadcasts {
	wbs := &watchBroadcasts{
		updatec: make(chan *watchBroadcast, 1),
		donec:   make(chan struct{}),
	}
	go func() {
		defer close(wbs.donec)
		for wb := range wbs.updatec {
			wbs.coalesce(wb)
		}
	}()
	return wbs
}

func (wbs *watchBroadcasts) coalesce(_ *watchBroadcast) {
	wbs.mu.Lock()
	wbs.mu.Unlock()
}

func (wbs *watchBroadcasts) stop() {
	wbs.mu.Lock()
	defer wbs.mu.Unlock()
	close(wbs.updatec)
	<-wbs.donec
}

func (wbs *watchBroadcasts) update(wb *watchBroadcast) {
	select {
	case wbs.updatec <- wb:
	default:
	}
}

// Workload is the raw test function for benchmark use.
func Workload(t *testing.T) {
	wbs := newWatchBroadcasts()
	wbs.update(&watchBroadcast{})
	go wbs.stop()
}
