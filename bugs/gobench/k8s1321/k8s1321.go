// kubernetes#1321 — Mux Distribute + StopWatching Deadlock (2 goroutines)
// https://github.com/kubernetes/kubernetes/pull/1321
//
// G2 (loop) holds m.lock and blocks sending on w.result channel.
// G1 calls stopWatching() which needs m.lock to close w.result.
// Deadlock: G2 holds lock + blocked on channel, G1 needs lock to unblock G2.
// Flaky: 1/100
package k8s1321

import (
	"sync"
	"testing"
)

type muxWatcher struct {
	result chan struct{}
	m      *Mux
	id     int64
}

func (mw *muxWatcher) Stop() {
	mw.m.stopWatching(mw.id)
}

type Mux struct {
	lock      sync.Mutex
	globalMtx sync.Mutex // was package-level in original
	watchers  map[int64]*muxWatcher
}

func newMux() *Mux {
	m := &Mux{
		watchers: map[int64]*muxWatcher{},
	}
	go m.loop()
	return m
}

func (m *Mux) Watch() *muxWatcher {
	mw := &muxWatcher{
		result: make(chan struct{}),
		m:      m,
		id:     int64(len(m.watchers)),
	}
	m.globalMtx.Lock()
	m.watchers[mw.id] = mw
	m.globalMtx.Unlock()
	return mw
}

func (m *Mux) loop() {
	for i := 0; i < 3; i++ {
		m.distribute()
	}
}

func (m *Mux) distribute() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.globalMtx.Lock()
	for _, w := range m.watchers {
		w.result <- struct{}{}
	}
	m.globalMtx.Unlock()
}

func (m *Mux) stopWatching(id int64) {
	m.lock.Lock()
	defer m.lock.Unlock()
	w, ok := m.watchers[id]
	if !ok {
		return
	}
	delete(m.watchers, id)
	close(w.result)
}

// Workload is the raw test function for benchmark use.
func Workload(t *testing.T) {
	go func() {
		m := newMux()
		w := m.Watch()
		w.Stop()
	}()
}
