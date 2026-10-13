// istio#16224 — Channel + Mutex Deadlock (3 goroutines)
// https://github.com/istio/istio/issues/16224
//
// Handler (called by Run goroutine) holds lock and blocks sending on done channel.
// Main goroutine tries to acquire lock (which handler holds).
// Deadlock: handler holds lock + blocked on channel, main blocked on lock.
package istio16224

import (
	"sync"
	"testing"
)

type Event int

type Handler func(Event)

type configstoreMonitor struct {
	handlers []Handler
	eventCh  chan Event
}

func (m *configstoreMonitor) Run(stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			if _, ok := <-m.eventCh; ok {
				close(m.eventCh)
			}
			return
		case ce, ok := <-m.eventCh:
			if ok {
				m.applyHandlers(ce)
			}
		}
	}
}

func (m *configstoreMonitor) applyHandlers(e Event) {
	for _, f := range m.handlers {
		f(e)
	}
}

func (m *configstoreMonitor) AppendEventHandler(h Handler) {
	m.handlers = append(m.handlers, h)
}

func (m *configstoreMonitor) ScheduleProcessEvent(configEvent Event) {
	m.eventCh <- configEvent
}

type controller struct {
	monitor *configstoreMonitor
}

func (c *controller) RegisterEventHandler(f func(Event)) {
	c.monitor.AppendEventHandler(f)
}

func (c *controller) Run(stop <-chan struct{}) {
	c.monitor.Run(stop)
}

func (c *controller) Create() {
	c.monitor.ScheduleProcessEvent(Event(0))
}

// Workload is the raw test function for benchmark use.
func Workload(t *testing.T) {
	ctrl := &controller{
		monitor: &configstoreMonitor{
			eventCh: make(chan Event),
		},
	}
	done := make(chan bool)
	lock := sync.Mutex{}
	ctrl.RegisterEventHandler(func(event Event) {
		lock.Lock()
		defer lock.Unlock()
		done <- true
	})

	stop := make(chan struct{})
	go ctrl.Run(stop)

	ctrl.Create()

	lock.Lock()
	lock.Unlock()
	<-done

	close(stop)
}
