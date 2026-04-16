// kubernetes#6632 — Mutex-Channel Deadlock (3 goroutines)
// https://github.com/kubernetes/kubernetes/pull/6632
//
// WriteFrame() holds writeLock, blocks sending on unbuffered resetChan.
// monitor() needs resetChan but must acquire writeLock first.
// Fix: create a goroutine to drain the channel.
package k8s6632

import (
	"sync"
	"testing"
)

type Connection struct {
	closeChan chan bool
}

type idleAwareFramer struct {
	resetChan chan bool
	writeLock sync.Mutex
	conn      *Connection
}

func (i *idleAwareFramer) monitor() {
	var resetChan = i.resetChan
Loop:
	for {
		select {
		case <-i.conn.closeChan:
			i.writeLock.Lock()
			close(resetChan)
			i.resetChan = nil
			i.writeLock.Unlock()
			break Loop
		}
	}
}

func (i *idleAwareFramer) WriteFrame() {
	i.writeLock.Lock()
	defer i.writeLock.Unlock()
	if i.resetChan == nil {
		return
	}
	i.resetChan <- true
}

func newIdleAwareFramer() *idleAwareFramer {
	return &idleAwareFramer{
		resetChan: make(chan bool),
		conn: &Connection{
			closeChan: make(chan bool),
		},
	}
}

// Workload is the raw test function for benchmark use.
func Workload(t *testing.T) {
	i := newIdleAwareFramer()
	go func() { i.conn.closeChan <- true }()
	go i.monitor()
	go i.WriteFrame()
}
