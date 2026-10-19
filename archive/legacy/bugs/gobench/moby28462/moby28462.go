// moby#28462 — Embedded Mutex + Channel Deadlock (2 goroutines)
// https://github.com/moby/moby/pull/28462
//
// G1 (monitor) loops: tries to acquire Container lock in handleProbeResult.
// G2 (StateChanged) acquires Container lock, sends on stop channel.
// Deadlock: G1 blocked on lock held by G2, G2 blocked sending on channel G1 reads.
// Flaky: 69/100
package moby28462

import (
	"sync"
	"testing"
)

type State struct {
	Health *Health
}

type Container struct {
	sync.Mutex
	State *State
}

type Store struct {
	ctr *Container
}

func (s *Store) Get() *Container {
	return s.ctr
}

type Daemon struct {
	containers Store
}

func (d *Daemon) StateChanged() {
	c := d.containers.Get()
	c.Lock()
	d.updateHealthMonitorElseBranch(c)
	defer c.Unlock()
}

func (d *Daemon) updateHealthMonitorElseBranch(c *Container) {
	h := c.State.Health
	h.CloseMonitorChannel()
}

type Health struct {
	stop chan struct{}
}

func (s *Health) OpenMonitorChannel() chan struct{} {
	return s.stop
}

func (s *Health) CloseMonitorChannel() {
	if s.stop != nil {
		s.stop <- struct{}{}
	}
}

func monitor(c *Container, stop chan struct{}) {
	for {
		select {
		case <-stop:
			return
		default:
			handleProbeResult(c)
		}
	}
}

func handleProbeResult(c *Container) {
	c.Lock()
	defer c.Unlock()
}

func newDaemonAndContainer() (*Daemon, *Container) {
	c := &Container{
		State: &State{&Health{make(chan struct{})}},
	}
	d := &Daemon{Store{c}}
	return d, c
}

// Workload is the raw test function for benchmark use.
func Workload(t *testing.T) {
	d, c := newDaemonAndContainer()
	go monitor(c, c.State.Health.OpenMonitorChannel())
	go d.StateChanged()
}
