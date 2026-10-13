// moby#33781 — Channel Goroutine Leak (3 goroutines)
// https://github.com/moby/moby/pull/33781
//
// When <-stop fires, function returns without draining unbuffered results.
// G3 permanently blocked trying to send on results.
// Fix: drain results on the stop path.
package moby33781

import (
	"context"
	"testing"
	"time"
)

func monitor(stop chan bool) {
	probeInterval := 50 * time.Nanosecond
	probeTimeout := 50 * time.Nanosecond
	for {
		select {
		case <-stop:
			return
		case <-time.After(probeInterval):
			results := make(chan bool)
			ctx, cancelProbe := context.WithTimeout(context.Background(), probeTimeout)
			go func() {
				results <- true
				close(results)
			}()
			select {
			case <-stop:
				// BUG: results should be drained here
				cancelProbe()
				return
			case <-results:
				cancelProbe()
			case <-ctx.Done():
				cancelProbe()
				<-results
			}
		}
	}
}

// Workload is the raw test function for benchmark use.
func Workload(t *testing.T) {
	stop := make(chan bool)
	go monitor(stop)
	go func() {
		time.Sleep(50 * time.Nanosecond)
		stop <- true
	}()
}
