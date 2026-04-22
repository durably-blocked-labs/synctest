// grpc#1460 — Keepalive Mutex + Channel Deadlock (2 goroutines)
// https://github.com/grpc/grpc-go/pull/1460
//
// G1 (keepalive) holds mu and blocks on <-awakenKeepalive.
// G2 (NewStream) tries to acquire mu to send on awakenKeepalive.
// Deadlock: G1 holds lock + waits on channel, G2 needs lock to signal channel.
// Flaky: 100/100
package grpc1460

import (
	"sync"
	"testing"
)

type Stream struct{}

type http2Client struct {
	mu              sync.Mutex
	awakenKeepalive chan struct{}
	activeStream    []*Stream
}

func (t *http2Client) keepalive() {
	t.mu.Lock()
	if len(t.activeStream) < 1 {
		<-t.awakenKeepalive
		t.mu.Unlock()
	} else {
		t.mu.Unlock()
	}
}

func (t *http2Client) NewStream() {
	t.mu.Lock()
	t.activeStream = append(t.activeStream, &Stream{})
	if len(t.activeStream) == 1 {
		select {
		case t.awakenKeepalive <- struct{}{}:
		default:
		}
	}
	t.mu.Unlock()
}

// Workload is the raw test function for benchmark use.
func Workload(t *testing.T) {
	client := &http2Client{
		awakenKeepalive: make(chan struct{}),
	}
	go client.keepalive()
	go client.NewStream()
}
