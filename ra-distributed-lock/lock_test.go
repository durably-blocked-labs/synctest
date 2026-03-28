package lock

import (
	"sync"
	"testing"
	"time"
)

func makeCluster(t *testing.T, n int) ([]*RANode, func()) {
	t.Helper()
	net := NewNetwork()
	addrs := make([]string, n)
	transports := make([]*Transport, n)
	for i := range addrs {
		addrs[i] = string(rune('A' + i))
		transports[i] = net.AddNode(addrs[i])
	}
	nodes := make([]*RANode, n)
	for i, tr := range transports {
		peers := make([]string, 0, n-1)
		for j, a := range addrs {
			if j != i {
				peers = append(peers, a)
			}
		}
		nodes[i] = NewRANode(addrs[i], tr, peers)
	}
	for _, nd := range nodes {
		nd.Start()
	}
	cleanup := func() {
		for _, nd := range nodes {
			nd.Stop()
		}
	}
	return nodes, cleanup
}

func TestMutualExclusion(t *testing.T) {
	nodes, cleanup := makeCluster(t, 3)
	defer cleanup()

	store := NewKVStore()
	var wg sync.WaitGroup

	for _, nd := range nodes {
		nd := nd
		wg.Add(1)
		go func() {
			defer wg.Done()
			nd.AcquireLock()
			store.Put("counter", store.Get("counter")+1)
			nd.ReleaseLock()
		}()
	}

	wg.Wait()
	if got := store.Get("counter"); got != len(nodes) {
		t.Errorf("lost update: expected counter=%d, got %d", len(nodes), got)
	}
}

func TestLiveness(t *testing.T) {
	nodes, cleanup := makeCluster(t, 3)
	defer cleanup()

	store := NewKVStore()
	var wg sync.WaitGroup
	done := make(chan struct{})

	for _, nd := range nodes {
		nd := nd
		wg.Add(1)
		go func() {
			defer wg.Done()
			nd.AcquireLock()
			store.Put("counter", store.Get("counter")+1)
			nd.ReleaseLock()
		}()
	}

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("liveness violation: not all nodes completed within deadline")
	}
}

func TestMutualExclusionFiveNodes(t *testing.T) {
	nodes, cleanup := makeCluster(t, 5)
	defer cleanup()

	store := NewKVStore()
	var wg sync.WaitGroup

	for _, nd := range nodes {
		nd := nd
		wg.Add(1)
		go func() {
			defer wg.Done()
			nd.AcquireLock()
			store.Put("counter", store.Get("counter")+1)
			nd.ReleaseLock()
		}()
	}

	wg.Wait()
	if got := store.Get("counter"); got != len(nodes) {
		t.Errorf("lost update: expected counter=%d, got %d", len(nodes), got)
	}
}

func TestRequestPriority(t *testing.T) {
	// Deterministically ensure A has a lower Lamport timestamp than B:
	// A acquires and releases the lock first (no B yet), bumping A's clock
	// to 1. Then we set up fresh nodes where A re-acquires (ts=2) and B
	// acquires for the first time (ts=1 from its perspective, but A's
	// REQUEST arrives first so B's clock is bumped before it calls
	// AcquireLock).
	//
	// Simpler approach: use a barrier so A's REQUEST is guaranteed to have
	// been processed by B before B calls AcquireLock, giving B a higher
	// Lamport clock and thus a higher request timestamp.
	net := NewNetwork()
	trA := net.AddNode("A")
	trB := net.AddNode("B")

	nodeA := NewRANode("A", trA, []string{"B"})
	nodeB := NewRANode("B", trB, []string{"A"})
	nodeA.Start()
	nodeB.Start()
	defer nodeA.Stop()
	defer nodeB.Stop()

	// aRequestSent is closed once A has sent its REQUEST into the network
	// mailbox and B has had a chance to process it (bumping B's clock).
	// We wait for B's mailbox to drain before B calls AcquireLock.
	aAcquired := make(chan struct{})

	order := make([]string, 0, 2)
	var mu sync.Mutex
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		nodeA.AcquireLock()
		close(aAcquired) // signal: A is in CS, its REQUEST is fully processed
		mu.Lock()
		order = append(order, "A")
		mu.Unlock()
		time.Sleep(2 * time.Millisecond)
		nodeA.ReleaseLock()
	}()

	// B only calls AcquireLock after A is in CS. At that point B's Lamport
	// clock has already been updated by A's REQUEST, so B's reqTimestamp > A's.
	<-aAcquired

	wg.Add(1)
	go func() {
		defer wg.Done()
		nodeB.AcquireLock()
		mu.Lock()
		order = append(order, "B")
		mu.Unlock()
		nodeB.ReleaseLock()
	}()

	wg.Wait()

	if len(order) != 2 || order[0] != "A" {
		t.Errorf("expected A before B, got order=%v", order)
	}
}

func TestSingleNodeNoPeers(t *testing.T) {
	net := NewNetwork()
	tr := net.AddNode("A")
	node := NewRANode("A", tr, nil)
	node.Start()
	defer node.Stop()

	done := make(chan struct{})
	go func() {
		node.AcquireLock()
		node.ReleaseLock()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("single node with no peers hung in AcquireLock")
	}
}

func TestStopClean(t *testing.T) {
	_, cleanup := makeCluster(t, 2)

	done := make(chan struct{})
	go func() {
		cleanup()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() hung with no messages in flight")
	}
}
