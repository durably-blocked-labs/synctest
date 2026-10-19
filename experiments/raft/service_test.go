package raftexperiment

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/hashicorp/raft"
)

type command struct {
	ID    string
	Value int
}
type applied struct {
	Index, Term uint64
	Command     command
	Previous    int
}
type register struct {
	mu      sync.Mutex
	value   int
	journal []applied
}

func (s *register) Apply(log *raft.Log) interface{} {
	var c command
	if err := json.Unmarshal(log.Data, &c); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	previous := s.value
	s.value = c.Value
	s.journal = append(s.journal, applied{log.Index, log.Term, c, previous})
	return previous
}

// Snapshots/recovery are deliberately excluded from this memory-only model.
func (*register) Snapshot() (raft.FSMSnapshot, error) {
	return nil, fmt.Errorf("snapshots outside experiment model")
}
func (*register) Restore(io.ReadCloser) error { return fmt.Errorf("recovery outside experiment model") }
func (s *register) observe() (int, []applied) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.value, append([]applied(nil), s.journal...)
}

type operation struct {
	ID              string
	Value, Previous int
	Invoke, Return  uint64
}

// historyLegal is independent of the FSM journal: enumerate sequential register
// swaps respecting response-before-invocation edges. Intended only for tiny histories.
func historyLegal(history []operation) bool {
	if len(history) > 6 {
		return false
	}
	ids := make(map[string]bool)
	for _, op := range history {
		if op.Invoke >= op.Return || ids[op.ID] {
			return false
		}
		ids[op.ID] = true
	}
	var visit func(uint, int) bool
	visit = func(used uint, value int) bool {
		if used == (1<<len(history))-1 {
			return true
		}
		for i, op := range history {
			if used&(1<<i) != 0 || op.Previous != value {
				continue
			}
			blocked := false
			for j, before := range history {
				if before.Return < op.Invoke && used&(1<<j) == 0 {
					blocked = true
				}
			}
			if !blocked && visit(used|(1<<i), op.Value) {
				return true
			}
		}
		return false
	}
	return visit(0, 0)
}

func TestHistoryOracle(t *testing.T) {
	good := []operation{{"a", 1, 0, 1, 4}, {"b", 2, 1, 2, 3}, {"c", 3, 2, 5, 6}}
	if !historyLegal(good) {
		t.Fatal("legal overlapping history rejected")
	}
	for _, bad := range [][]operation{
		{{"a", 1, 9, 1, 2}},                    // wrong reply
		{{"a", 1, 0, 1, 2}, {"b", 2, 0, 3, 4}}, // ignores precedence
		{{"a", 1, 0, 1, 4}, {"a", 2, 1, 2, 3}}, // duplicate identity
	} {
		if historyLegal(bad) {
			t.Fatalf("accepted invalid history: %+v", bad)
		}
	}
}

// One cluster in ONE bubble/clock, not a per-node distributed-controller/replay
// experiment. InmemTransport and stores exercise real Raft, not wire/disk fidelity.
func TestThreeVoterMemoryServiceSingleBubble(t *testing.T) {
	synctest.Test(t, runMemoryService)
}

func runMemoryService(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var nodes []*raft.Raft
	var clients sync.WaitGroup
	var transports []*raft.InmemTransport
	var machines []*register
	defer func() {
		cancel()
		// Futures are not cancellable. Shutdown releases admitted work before join.
		for _, node := range nodes {
			if err := node.Shutdown().Error(); err != nil {
				t.Errorf("shutdown: %v", err)
			}
		}
		for _, transport := range transports {
			_ = transport.Close()
		}
		clients.Wait()
	}()
	configuration := raft.Configuration{}
	for i := 0; i < 3; i++ {
		address := raft.ServerAddress(fmt.Sprintf("memory-%d", i))
		_, transport := raft.NewInmemTransport(address)
		transports = append(transports, transport)
		configuration.Servers = append(configuration.Servers, raft.Server{Suffrage: raft.Voter, ID: raft.ServerID(address), Address: address})
	}
	for i, transport := range transports {
		for j, peer := range transports {
			if i != j {
				transport.Connect(peer.LocalAddr(), peer)
			}
		}
	}
	for i, transport := range transports {
		config := raft.DefaultConfig()
		config.LocalID = configuration.Servers[i].ID
		config.HeartbeatTimeout = 100 * time.Millisecond
		config.ElectionTimeout = 100 * time.Millisecond
		config.LeaderLeaseTimeout = 100 * time.Millisecond
		config.CommitTimeout = 5 * time.Millisecond
		config.SnapshotThreshold = ^uint64(0)
		config.LogOutput = io.Discard
		store := raft.NewInmemStore()
		snapshots := raft.NewDiscardSnapshotStore()
		if err := raft.BootstrapCluster(config, store, store, snapshots, transport, configuration); err != nil {
			t.Fatal(err)
		}
		machine := &register{}
		node, err := raft.NewRaft(config, machine, store, store, snapshots, transport)
		if err != nil {
			t.Fatal(err)
		}
		nodes = append(nodes, node)
		machines = append(machines, machine)
	}
	var leader *raft.Raft
	deadline := time.Now().Add(5 * time.Second)
	for leader == nil && time.Now().Before(deadline) {
		for _, node := range nodes {
			if node.State() == raft.Leader {
				leader = node
				break
			}
		}
		if leader == nil {
			time.Sleep(10 * time.Millisecond)
		}
	}
	if leader == nil {
		t.Fatal("inconclusive: leader-election virtual-time bound exhausted")
	}
	type result struct {
		op  operation
		err error
	}
	results := make(chan result, 3)
	var clock atomic.Uint64
	submit := func(c command) {
		clients.Add(1)
		go func() {
			defer clients.Done()
			select {
			case <-ctx.Done():
				return
			default:
			}
			op := operation{ID: c.ID, Value: c.Value, Invoke: clock.Add(1)}
			payload, _ := json.Marshal(c)
			future := leader.Apply(payload, time.Second)
			err := future.Error() // Enqueue timeout is NOT a commit timeout.
			if err == nil {
				var ok bool
				op.Previous, ok = future.Response().(int)
				if !ok {
					err = fmt.Errorf("unexpected FSM response: %v", future.Response())
				}
			}
			op.Return = clock.Add(1)
			results <- result{op, err}
		}()
	}
	var history []operation
	receive := func() {
		select {
		case result := <-results:
			if result.err != nil {
				t.Fatalf("inconclusive: Apply failed/uncertain, not a no-op: %v", result.err)
			}
			history = append(history, result.op)
		case <-time.After(5 * time.Second):
			t.Fatal("inconclusive: future completion bound exhausted")
		}
	}
	submit(command{"a", 1})
	submit(command{"b", 2})
	receive()
	receive()
	submit(command{"c", 3})
	receive()
	if !historyLegal(history) {
		t.Fatalf("illegal register history: %+v", history)
	}
	deadline = time.Now().Add(5 * time.Second)
	for {
		caughtUp := true
		for _, machine := range machines {
			_, journal := machine.observe()
			caughtUp = caughtUp && len(journal) == 3
		}
		if caughtUp {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("inconclusive: follower catch-up bound exhausted")
		}
		time.Sleep(10 * time.Millisecond)
	}
	value, reference := machines[0].observe()
	if value != 3 {
		t.Fatalf("final value %d", value)
	}
	seen := make(map[string]bool)
	for i, entry := range reference {
		if entry.Term == 0 || (i > 0 && entry.Index <= reference[i-1].Index) || seen[entry.Command.ID] {
			t.Fatalf("invalid applied journal: %+v", reference)
		}
		seen[entry.Command.ID] = true
		found := false
		for _, op := range history {
			if op.ID == entry.Command.ID && op.Value == entry.Command.Value && op.Previous == entry.Previous {
				found = true
			}
		}
		if !found {
			t.Fatalf("acknowledgement/journal mismatch: %+v", entry)
		}
	}
	for i, machine := range machines {
		got, journal := machine.observe()
		if got != value || !reflect.DeepEqual(journal, reference) {
			t.Fatalf("node %d journal disagreement", i)
		}
	}
	t.Logf("real Apply/replication/commit/FSM/futures: history=%+v journal=%+v", history, reference)
}
