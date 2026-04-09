package lock

// Bug demo tests for the Ricart-Agrawala lock implementation.
//
// Apply the mutation described above each test to lock.go, run that test,
// then revert. The tests are designed to run against the mutated code.
//
// Detection mechanism
// -------------------
// The critical section body is a distributed read-modify-write against a KV
// server node. Each node in the CS:
//
//   1. Sends a KVGet RPC to the KV server and waits for the reply.   ← interleaving point
//   2. Sends a KVPut RPC with val+1 and waits for the ack.           ← interleaving point
//
// The KV server is a real orchestrator node — its messages appear in the
// delivery queue alongside RA protocol messages, and Explore can reorder
// them. Two detection layers are used:
//
//   Primary:   inCS atomic.Int32 incremented at CS entry, decremented at exit.
//              If two nodes are in the CS simultaneously, the second increment
//              produces a value > 1 and t.Errorf fires immediately.
//
//   Secondary: KV counter check at shutdown. If both nodes read the same stale
//              value before either writes, both write the same value and one
//              increment is lost → counter < expected → t.Errorf.
//
// KV replies flow through the orchestrator's transport (not a side channel)
// so Resume is delivered to the RA node's bubble on each reply. A dispatcher
// goroutine inside each RA bubble routes KV replies to kvMailbox and RA
// protocol messages to raMailbox; csViaRPC blocks on kvMailbox (a durable
// in-bubble channel receive) while the RA handler goroutine reads raMailbox.

import (
	"sync/atomic"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

// dispatchTransport wraps OrchestratorTransport and overrides Mailbox() to
// return a filtered channel containing only RA protocol messages. KV reply
// messages are routed to kvMailbox by a dispatcher goroutine started inside
// the bubble. This lets RANode.Start() and csViaRPC read from separate
// channels without racing on the underlying mailbox.
type dispatchTransport struct {
	*OrchestratorTransport
	raMailbox chan Message
	kvMailbox chan Message
}

func (d *dispatchTransport) Mailbox() <-chan Message { return d.raMailbox }

// setupKVCluster builds RA transports (connected to each other and to the
// KV server) with bidirectional connections so the KV server can send
// replies back to RA nodes via the orchestrator transport.
func setupKVCluster(addrs []string) (
	transports map[string]*OrchestratorTransport,
	kvTr *OrchestratorTransport,
) {
	kvTr = NewOrchestratorTransport("kv")
	transports = make(map[string]*OrchestratorTransport, len(addrs))

	for _, addr := range addrs {
		tr := NewOrchestratorTransport(addr)
		transports[addr] = tr
		tr.Connect(kvTr) // RA node → KV server
		kvTr.Connect(tr) // KV server → RA node (for replies)
	}
	for _, tr := range transports {
		for _, peer := range transports {
			if tr != peer {
				tr.Connect(peer)
			}
		}
	}
	return
}

// addKVServer registers the KV server as an orchestrator node.
// It processes MsgKVGet, MsgKVPut, and MsgKVDone messages, sending
// MsgKVReply responses back through the transport so the orchestrator
// delivers them and resumes the target RA bubble.
func addKVServer(
	orch *orchestrator.Orchestrator,
	kvTr *OrchestratorTransport,
	expectedCounter int,
	totalDone int,
) {
	orch.AddNode(kvTr, func(t *testing.T) {
		kvTr.StartBridge()
		store := NewKVStore()
		done := 0
		for {
			msg := <-kvTr.Mailbox()
			switch msg.Kind {
			case MsgKVGet:
				kvTr.Send(msg.From, Message{Kind: MsgKVReply, From: "kv", Value: store.Get(msg.Key)})
			case MsgKVPut:
				store.Put(msg.Key, msg.Value)
				kvTr.Send(msg.From, Message{Kind: MsgKVReply, From: "kv", Value: 0}) // ack
			case MsgKVDone:
				done++
				if done == totalDone {
					if got := store.Get("counter"); got != expectedCounter {
						t.Errorf("lost update: counter=%d, want %d — mutual exclusion violated", got, expectedCounter)
					}
					kvTr.Close()
					return
				}
			}
		}
	})
}

// csViaRPC executes one critical-section acquisition using KV RPCs.
// It reads replies from kvMailbox, which is a durable in-bubble channel
// populated by the dispatcher goroutine when MsgKVReply messages arrive.
// The two channel reads are the interleaving points the orchestrator exploits.
//
// inCS is an optional atomic counter tracking simultaneous CS occupants.
// When non-nil, a value > 1 after increment means mutual exclusion is violated
// and is reported immediately — without waiting for the KV counter mismatch.
func csViaRPC(t *testing.T, addr string, tr *OrchestratorTransport, kvMailbox <-chan Message, inCS *atomic.Int32) {
	t.Helper()

	if inCS != nil {
		if v := inCS.Add(1); v > 1 {
			t.Errorf("mutual exclusion violated on %s: %d nodes in CS simultaneously", addr, v)
		}
	}

	// Get: send request, wait for reply (durable block on in-bubble channel).
	tr.Send("kv", Message{Kind: MsgKVGet, From: addr, Key: "counter"})
	reply := <-kvMailbox

	// Put: send val+1, wait for ack (durable block on in-bubble channel).
	tr.Send("kv", Message{Kind: MsgKVPut, From: addr, Key: "counter", Value: reply.Value + 1})
	<-kvMailbox

	if inCS != nil {
		inCS.Add(-1)
	}
}

// addRANodes registers nodes that each acquire the lock n times.
// rounds maps addr → number of acquisitions. inCS is an optional shared
// atomic counter passed to csViaRPC for immediate mutual-exclusion detection.
func addRANodes(
	orch *orchestrator.Orchestrator,
	addrs []string,
	transports map[string]*OrchestratorTransport,
	rounds map[string]int,
	inCS *atomic.Int32,
) {
	for i, addr := range addrs {
		addr := addr
		tr := transports[addr]
		n := rounds[addr]

		peers := make([]string, 0, len(addrs)-1)
		for j, a := range addrs {
			if j != i {
				peers = append(peers, a)
			}
		}

		orch.AddNode(tr, func(t *testing.T) {
			tr.StartBridge()

			// Dispatcher routes KV replies to kvMailbox and RA messages to
			// raMailbox. It exits when tr.Close() closes internalMailbox.
			raMailbox := make(chan Message, 64)
			kvMailbox := make(chan Message, 4)
			dt := &dispatchTransport{tr, raMailbox, kvMailbox}

			go func() {
				for msg := range tr.Mailbox() {
					if msg.Kind == MsgKVReply {
						kvMailbox <- msg
					} else {
						raMailbox <- msg
					}
				}
				close(raMailbox)
				close(kvMailbox)
			}()

			node := NewRANode(addr, dt, peers)
			node.Start()

			for range n {
				node.AcquireLock()
				csViaRPC(t, addr, tr, kvMailbox, inCS)
				node.ReleaseLock()
			}

			tr.Send("kv", Message{Kind: MsgKVDone, From: addr})
			node.Stop()
			tr.Close()
		})
	}
}

// singleRound returns a rounds map where every addr acquires the lock once.
func singleRound(addrs []string) map[string]int {
	m := make(map[string]int, len(addrs))
	for _, a := range addrs {
		m[a] = 1
	}
	return m
}

// =============================================================================
// Bug 1 — Stale Clock in shouldDefer (FIFO-latent)
//
// TWO-PART MUTATION to lock.go:
//
//   Change 1 — move the Lamport clock update inside case MsgReply only:
//     Before: clock updated at top of handleMessage (for all message kinds)
//     After:  clock updated only inside case MsgReply
//
//   Change 2 — use n.timestamp instead of n.reqTimestamp in shouldDefer:
//     Before: (n.state == Wanted && (n.reqTimestamp < msg.Timestamp || ...))
//     After:  (n.state == Wanted && (n.timestamp  < msg.Timestamp || ...))
//
// Under FIFO, no REPLY arrives before its corresponding REQUEST, so when
// processing a REQUEST n.timestamp has never been incremented (clock update
// is gated on MsgReply) → n.timestamp == n.reqTimestamp → bug is invisible.
//
// With two nodes the bug cannot fire: receiving the only peer's REPLY
// immediately transitions the node to Held, where the existing Held guard
// handles any incoming REQUEST correctly. Three nodes are required so a node
// can receive one REPLY (bumping its clock) while still in Wanted state
// (waiting for the second REPLY).
//
// Explore finds an ordering where C_REPLY→B is delivered before C_REQ→B.
// B (still Wanted, waiting for A's reply) now has B.timestamp=2 > C.reqTs=1.
// When C_REQ→B arrives, shouldDefer=false → B prematurely replies to C.
// After A releases and sends deferred replies, B and C both collect two
// replies and enter Held simultaneously → mutual exclusion violated.
// =============================================================================

func TestStaleClockBug_FIFOPassesBugLatent(t *testing.T) {
	addrs := []string{"A", "B", "C"}
	transports, kvTr := setupKVCluster(addrs)
	rounds := singleRound(addrs)

	orch := orchestrator.New()
	addKVServer(orch, kvTr, len(addrs), len(addrs))
	addRANodes(orch, addrs, transports, rounds, nil)

	_, ok := orch.Run(t)
	if !ok {
		t.Fatal("FIFO run failed — bug should be latent under FIFO delivery")
	}
	t.Log("FIFO delivery: PASSED (bug is latent — only triggers under reordered delivery)")
}

func TestStaleClockBug_ExploreFindsViolation(t *testing.T) {
	addrs := []string{"A", "B", "C"}

	orch := orchestrator.New()
	ok := orch.Explore(t, func(o *orchestrator.Orchestrator) {
		transports, kvTr := setupKVCluster(addrs)
		rounds := singleRound(addrs)
		var inCS atomic.Int32
		addRANodes(o, addrs, transports, rounds, &inCS)
		addKVServer(o, kvTr, len(addrs), len(addrs))
	}, orchestrator.GlobalBound(2), orchestrator.GlobalMaxRuns(200),
		orchestrator.BoundFromEnv(), orchestrator.MaxRunsFromEnv(),
		orchestrator.ObserverFromEnv(t))

	if ok {
		t.Fatal("Explore found no violation — bug was not triggered")
	}
	t.Log("Explore found a lost update (stale-clock bug confirmed)")
}

// =============================================================================
// Bug 2 — Missing `state == Held` Guard (FIFO-latent)
//
// MUTATION (lock.go): remove the n.state == Held || clause from shouldDefer
//
//   Before:
//     shouldDefer := n.state == Held ||
//         (n.state == Wanted && (n.reqTimestamp < msg.Timestamp || ...))
//   After:
//     shouldDefer := n.state == Wanted &&
//         (n.reqTimestamp < msg.Timestamp || ...)
//
// Under FIFO, all nodes send their REQUESTs before any message is delivered.
// By the time a node enters Held, every peer's REQUEST has already been
// processed while it was in Wanted state (where the bug doesn't fire). No
// REQUEST ever reaches a Held node under FIFO → bug is latent.
//
// With two nodes A and B: Explore finds an ordering where B's REQUEST reaches
// A after A has entered Held (waiting for its KVGet reply). A replies
// immediately instead of deferring. B collects A's reply, enters Held
// simultaneously with A → lost update detected.
// =============================================================================

func TestNoHeldCheckBug_FIFOPassesBugLatent(t *testing.T) {
	addrs := []string{"A", "B"}
	transports, kvTr := setupKVCluster(addrs)
	rounds := singleRound(addrs)

	orch := orchestrator.New()
	addKVServer(orch, kvTr, len(addrs), len(addrs))
	addRANodes(orch, addrs, transports, rounds, nil)

	_, ok := orch.Run(t)
	if !ok {
		t.Fatal("FIFO run failed — bug should be latent under FIFO delivery")
	}
	t.Log("FIFO delivery: PASSED (bug is latent — only triggers under reordered delivery)")
}

func TestNoHeldCheckBug_ExploreFindsViolation(t *testing.T) {
	addrs := []string{"A", "B"}

	orch := orchestrator.New()
	ok := orch.Explore(t, func(o *orchestrator.Orchestrator) {
		transports, kvTr := setupKVCluster(addrs)
		rounds := singleRound(addrs)
		addKVServer(o, kvTr, len(addrs), len(addrs))
		var inCS atomic.Int32
		addRANodes(o, addrs, transports, rounds, &inCS)
	}, orchestrator.GlobalBound(2), orchestrator.GlobalMaxRuns(100))

	if ok {
		t.Fatal("Explore found no violation — bug was not triggered")
	}
	t.Log("Explore found a lost update (missing-Held-guard bug confirmed)")
}
