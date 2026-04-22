# Quorum Read Repair Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `bugs/quorum-read-repair`, a Dynamo-style quorum KV bug where read repair can drop a concurrent sibling only under mixed global and local interleavings.

**Architecture:** Keep the package isolated under `bugs/quorum-read-repair` and follow the existing `bugs/ra-*` pattern: package-local transport, scenario builder, correctness tests, benchmark tests, and `BUG.md`. Replicas run a router plus separate normal-write and repair apply loops; the deliberate bug exists only in the repair merge path.

**Tech Stack:** Go, custom repo Go toolchain `./go/bin/go`, `testing/synctest`, `github.com/shubhaankar/synctest/orchestrator`.

---

## File Structure

- Create `bugs/quorum-read-repair/transport.go`
  - Message enum, `Message`, and `OrchestratorTransport`.
  - Uses `synctest.ExternalWait` and `orchestrator.PendingOp`.
  - No sleep, timer delay, duplication, or synthetic failure injection.
- Create `bugs/quorum-read-repair/store.go`
  - Vector-clock types and merge helpers.
  - `Replica` with router, `applyLoop`, `repairLoop`, and debug snapshot helpers.
  - Correct merge for normal writes; deliberately buggy single-winner merge for repair.
- Create `bugs/quorum-read-repair/client.go`
  - Quorum client with `Put`, `GetAndRepair`, quorum response collection, and repair fanout.
- Create `bugs/quorum-read-repair/scenario.go`
  - Cluster setup, replica/client wiring, workload construction, invariant checking, and observed-outcome helpers.
- Create `bugs/quorum-read-repair/bug_demo_test.go`
  - FIFO, global-only, global+local, and targeted `FindBug` tests.
- Create `bugs/quorum-read-repair/bench_test.go`
  - CHESS global-only, CHESS global+local, PCT depth 2, PCT depth 3, and Random benchmarks.
- Create `bugs/quorum-read-repair/BUG.md`
  - Explain protocol, bug, required mixed interleaving, and no-sleep constraint.

---

### Task 1: Transport And Message Skeleton

**Files:**
- Create: `bugs/quorum-read-repair/transport.go`
- Test: `bugs/quorum-read-repair/transport_test.go`

- [ ] **Step 1: Write the failing transport smoke test**

Create `bugs/quorum-read-repair/transport_test.go`:

```go
package quorumreadrepair

import (
	"runtime"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

func TestTransportSmoke_FIFODeliversPut(t *testing.T) {
	runtime.GOMAXPROCS(8)

	a := NewOrchestratorTransport("A")
	b := NewOrchestratorTransport("B")
	a.Connect(b)
	b.Connect(a)

	orch := orchestrator.New()
	orch.AddNode(a, func(t *testing.T) {
		a.StartBridge()
		a.Send("B", Message{Kind: MsgPut, From: "A", Key: "x", Values: []VersionedValue{{Value: "A", Clock: Clock{"C1": 1}}}})
		a.Shutdown()
	})
	orch.AddNode(b, func(t *testing.T) {
		b.StartBridge()
		msg := <-b.Mailbox()
		if msg.Kind != MsgPut || msg.From != "A" || msg.Key != "x" {
			t.Fatalf("delivered message = %#v", msg)
		}
		b.Shutdown()
	})

	if _, ok := orch.Run(t); !ok {
		t.Fatal("transport smoke run failed")
	}
}
```

- [ ] **Step 2: Run the failing test**

Run:

```bash
./go/bin/go test -count=1 -run TestTransportSmoke_FIFODeliversPut ./bugs/quorum-read-repair
```

Expected: compile failure because `NewOrchestratorTransport`, `Message`, `MsgPut`, `VersionedValue`, and `Clock` do not exist.

- [ ] **Step 3: Implement the transport**

Create `bugs/quorum-read-repair/transport.go`:

```go
package quorumreadrepair

import (
	"testing/synctest"

	"github.com/shubhaankar/synctest/orchestrator"
)

type MsgKind int

const (
	MsgPut MsgKind = iota
	MsgPutAck
	MsgGet
	MsgGetResp
	MsgRepair
	MsgRepairAck
	MsgControl
)

type Clock map[string]int

type VersionedValue struct {
	Value string
	Clock Clock
}

type Message struct {
	Kind      MsgKind
	From      string
	RequestID string
	Key       string
	Values    []VersionedValue
	Control   string
}

type OrchestratorTransport struct {
	addr    string
	outbox  chan *orchestrator.PendingOp
	mailbox chan Message
	closeCh chan struct{}
	peers   map[string]*OrchestratorTransport

	internalMailbox chan Message
	internalControl chan Message
	bridgeDone      chan struct{}
}

func NewOrchestratorTransport(addr string) *OrchestratorTransport {
	return &OrchestratorTransport{
		addr:    addr,
		outbox:  make(chan *orchestrator.PendingOp, 128),
		mailbox: make(chan Message, 128),
		closeCh: make(chan struct{}),
		peers:   make(map[string]*OrchestratorTransport),
	}
}

func (t *OrchestratorTransport) Connect(peer *OrchestratorTransport) {
	t.peers[peer.addr] = peer
}

func (t *OrchestratorTransport) Addr() string                           { return t.addr }
func (t *OrchestratorTransport) Outbox() <-chan *orchestrator.PendingOp { return t.outbox }
func (t *OrchestratorTransport) Mailbox() <-chan Message                { return t.internalMailbox }
func (t *OrchestratorTransport) ControlMailbox() <-chan Message         { return t.internalControl }

func (t *OrchestratorTransport) StartBridge() {
	t.internalMailbox = make(chan Message, 128)
	t.internalControl = make(chan Message, 32)
	t.bridgeDone = make(chan struct{})

	go func() {
		defer close(t.bridgeDone)
		defer close(t.internalMailbox)
		defer close(t.internalControl)
		for {
			var msg Message
			var closed bool
			synctest.ExternalWait(func() {
				select {
				case msg = <-t.mailbox:
				case <-t.closeCh:
					closed = true
				}
			})
			if closed {
				return
			}
			switch msg.Kind {
			case MsgControl:
				select {
				case t.internalControl <- msg:
				case <-t.closeCh:
					return
				}
			default:
				select {
				case t.internalMailbox <- msg:
				case <-t.closeCh:
					return
				}
			}
		}
	}()
}

func (t *OrchestratorTransport) Send(to string, msg Message) {
	peer, ok := t.peers[to]
	if !ok {
		return
	}
	if msg.From == "" {
		msg.From = t.addr
	}
	synctest.ExternalWait(func() {
		select {
		case t.outbox <- &orchestrator.PendingOp{
			Dir:  orchestrator.OpSend,
			From: t.addr,
			To:   to,
			Type: msgKindName(msg.Kind),
			Execute: func() {
				select {
				case peer.mailbox <- cloneMessage(msg):
				case <-peer.closeCh:
				}
			},
		}:
		case <-t.closeCh:
		}
	})
}

func (t *OrchestratorTransport) SendControl(to, label string) {
	t.Send(to, Message{Kind: MsgControl, Control: label})
}

func (t *OrchestratorTransport) WaitControl(label string) {
	for msg := range t.ControlMailbox() {
		if msg.Control == label {
			return
		}
	}
}

func (t *OrchestratorTransport) Shutdown() {
	select {
	case <-t.closeCh:
	default:
		close(t.closeCh)
	}
}

func (t *OrchestratorTransport) Close() {
	t.Shutdown()
	if t.bridgeDone != nil {
		<-t.bridgeDone
	}
}

func cloneMessage(msg Message) Message {
	out := msg
	out.Values = cloneValues(msg.Values)
	return out
}

func cloneValues(values []VersionedValue) []VersionedValue {
	if len(values) == 0 {
		return nil
	}
	out := make([]VersionedValue, len(values))
	for i, v := range values {
		out[i] = VersionedValue{Value: v.Value, Clock: cloneClock(v.Clock)}
	}
	return out
}

func cloneClock(c Clock) Clock {
	if len(c) == 0 {
		return nil
	}
	out := make(Clock, len(c))
	for k, v := range c {
		out[k] = v
	}
	return out
}

func msgKindName(k MsgKind) string {
	switch k {
	case MsgPut:
		return "Put"
	case MsgPutAck:
		return "PutAck"
	case MsgGet:
		return "Get"
	case MsgGetResp:
		return "GetResp"
	case MsgRepair:
		return "Repair"
	case MsgRepairAck:
		return "RepairAck"
	case MsgControl:
		return "Control"
	default:
		return "Unknown"
	}
}
```

- [ ] **Step 4: Run the smoke test**

Run:

```bash
./go/bin/go test -count=1 -run TestTransportSmoke_FIFODeliversPut ./bugs/quorum-read-repair
```

Expected: `ok`.

- [ ] **Step 5: Commit**

Run:

```bash
git add bugs/quorum-read-repair/transport.go bugs/quorum-read-repair/transport_test.go
git commit -m "Add quorum read repair transport"
```

---

### Task 2: Vector Clock And Merge Semantics

**Files:**
- Modify: `bugs/quorum-read-repair/store.go`
- Test: `bugs/quorum-read-repair/store_test.go`

- [ ] **Step 1: Write failing merge tests**

Create `bugs/quorum-read-repair/store_test.go`:

```go
package quorumreadrepair

import "testing"

func TestCorrectMergePreservesConcurrentSiblings(t *testing.T) {
	got := mergeSiblings(
		[]VersionedValue{{Value: "A", Clock: Clock{"C1": 1}}},
		[]VersionedValue{{Value: "B", Clock: Clock{"C2": 1}}},
	)
	assertValues(t, got, "A", "B")
}

func TestCorrectMergeDropsDominatedValue(t *testing.T) {
	got := mergeSiblings(
		[]VersionedValue{{Value: "old", Clock: Clock{"C1": 1}}},
		[]VersionedValue{{Value: "new", Clock: Clock{"C1": 2}}},
	)
	assertValues(t, got, "new")
}

func TestBuggyRepairMergeDropsConcurrentSibling(t *testing.T) {
	got := buggyRepairMerge(
		[]VersionedValue{{Value: "A", Clock: Clock{"C1": 1}}},
		[]VersionedValue{{Value: "B", Clock: Clock{"C2": 1}}},
	)
	if len(got) != 1 {
		t.Fatalf("buggyRepairMerge kept %d values, want exactly 1 to model the bug: %#v", len(got), got)
	}
}
```

- [ ] **Step 2: Run the failing merge tests**

Run:

```bash
./go/bin/go test -count=1 -run 'TestCorrectMerge|TestBuggyRepairMerge' ./bugs/quorum-read-repair
```

Expected: compile failure because merge helpers do not exist.

- [ ] **Step 3: Implement merge helpers and test assertion helper**

Create `bugs/quorum-read-repair/store.go`:

```go
package quorumreadrepair

import (
	"sort"
	"sync"
)

type Replica struct {
	addr string
	tr   *OrchestratorTransport

	applyCh  chan applyReq
	repairCh chan applyReq
	closeCh  chan struct{}

	mu    sync.Mutex
	store map[string][]VersionedValue
}

type applyReq struct {
	msg Message
	ack string
}

func compareClock(a, b Clock) int {
	aGreater := false
	bGreater := false
	keys := make(map[string]struct{}, len(a)+len(b))
	for k := range a {
		keys[k] = struct{}{}
	}
	for k := range b {
		keys[k] = struct{}{}
	}
	for k := range keys {
		av := a[k]
		bv := b[k]
		if av > bv {
			aGreater = true
		}
		if bv > av {
			bGreater = true
		}
	}
	switch {
	case aGreater && !bGreater:
		return 1
	case bGreater && !aGreater:
		return -1
	default:
		return 0
	}
}

func mergeSiblings(existing, incoming []VersionedValue) []VersionedValue {
	all := append(cloneValues(existing), cloneValues(incoming)...)
	var kept []VersionedValue
	for i, candidate := range all {
		dominated := false
		duplicate := false
		for j, other := range all {
			if i == j {
				continue
			}
			cmp := compareClock(candidate.Clock, other.Clock)
			if cmp < 0 {
				dominated = true
				break
			}
			if candidate.Value == other.Value && clockEqual(candidate.Clock, other.Clock) && j < i {
				duplicate = true
				break
			}
		}
		if !dominated && !duplicate {
			kept = append(kept, VersionedValue{Value: candidate.Value, Clock: cloneClock(candidate.Clock)})
		}
	}
	sortValues(kept)
	return kept
}

func buggyRepairMerge(existing, incoming []VersionedValue) []VersionedValue {
	all := mergeSiblings(existing, incoming)
	if len(all) <= 1 {
		return all
	}
	winner := all[0]
	for _, v := range all[1:] {
		if clockScore(v.Clock) >= clockScore(winner.Clock) {
			winner = v
		}
	}
	return []VersionedValue{{Value: winner.Value, Clock: cloneClock(winner.Clock)}}
}

func clockScore(c Clock) int {
	var score int
	for _, v := range c {
		score += v
	}
	return score
}

func clockEqual(a, b Clock) bool {
	if len(a) != len(b) {
		return false
	}
	for k, av := range a {
		if b[k] != av {
			return false
		}
	}
	return true
}

func sortValues(values []VersionedValue) {
	sort.Slice(values, func(i, j int) bool {
		return values[i].Value < values[j].Value
	})
}
```

Append this helper to `bugs/quorum-read-repair/store_test.go`:

```go
func assertValues(t *testing.T, got []VersionedValue, want ...string) {
	t.Helper()
	values := make([]string, 0, len(got))
	for _, v := range got {
		values = append(values, v.Value)
	}
	sort.Strings(values)
	sort.Strings(want)
	if len(values) != len(want) {
		t.Fatalf("values=%v want=%v", values, want)
	}
	for i := range want {
		if values[i] != want[i] {
			t.Fatalf("values=%v want=%v", values, want)
		}
	}
}
```

Also add `import "sort"` to `store_test.go`:

```go
import (
	"sort"
	"testing"
)
```

- [ ] **Step 4: Run merge tests**

Run:

```bash
./go/bin/go test -count=1 -run 'TestCorrectMerge|TestBuggyRepairMerge' ./bugs/quorum-read-repair
```

Expected: `ok`.

- [ ] **Step 5: Commit**

Run:

```bash
git add bugs/quorum-read-repair/store.go bugs/quorum-read-repair/store_test.go
git commit -m "Add quorum read repair merge semantics"
```

---

### Task 3: Replica Runtime

**Files:**
- Modify: `bugs/quorum-read-repair/store.go`
- Test: `bugs/quorum-read-repair/store_test.go`

- [ ] **Step 1: Add a failing replica FIFO test**

Append to `bugs/quorum-read-repair/store_test.go`:

```go
func TestReplicaFIFO_NormalWritesPreserveSiblings(t *testing.T) {
	runtime.GOMAXPROCS(8)

	r := NewOrchestratorTransport("R1")
	c := NewOrchestratorTransport("C")
	r.Connect(c)
	c.Connect(r)

	orch := orchestrator.New()
	orch.AddNode(r, func(t *testing.T) {
		r.StartBridge()
		replica := NewReplica("R1", r)
		replica.Start()
	})
	orch.AddNode(c, func(t *testing.T) {
		c.StartBridge()
		c.Send("R1", Message{Kind: MsgPut, RequestID: "put-a", Key: "x", Values: []VersionedValue{{Value: "A", Clock: Clock{"C1": 1}}}})
		c.Send("R1", Message{Kind: MsgPut, RequestID: "put-b", Key: "x", Values: []VersionedValue{{Value: "B", Clock: Clock{"C2": 1}}}})
		waitForAck(t, c, MsgPutAck, "put-a")
		waitForAck(t, c, MsgPutAck, "put-b")
		c.Send("R1", Message{Kind: MsgGet, RequestID: "get", Key: "x"})
		resp := waitForResponse(t, c, MsgGetResp, "get")
		assertValues(t, resp.Values, "A", "B")
		c.Shutdown()
	})

	if _, ok := orch.Run(t); !ok {
		t.Fatal("replica FIFO run failed")
	}
}
```

Add imports to `store_test.go`:

```go
import (
	"runtime"
	"sort"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)
```

- [ ] **Step 2: Run the failing replica test**

Run:

```bash
./go/bin/go test -count=1 -run TestReplicaFIFO_NormalWritesPreserveSiblings ./bugs/quorum-read-repair
```

Expected: compile failure because `NewReplica`, `Start`, `waitForAck`, and `waitForResponse` do not exist.

- [ ] **Step 3: Implement replica runtime and test wait helpers**

Append to `bugs/quorum-read-repair/store.go`:

```go
func NewReplica(addr string, tr *OrchestratorTransport) *Replica {
	return &Replica{
		addr:     addr,
		tr:       tr,
		applyCh:  make(chan applyReq, 64),
		repairCh: make(chan applyReq, 64),
		closeCh:  make(chan struct{}),
		store:    make(map[string][]VersionedValue),
	}
}

func (r *Replica) Start() {
	go r.router()
	go r.applyLoop()
	go r.repairLoop()
}

func (r *Replica) router() {
	for msg := range r.tr.Mailbox() {
		switch msg.Kind {
		case MsgPut:
			r.applyCh <- applyReq{msg: msg, ack: msg.From}
		case MsgRepair:
			r.repairCh <- applyReq{msg: msg, ack: msg.From}
		case MsgGet:
			values := r.Snapshot(msg.Key)
			r.tr.Send(msg.From, Message{
				Kind:      MsgGetResp,
				RequestID: msg.RequestID,
				Key:       msg.Key,
				Values:    values,
			})
		}
	}
}

func (r *Replica) applyLoop() {
	for req := range r.applyCh {
		r.mu.Lock()
		r.store[req.msg.Key] = mergeSiblings(r.store[req.msg.Key], req.msg.Values)
		r.mu.Unlock()
		r.tr.Send(req.ack, Message{Kind: MsgPutAck, RequestID: req.msg.RequestID, Key: req.msg.Key})
	}
}

func (r *Replica) repairLoop() {
	for req := range r.repairCh {
		r.mu.Lock()
		r.store[req.msg.Key] = buggyRepairMerge(r.store[req.msg.Key], req.msg.Values)
		r.mu.Unlock()
		r.tr.Send(req.ack, Message{Kind: MsgRepairAck, RequestID: req.msg.RequestID, Key: req.msg.Key})
	}
}

func (r *Replica) Snapshot(key string) []VersionedValue {
	r.mu.Lock()
	defer r.mu.Unlock()
	return cloneValues(r.store[key])
}
```

Append to `bugs/quorum-read-repair/store_test.go`:

```go
func waitForAck(t *testing.T, tr *OrchestratorTransport, kind MsgKind, requestID string) Message {
	t.Helper()
	for msg := range tr.Mailbox() {
		if msg.Kind == kind && msg.RequestID == requestID {
			return msg
		}
	}
	t.Fatalf("mailbox closed before %s %s", msgKindName(kind), requestID)
	return Message{}
}

func waitForResponse(t *testing.T, tr *OrchestratorTransport, kind MsgKind, requestID string) Message {
	t.Helper()
	for msg := range tr.Mailbox() {
		if msg.Kind == kind && msg.RequestID == requestID {
			return msg
		}
	}
	t.Fatalf("mailbox closed before %s %s", msgKindName(kind), requestID)
	return Message{}
}
```

- [ ] **Step 4: Run replica tests**

Run:

```bash
./go/bin/go test -count=1 -run 'TestCorrectMerge|TestBuggyRepairMerge|TestReplicaFIFO' ./bugs/quorum-read-repair
```

Expected: `ok`.

- [ ] **Step 5: Commit**

Run:

```bash
git add bugs/quorum-read-repair/store.go bugs/quorum-read-repair/store_test.go
git commit -m "Add quorum read repair replica runtime"
```

---

### Task 4: Quorum Client

**Files:**
- Create: `bugs/quorum-read-repair/client.go`
- Test: `bugs/quorum-read-repair/client_test.go`

- [ ] **Step 1: Write failing quorum client test**

Create `bugs/quorum-read-repair/client_test.go`:

```go
package quorumreadrepair

import (
	"runtime"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

func TestClientFIFO_PutAndReadRepairs(t *testing.T) {
	runtime.GOMAXPROCS(8)

	addrs := []string{"R1", "R2", "R3", "C1"}
	transports := setupCluster(addrs)
	orch := orchestrator.New()
	addReplicas(orch, []string{"R1", "R2", "R3"}, transports)

	orch.AddNode(transports["C1"], func(t *testing.T) {
		tr := transports["C1"]
		tr.StartBridge()
		client := NewClient("C1", tr, []string{"R1", "R2", "R3"})
		client.Put("x", "A")
		got := client.GetAndRepair("x", "read-1")
		assertValues(t, got, "A")
		tr.Shutdown()
	})

	if _, ok := orch.Run(t); !ok {
		t.Fatal("client FIFO run failed")
	}
}
```

- [ ] **Step 2: Run the failing client test**

Run:

```bash
./go/bin/go test -count=1 -run TestClientFIFO_PutAndReadRepairs ./bugs/quorum-read-repair
```

Expected: compile failure because `setupCluster`, `addReplicas`, `NewClient`, `Put`, and `GetAndRepair` do not exist.

- [ ] **Step 3: Implement client and initial cluster helpers**

Create `bugs/quorum-read-repair/client.go`:

```go
package quorumreadrepair

import "fmt"

const quorum = 2

type Client struct {
	addr     string
	tr       *OrchestratorTransport
	replicas []string
	seq      int
}

func NewClient(addr string, tr *OrchestratorTransport, replicas []string) *Client {
	return &Client{addr: addr, tr: tr, replicas: append([]string(nil), replicas...)}
}

func (c *Client) Put(key, value string) {
	c.seq++
	requestID := fmt.Sprintf("%s-put-%d", c.addr, c.seq)
	vv := VersionedValue{Value: value, Clock: Clock{c.addr: c.seq}}
	for _, replica := range c.replicas {
		c.tr.Send(replica, Message{
			Kind:      MsgPut,
			RequestID: requestID,
			Key:       key,
			Values:    []VersionedValue{vv},
		})
	}
	acks := 0
	for msg := range c.tr.Mailbox() {
		if msg.Kind == MsgPutAck && msg.RequestID == requestID {
			acks++
			if acks == quorum {
				return
			}
		}
	}
}

func (c *Client) GetAndRepair(key, requestID string) []VersionedValue {
	for _, replica := range c.replicas {
		c.tr.Send(replica, Message{Kind: MsgGet, RequestID: requestID, Key: key})
	}

	responses := make(map[string][]VersionedValue)
	merged := []VersionedValue(nil)
	for msg := range c.tr.Mailbox() {
		if msg.Kind != MsgGetResp || msg.RequestID != requestID {
			continue
		}
		responses[msg.From] = cloneValues(msg.Values)
		merged = mergeSiblings(merged, msg.Values)
		if len(responses) == quorum {
			break
		}
	}

	for _, replica := range c.replicas {
		if !sameValues(responses[replica], merged) {
			c.tr.Send(replica, Message{
				Kind:      MsgRepair,
				RequestID: requestID + "-repair-" + replica,
				Key:       key,
				Values:    merged,
			})
		}
	}
	return merged
}

func sameValues(a, b []VersionedValue) bool {
	a = mergeSiblings(nil, a)
	b = mergeSiblings(nil, b)
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Value != b[i].Value || !clockEqual(a[i].Clock, b[i].Clock) {
			return false
		}
	}
	return true
}
```

Create the initial cluster helpers in `bugs/quorum-read-repair/scenario.go`; Task 5 replaces this file with the full scenario:

```go
package quorumreadrepair

import (
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

var replicaAddrs = []string{"R1", "R2", "R3"}

func setupCluster(addrs []string) map[string]*OrchestratorTransport {
	transports := make(map[string]*OrchestratorTransport, len(addrs))
	for _, addr := range addrs {
		transports[addr] = NewOrchestratorTransport(addr)
	}
	for _, tr := range transports {
		for _, peer := range transports {
			if tr != peer {
				tr.Connect(peer)
			}
		}
	}
	return transports
}

func addReplicas(orch *orchestrator.Orchestrator, addrs []string, transports map[string]*OrchestratorTransport) {
	for _, addr := range addrs {
		addr := addr
		tr := transports[addr]
		orch.AddNode(tr, func(t *testing.T) {
			tr.StartBridge()
			replica := NewReplica(addr, tr)
			replica.Start()
		})
	}
}
```

- [ ] **Step 4: Run client test**

Run:

```bash
./go/bin/go test -count=1 -run TestClientFIFO_PutAndReadRepairs ./bugs/quorum-read-repair
```

Expected: `ok`.

- [ ] **Step 5: Commit**

Run:

```bash
git add bugs/quorum-read-repair/client.go bugs/quorum-read-repair/client_test.go bugs/quorum-read-repair/scenario.go
git commit -m "Add quorum read repair client"
```

---

### Task 5: Scenario And FIFO Oracle

**Files:**
- Modify: `bugs/quorum-read-repair/scenario.go`
- Create: `bugs/quorum-read-repair/bug_demo_test.go`

- [ ] **Step 1: Write failing FIFO scenario test**

Create `bugs/quorum-read-repair/bug_demo_test.go`:

```go
package quorumreadrepair

import (
	"runtime"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

func TestQuorumReadRepair_FIFOPasses(t *testing.T) {
	runtime.GOMAXPROCS(8)
	resetObservedOutcome()

	orch := orchestrator.New()
	addQuorumReadRepairScenario(orch, false)

	if _, ok := orch.Run(t); !ok {
		t.Fatal("FIFO run failed")
	}
	if observedBugFound() {
		t.Fatalf("FIFO should preserve siblings, got %v", lastObservedValues())
	}
}
```

- [ ] **Step 2: Run the failing FIFO test**

Run:

```bash
./go/bin/go test -count=1 -run TestQuorumReadRepair_FIFOPasses ./bugs/quorum-read-repair
```

Expected: compile failure because scenario outcome helpers do not exist.

- [ ] **Step 3: Implement scenario and outcome helpers**

Replace `bugs/quorum-read-repair/scenario.go` with:

```go
package quorumreadrepair

import (
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

var replicaAddrs = []string{"R1", "R2", "R3"}
var clientAddrs = []string{"C1", "C2", "Reader"}

var outcome struct {
	mu     sync.Mutex
	values map[string][]string
	bug    bool
}

func resetObservedOutcome() {
	outcome.mu.Lock()
	defer outcome.mu.Unlock()
	outcome.values = make(map[string][]string)
	outcome.bug = false
}

func observedBugFound() bool {
	outcome.mu.Lock()
	defer outcome.mu.Unlock()
	return outcome.bug
}

func lastObservedValues() map[string][]string {
	outcome.mu.Lock()
	defer outcome.mu.Unlock()
	out := make(map[string][]string, len(outcome.values))
	for k, v := range outcome.values {
		out[k] = append([]string(nil), v...)
	}
	return out
}

func setupCluster(addrs []string) map[string]*OrchestratorTransport {
	transports := make(map[string]*OrchestratorTransport, len(addrs))
	for _, addr := range addrs {
		transports[addr] = NewOrchestratorTransport(addr)
	}
	for _, tr := range transports {
		for _, peer := range transports {
			if tr != peer {
				tr.Connect(peer)
			}
		}
	}
	return transports
}

func addReplicas(orch *orchestrator.Orchestrator, addrs []string, transports map[string]*OrchestratorTransport) {
	for _, addr := range addrs {
		addr := addr
		tr := transports[addr]
		orch.AddNode(tr, func(t *testing.T) {
			tr.StartBridge()
			replica := NewReplica(addr, tr)
			replica.Start()
		})
	}
}

func addQuorumReadRepairScenario(orch *orchestrator.Orchestrator, checked bool) {
	all := append([]string{}, replicaAddrs...)
	all = append(all, clientAddrs...)
	transports := setupCluster(all)
	addReplicas(orch, replicaAddrs, transports)

	orch.AddNode(transports["C1"], func(t *testing.T) {
		tr := transports["C1"]
		tr.StartBridge()
		client := NewClient("C1", tr, replicaAddrs)
		client.Put("x", "A")
		tr.SendControl("Reader", "c1-done")
		tr.Shutdown()
	})

	orch.AddNode(transports["C2"], func(t *testing.T) {
		tr := transports["C2"]
		tr.StartBridge()
		client := NewClient("C2", tr, replicaAddrs)
		client.Put("x", "B")
		tr.SendControl("Reader", "c2-done")
		tr.Shutdown()
	})

	orch.AddNode(transports["Reader"], func(t *testing.T) {
		tr := transports["Reader"]
		tr.StartBridge()
		tr.WaitControl("c1-done")
		tr.WaitControl("c2-done")
		client := NewClient("Reader", tr, replicaAddrs)
		client.GetAndRepair("x", "read-1")
		final := client.GetAndRepair("x", "read-2")
		recordOutcome("reader", final)
		if checked && !hasValues(final, "A", "B") {
			t.Errorf("lost concurrent sibling in reader result: got %v want [A B]", valuesOnly(final))
		}
		tr.Shutdown()
	})
}

func recordOutcome(replica string, values []VersionedValue) {
	got := valuesOnly(values)
	outcome.mu.Lock()
	defer outcome.mu.Unlock()
	outcome.values[replica] = got
	if !stringSetEqual(got, []string{"A", "B"}) {
		outcome.bug = true
	}
}

func valuesOnly(values []VersionedValue) []string {
	out := make([]string, 0, len(values))
	for _, v := range values {
		out = append(out, v.Value)
	}
	sort.Strings(out)
	return out
}

func hasValues(values []VersionedValue, want ...string) bool {
	return stringSetEqual(valuesOnly(values), want)
}

func stringSetEqual(got, want []string) bool {
	got = append([]string(nil), got...)
	want = append([]string(nil), want...)
	sort.Strings(got)
	sort.Strings(want)
	return strings.Join(got, ",") == strings.Join(want, ",")
}
```

- [ ] **Step 4: Run FIFO scenario**

Run:

```bash
./go/bin/go test -count=1 -run TestQuorumReadRepair_FIFOPasses ./bugs/quorum-read-repair
```

Expected: `ok`.

- [ ] **Step 5: Commit**

Run:

```bash
git add bugs/quorum-read-repair/scenario.go bugs/quorum-read-repair/bug_demo_test.go
git commit -m "Add quorum read repair FIFO scenario"
```

---

### Task 6: Mixed-Interleaving Bug Exposure Tests

**Files:**
- Modify: `bugs/quorum-read-repair/scenario.go`
- Modify: `bugs/quorum-read-repair/bug_demo_test.go`

- [ ] **Step 1: Add exploration and targeted tests**

Append to `bugs/quorum-read-repair/bug_demo_test.go`:

```go
func TestQuorumReadRepair_ExploreGlobalOnly(t *testing.T) {
	runtime.GOMAXPROCS(8)
	resetObservedOutcome()

	orch := orchestrator.New()
	ok := orch.Explore(t, func(o *orchestrator.Orchestrator) {
		resetObservedOutcome()
		addQuorumReadRepairScenario(o, true)
	}, orchestrator.GlobalBound(4), orchestrator.GlobalMaxRuns(500))

	t.Logf("G-only Explore: ok=%v bug=%v values=%v", ok, observedBugFound(), lastObservedValues())
}

func TestQuorumReadRepair_ExploreAll(t *testing.T) {
	runtime.GOMAXPROCS(8)
	resetObservedOutcome()

	orch := orchestrator.New()
	ok := orch.ExploreAll(t, func(o *orchestrator.Orchestrator) {
		resetObservedOutcome()
		addQuorumReadRepairScenario(o, true)
	}, orchestrator.GlobalBound(6), orchestrator.GlobalMaxRuns(2000))

	if ok {
		t.Fatalf("ExploreAll did not find quorum read-repair bug within max runs; values=%v", lastObservedValues())
	}
	t.Logf("ExploreAll: FOUND bug values=%v", lastObservedValues())
}

func TestQuorumReadRepair_FindBug(t *testing.T) {
	runtime.GOMAXPROCS(8)
	resetObservedOutcome()

	orch := orchestrator.New()
	addQuorumReadRepairScenario(orch, false)

	rr := orch.RunWith(t, func(dp orchestrator.DecisionPoint) int {
		if dp.Kind == orchestrator.Global && dp.N() > 1 {
			if idx := chooseGlobal(dp, "C1", "R1", "Put"); idx >= 0 {
				return idx
			}
			if idx := chooseGlobal(dp, "C1", "R2", "Put"); idx >= 0 {
				return idx
			}
			if idx := chooseGlobal(dp, "C2", "R2", "Put"); idx >= 0 {
				return idx
			}
			if idx := chooseGlobal(dp, "C2", "R3", "Put"); idx >= 0 {
				return idx
			}
			if idx := chooseGlobal(dp, "R1", "Reader", "GetResp"); idx >= 0 {
				return idx
			}
			if idx := chooseGlobal(dp, "R2", "Reader", "GetResp"); idx >= 0 {
				return idx
			}
			if idx := chooseGlobal(dp, "Reader", "R3", "Repair"); idx >= 0 {
				return idx
			}
		}
		if dp.Kind == orchestrator.Local && dp.N() > 1 && dp.Node == "R3" {
			return dp.N() - 1
		}
		return 0
	})
	if !observedBugFound() {
		t.Fatalf("expected targeted scheduler to expose bug, passed=%v values=%v", rr.Passed, lastObservedValues())
	}
	t.Logf("FindBug: passed=%v values=%v", rr.Passed, lastObservedValues())
}

func chooseGlobal(dp orchestrator.DecisionPoint, from, to, msgType string) int {
	for i := range dp.Alts {
		alt := dp.Alts[i]
		if alt.From == from && alt.To == to && alt.MsgType == msgType {
			return i
		}
	}
	return -1
}
```

- [ ] **Step 2: Run targeted test and inspect trace**

Run:

```bash
./go/bin/go test -count=1 -v -run TestQuorumReadRepair_FindBug ./bugs/quorum-read-repair
```

Expected: the targeted run records `observedBugFound() == true` and the test passes. The scenario is called with `checked=false` in this test so the targeted bug proof can assert on the recorded outcome instead of failing through `t.Errorf`.

- [ ] **Step 3: Make `FindBug` assert failure without making the package unusable**

Keep this checked-mode split:

```go
func addQuorumReadRepairScenario(orch *orchestrator.Orchestrator, checked bool) {
	// checked=false records outcome only.
	// checked=true calls t.Errorf when the invariant is violated.
}
```

Call:

```go
addQuorumReadRepairScenario(orch, false)
```

inside `TestQuorumReadRepair_FindBug`, and call:

```go
addQuorumReadRepairScenario(o, true)
```

inside `TestQuorumReadRepair_ExploreAll`.

- [ ] **Step 4: Run exploration tests**

Run:

```bash
./go/bin/go test -count=1 -v -run 'TestQuorumReadRepair_(FIFOPasses|ExploreGlobalOnly|ExploreAll|FindBug)$' ./bugs/quorum-read-repair
```

Expected:
- FIFO passes.
- Global-only logs its result.
- ExploreAll finds the bug within 2000 runs.
- FindBug proves a mixed global/local schedule reaches the bug.

- [ ] **Step 5: Commit**

Run:

```bash
git add bugs/quorum-read-repair/scenario.go bugs/quorum-read-repair/bug_demo_test.go
git commit -m "Expose quorum read repair mixed interleaving bug"
```

---

### Task 7: Benchmarks

**Files:**
- Create: `bugs/quorum-read-repair/bench_test.go`

- [ ] **Step 1: Add benchmark tests**

Create `bugs/quorum-read-repair/bench_test.go`:

```go
package quorumreadrepair

import (
	"fmt"
	"os"
	"runtime"
	"testing"

	"github.com/shubhaankar/synctest/orchestrator"
)

const benchMaxRuns = 500

func benchSetup(o *orchestrator.Orchestrator) {
	resetObservedOutcome()
	addQuorumReadRepairScenario(o, true)
}

func benchDir() string {
	d := os.Getenv("BENCH_DIR")
	if d != "" {
		_ = os.MkdirAll(d, 0755)
	}
	return d
}

func observers(t *testing.T, policy string) []orchestrator.ExploreOption {
	dir := benchDir()
	if dir == "" {
		return nil
	}
	var opts []orchestrator.ExploreOption
	if f, err := os.Create(dir + "/" + policy + ".jsonl"); err == nil {
		t.Cleanup(func() { _ = f.Close() })
		opts = append(opts, orchestrator.WithObserver(orchestrator.NewJSONLObserver(f, policy)))
	}
	if f, err := os.Create(dir + "/" + policy + "-trace.jsonl"); err == nil {
		t.Cleanup(func() { _ = f.Close() })
		opts = append(opts, orchestrator.WithObserver(orchestrator.NewDetailedObserver(f, policy)))
	}
	return opts
}

func logBenchStart(t *testing.T, label string) {
	t.Helper()
	fmt.Printf("START %s\n", label)
}

func logBenchDone(t *testing.T, label string, runs int, firstBug int, elapsed interface{}) {
	t.Helper()
	fmt.Printf("DONE %s: runs=%d first_bug=%d elapsed=%v\n", label, runs, firstBug, elapsed)
}

func requireFirstBug(t *testing.T, label string, firstBug int) {
	t.Helper()
	if firstBug == -1 {
		t.Fatalf("%s did not find bug within %d runs", label, benchMaxRuns)
	}
}

func TestBench_CHESS_GlobalOnly(t *testing.T) {
	runtime.GOMAXPROCS(8)
	logBenchStart(t, "CHESS(G-only,k=4)")
	algo := &orchestrator.CHESS{Bound: 4, GlobalOnly: true}
	opts := append([]orchestrator.ExploreOption{orchestrator.GlobalMaxRuns(benchMaxRuns)}, observers(t, "chess-global")...)
	orch := orchestrator.New()
	r := orch.ExploreWith(t, benchSetup, algo, opts...)
	logBenchDone(t, "CHESS(G-only,k=4)", r.Runs, r.FirstBug, r.Elapsed)
	if dir := benchDir(); dir != "" {
		if f, err := os.Create(dir + "/chess-global-tree.json"); err == nil {
			orchestrator.WriteTreeJSON(f, algo.Tree())
			_ = f.Close()
		}
	}
}

func TestBench_CHESS_GL(t *testing.T) {
	runtime.GOMAXPROCS(8)
	logBenchStart(t, "CHESS(G+L,k=4)")
	algo := &orchestrator.CHESS{Bound: 4}
	opts := append([]orchestrator.ExploreOption{orchestrator.GlobalMaxRuns(benchMaxRuns)}, observers(t, "chess-gl")...)
	orch := orchestrator.New()
	r := orch.ExploreWith(t, benchSetup, algo, opts...)
	logBenchDone(t, "CHESS(G+L,k=4)", r.Runs, r.FirstBug, r.Elapsed)
	requireFirstBug(t, "CHESS(G+L,k=4)", r.FirstBug)
	if dir := benchDir(); dir != "" {
		if f, err := os.Create(dir + "/chess-gl-tree.json"); err == nil {
			orchestrator.WriteTreeJSON(f, algo.Tree())
			_ = f.Close()
		}
	}
}

func TestBench_PCT_d2(t *testing.T) {
	runtime.GOMAXPROCS(8)
	logBenchStart(t, "PCT(d=2)")
	algo := &orchestrator.PCT{Depth: 2, MaxSteps: 256, Seed: 1}
	opts := append([]orchestrator.ExploreOption{orchestrator.GlobalMaxRuns(benchMaxRuns)}, observers(t, "pct-d2")...)
	orch := orchestrator.New()
	r := orch.ExploreWith(t, benchSetup, algo, opts...)
	logBenchDone(t, "PCT(d=2)", r.Runs, r.FirstBug, r.Elapsed)
}

func TestBench_PCT_d3(t *testing.T) {
	runtime.GOMAXPROCS(8)
	logBenchStart(t, "PCT(d=3)")
	algo := &orchestrator.PCT{Depth: 3, MaxSteps: 256, Seed: 1}
	opts := append([]orchestrator.ExploreOption{orchestrator.GlobalMaxRuns(benchMaxRuns)}, observers(t, "pct-d3")...)
	orch := orchestrator.New()
	r := orch.ExploreWith(t, benchSetup, algo, opts...)
	logBenchDone(t, "PCT(d=3)", r.Runs, r.FirstBug, r.Elapsed)
}

func TestBench_Random(t *testing.T) {
	runtime.GOMAXPROCS(8)
	logBenchStart(t, "Random")
	algo := &orchestrator.Random{Seed: 1}
	opts := append([]orchestrator.ExploreOption{orchestrator.GlobalMaxRuns(benchMaxRuns)}, observers(t, "random")...)
	orch := orchestrator.New()
	r := orch.ExploreWith(t, benchSetup, algo, opts...)
	logBenchDone(t, "Random", r.Runs, r.FirstBug, r.Elapsed)
}
```

- [ ] **Step 2: Run benchmark tests directly**

Run:

```bash
GODEBUG=asyncpreemptoff=1 ./go/bin/go test -count=1 -v -run '^TestBench_' ./bugs/quorum-read-repair
```

Expected: CHESS global+local finds the bug; other algorithms log comparable metrics.

- [ ] **Step 3: Run package benchmark chart target**

Run:

```bash
make benchmark-charts pkg=bugs/quorum-read-repair BENCH_TIMEOUT=45s
```

Expected: JSONL files under `bugs/quorum-read-repair/benchmarking/data` and figures under `bugs/quorum-read-repair/benchmarking/figures`.

- [ ] **Step 4: Commit**

Run:

```bash
git add bugs/quorum-read-repair/bench_test.go bugs/quorum-read-repair/benchmarking
git commit -m "Add quorum read repair benchmarks"
```

---

### Task 8: Bug Documentation And Final Verification

**Files:**
- Create: `bugs/quorum-read-repair/BUG.md`
- Modify: any `bugs/quorum-read-repair/*.go` files needed only for cleanup after verification.

- [ ] **Step 1: Write bug documentation**

Create `bugs/quorum-read-repair/BUG.md`:

```markdown
# `quorum-read-repair`

This package models a small Dynamo-style quorum KV store with three replicas,
two writers, and a read coordinator that performs read repair.

## Core Bug

Normal writes preserve vector-clock siblings. Read repair is deliberately
buggy: it collapses concurrent siblings to one winner using a scalar clock
score and arrival order. A repair can therefore erase a concurrent write that
should remain visible as a sibling.

The bug requires mixed interleaving:

1. Global message delivery forms overlapping write quorums for `x=A` and `x=B`.
2. A quorum read observes an incomplete sibling set.
3. The read coordinator sends a repair based on that incomplete observation.
4. A local scheduling choice on a replica runs `repairLoop` at the wrong point
   relative to normal write application.
5. The final value set loses `A` or `B`, or replicas diverge.

## No Sleep Or Delay Injection

The scenario does not use `time.Sleep`, timer delays, duplicated messages,
dropped messages, or transport-level delay rules. All failing schedules are
formed by ordinary protocol messages and local goroutine scheduling choices.

## Expected Invariant

Concurrent writes `A` with clock `{C1:1}` and `B` with clock `{C2:1}` must
converge to the sibling set `{A, B}`. Losing either sibling is a safety
violation.
```

- [ ] **Step 2: Run focused package tests**

Run:

```bash
GODEBUG=asyncpreemptoff=1 ./go/bin/go test -count=1 -v -run 'Test(TransportSmoke|CorrectMerge|BuggyRepairMerge|ReplicaFIFO|ClientFIFO|QuorumReadRepair_)' ./bugs/quorum-read-repair
```

Expected: all non-bug-exposing tests pass; bug-exposing exploration tests behave as documented in their assertions.

- [ ] **Step 3: Run all package tests**

Run:

```bash
GODEBUG=asyncpreemptoff=1 ./go/bin/go test -count=1 -v ./bugs/quorum-read-repair
```

Expected: package passes. Bug-discovery tests must report success when they find the deliberate bug.

- [ ] **Step 4: Run repository bug-package smoke**

Run:

```bash
GODEBUG=asyncpreemptoff=1 ./go/bin/go test -count=1 ./bugs/quorum-read-repair ./orchestrator
```

Expected: `ok` for both packages.

- [ ] **Step 5: Check formatting and dirty state**

Run:

```bash
gofmt -w bugs/quorum-read-repair
git status --short
```

Expected: only intended `bugs/quorum-read-repair` files and generated benchmarking files are changed.

- [ ] **Step 6: Commit**

Run:

```bash
git add bugs/quorum-read-repair
git commit -m "Document quorum read repair bug"
```
