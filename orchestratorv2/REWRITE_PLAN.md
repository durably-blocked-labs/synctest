# Orchestrator Cleanup Plan

Open a new Claude Code window in this directory and say:
"Read orchestratorv2/REWRITE_PLAN.md and implement it"

## Files to Modify

1. `orchestratorv2/orchestrator.go` (839 lines → ~500 lines)
2. `rafttest/transport.go` (transport shutdown fix)
3. `rafttest/orchestrator_test.go` (update callers)
4. `rafttest/bug_demo_test.go` (update callers)

## Change 1: Merge run() and runWithGlobalPrefix() into runOnce()

These two functions (lines ~343-610 and ~619-838) are 90% identical. Merge into:

```go
// pickFn selects which op to deliver from the schedulable queue.
// Returns the index into schedulable. Called once per delivery step.
type pickFn func(schedulable []*PendingOp, step int) int

func (o *Orchestrator) runOnce(t *testing.T, pick pickFn) (RecordedRun, []GlobalDecision, bool)
```

Add to RecordedRun:
```go
type RecordedRun struct {
    GlobalTrace     []GlobalStep
    GlobalDecisions []GlobalDecision // NEW: index-based decisions
    LocalTraces     map[string][]synctest.Decision
}

type GlobalDecision struct {
    Index     int
    QueueSize int
}
```

Wire public methods:
```go
func (o *Orchestrator) Run(t *testing.T) (RecordedRun, bool) {
    rec, _, ok := o.runOnce(t, func(_ []*PendingOp, _ int) int { return 0 })
    return rec, ok
}

func (o *Orchestrator) Replay(t *testing.T, rec RecordedRun) (RecordedRun, bool) {
    pick := func(_ []*PendingOp, step int) int {
        if step < len(rec.GlobalDecisions) {
            return rec.GlobalDecisions[step].Index
        }
        return 0
    }
    rec2, _, ok := o.runOnce(t, pick)
    return rec2, ok
}
```

For Explore, the pickFn follows the prefix then defaults to 0:
```go
pick := func(_ []*PendingOp, step int) int {
    if step < len(prefix) {
        return prefix[step].Index
    }
    return 0
}
```

## Change 2: Index-based replay (delete findDeliveryOp)

Delete the `findDeliveryOp()` function entirely. It matches by {From, To, Type} which is fragile. The new Replay uses `rec.GlobalDecisions[step].Index` directly.

In runOnce, at the delivery step, always record:
```go
decisions = append(decisions, GlobalDecision{
    Index:     idx,
    QueueSize: len(schedulable),
})
```

## Change 3: Replace reflect.Select with fan-in

Delete the reflect.SelectCase machinery. Replace with:

```go
type bubbleEvent struct {
    addr string
    idle *distributed.IdleState
    done bool
}

// collectEvents launches fan-in goroutines for nodes that haven't reported.
// Returns a channel that receives one event per needed node.
func (o *Orchestrator) collectEvents(needed map[string]*nodeCtrl) <-chan bubbleEvent {
    ch := make(chan bubbleEvent, len(needed))
    for addr, ctrl := range needed {
        addr, ctrl := addr, ctrl
        go func() {
            select {
            case idle := <-ctrl.bubble.Idle:
                ch <- bubbleEvent{addr: addr, idle: &idle}
            case <-ctrl.done:
                ch <- bubbleEvent{addr: addr, done: true}
            }
        }()
    }
    return ch
}
```

Then in the main loop:
```go
// Collect: wait for all non-idle, non-done bubbles
needed := map[string]*nodeCtrl{}
for _, addr := range o.order {
    if doneSet[addr] || pendingIdle[addr] != nil { continue }
    needed[addr] = o.nodes[addr]
}
if len(needed) > 0 {
    events := o.collectEvents(needed)
    for i := 0; i < len(needed); i++ {
        ev := <-events
        if ev.done { doneSet[ev.addr] = true; active--; /* trace */ }
        else { pendingIdle[ev.addr] = ev.idle }
    }
}
```

Remove the `"reflect"` import.

## Change 4: Transport shutdown fix

In `rafttest/transport.go`, ensure the shutdown contract:

**Contract:** Every respCh registered in pending receives exactly one value.

1. `makeRPC` already registers respCh before Phase 1. Keep this.
2. In the Execute closure, when `peer.closeCh` fires, route error through sender's mailbox with a BLOCKING send (not `default`):
```go
case <-peer.closeCh:
    t.mailbox <- envelope{
        kind:  envelopeResponse,
        reqID: reqID,
        from:  target,
        to:    t.localAddr,
        resp:  raft.RPCResponse{Error: fmt.Errorf("rafttest: peer %q closed", target)},
    }
```
3. `Close()` already drains pending. Keep this.
4. The TOCTOU check between Phase 1 and Phase 2 (lines ~270-274) can stay as an optimization but is not required for correctness.

## Change 5: Merge AddNode/AddNodeWithOptions

Replace both with one function:
```go
func (o *Orchestrator) AddNode(transport NodeTransport, f func(t *testing.T), opts ...distributed.Option) {
    addr := transport.Addr()
    ctrl := &nodeCtrl{
        transport:  transport,
        testFunc:   f,
        bubbleOpts: opts,
    }
    o.nodes[addr] = ctrl
    o.order = append(o.order, addr)
}
```

Update callers in orchestrator_test.go and bug_demo_test.go:
- `orch.AddNode(trans, bubbleFunc)` stays the same (variadic opts is empty)
- `orch.AddNodeWithOptions(trans, fn, opts...)` becomes `orch.AddNode(trans, fn, opts...)`

## Build & Test

```bash
cd /Users/shubhaankar/github.com/research/synctest
./go/bin/go build ./orchestratorv2/ ./rafttest/
timeout 60 ./go/bin/go test -run TestRaftThreeNodeElectionReplay ./rafttest/ -timeout 45s
timeout 60 ./go/bin/go test -run TestStaleTermConcept ./rafttest/ -timeout 45s
timeout 90 ./go/bin/go test -run TestRemoveLeaderConcept ./rafttest/ -timeout 60s
```

## What to DELETE
- `findDeliveryOp()` function
- `run()` function (replaced by runOnce)
- `runWithGlobalPrefix()` function (replaced by runOnce)
- `AddNodeWithOptions()` function (merged into AddNode)
- `reflect` import
- `OpDir` / `OpRecv` / `dirName()` if unused after merge
- All duplicate reflect.SelectCase blocks
