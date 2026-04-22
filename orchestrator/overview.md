# Package Overview

## orchestrator

Coordinates multiple synctest bubbles as distributed nodes. Each node runs in an isolated bubble; the orchestrator only acts when **all** bubbles are idle (every goroutine durably blocked). It then decides which message to deliver next, resumes the target bubble, and repeats until all nodes finish.

### Types

| Type | Purpose |
|---|---|
| `Orchestrator` | Central controller: holds per-node state, the schedulable op queue, and the global trace. |
| `nodeCtrl` | Per-node state: transport reference, bubble handle, done channel, local scheduling trace, pass/fail. |
| `PendingOp` | Unit of work: carries sender, receiver, RPC type name, and an `Execute` closure that writes the message into the target's mailbox. |
| `RecordedRun` | Complete record of one run: `GlobalTrace` (ordered delivery/time/done events) and `LocalTraces` (per-node goroutine scheduling decisions). Used as input to `Replay`. |
| `GlobalStep` | One event in the global trace — a message delivery, a clock advance, or a node completing. |
| `NodeTransport` | Interface transports must implement: `Addr() string` and `Outbox() <-chan *PendingOp`. |

### Functions

**`New() *Orchestrator`**
Creates an empty orchestrator with no nodes registered.

**`AddNode(transport, f)`**
Registers a node by its transport (for address and outbox access) and the bubble function to run inside it. Bubbles are created lazily at run time, not here.

**`Run(t) (RecordedRun, bool)`**
Starts all bubbles with fresh FIFO scheduling, drives them to completion, and returns a `RecordedRun`. Delivery order is first-in-first-out over drained outboxes.

**`Replay(t, rec) (RecordedRun, bool)`**
Re-runs with the same nodes but replays `rec.LocalTraces` as intra-bubble scheduling prefixes and follows `rec.GlobalTrace`'s delivery order for cross-node messages. When a recorded op cannot be matched (diverged run), falls back to FIFO and logs a warning.

**`initBubbles(localPrefixes)`** *(internal)*
Creates a fresh `distributed.Bubble` for each node. Passes `WithPrefix` options when replaying local traces, or nothing for a fresh run.

**`startBubble(t, ctrl)`** *(internal)*
Launches the node's test function inside `synctest.Explore` (not `synctest.Test`) so that test failures don't call `t.FailNow()` mid-goroutine, which would prevent the bubble's done channel from being closed and deadlock the orchestrator. Captures the local scheduling trace and signals `ctrl.done` on exit.

**`run(t, deliveryTrace)`** *(internal)*
Shared implementation for `Run` and `Replay`. The main loop:
1. Starts all bubbles sequentially, waiting for each to reach its first idle point before starting the next (eliminates concurrent rand access during init).
2. Waits for every active bubble to report idle or done via `reflect.Select`.
3. Drains each node's outbox non-blockingly into the schedulable queue.
4. **If schedulable ops exist**: picks one (replay order or FIFO), calls `Execute()`, resumes the target bubble.
5. **If no ops but timers exist**: computes the earliest timer across all nodes, advances the global clock, resumes each idle bubble sequentially with `AdvanceTimeTo`.
6. **If neither**: resumes all idle bubbles sequentially so they can finish remaining work and exit.
7. Repeats until all nodes are done.

**`findDeliveryOp(schedulable, want)`** *(internal)*
Searches the schedulable queue for the first op matching a recorded step's `(From, To, Type)` triple. Returns -1 on no match, triggering FIFO fallback.

---

## rafttest / RaftTransport

Implements `raft.Transport` by routing all cross-node RPCs through the orchestrator's controlled delivery pipeline. Instead of using real network connections, every request and response is a `PendingOp` submitted to the orchestrator outbox, ensuring the orchestrator decides when and in what order messages are delivered.

### Key design constraint

`internalConsumer` (the channel Raft reads incoming RPCs from) must be created **inside** the bubble via `StartBridge()`. If it were created outside, Raft goroutines blocking on it would not be durably blocked from the bubble's perspective and the bubble could never go idle.

### Types

| Type | Purpose |
|---|---|
| `RaftTransport` | The transport implementation. Holds the outbox (to orchestrator), mailbox (from orchestrator), peer map, and pending-response registry. |
| `envelope` | Internal message carrier: a request, response, or close sentinel with a request ID for correlation. |

### Functions

**`NewRaftTransport(addr)`**
Creates a transport with a buffered outbox (cap 64) and mailbox (cap 16). `internalConsumer` is left nil until `StartBridge`.

**`Connect(peer)`**
Registers a peer transport. Must be called before the orchestrator starts. Used by `makeRPC` to look up the target's mailbox for the `Execute` closure.

**`StartBridge()`**
Must be called **inside** the bubble before `raft.NewRaft`. Creates `internalConsumer` as a bubble channel, then spawns the bridge goroutine. The bridge loops: `ExternalWait` on the mailbox (making it a durable block), then dispatches the received envelope to `handleEnvelope`.

**`handleEnvelope(msg)`** *(internal)*
Dispatches by kind:
- `envelopeRequest`: forwards the RPC struct onto `internalConsumer` (Raft picks it up), creates a proxy response channel, and spawns `forwardResponse` to watch for the reply.
- `envelopeResponse`: looks up the pending response channel by request ID and delivers the response, unblocking `makeRPC`'s phase 2 wait.

**`forwardResponse(req, proxyRespCh)`** *(internal)*
Waits for Raft to write a response to `proxyRespCh`, then submits a new `PendingOp` to the outbox via `ExternalWait`. The orchestrator will later deliver this response envelope to the original sender's mailbox.

**`makeRPC(target, cmd, reader)`** *(internal)*
The core send path, called by all `raft.Transport` methods:
- **Phase 1**: `ExternalWait` on the outbox send. Submits a `PendingOp` whose `Execute` writes the request envelope into the target's mailbox. The orchestrator controls when this happens.
- **Phase 2**: blocks on `respCh` (a buffered bubble channel) for the response. The bridge goroutine will write to `respCh` when the orchestrator later delivers the response envelope.

**`AppendEntries`, `RequestVote`, `RequestPreVote`, `InstallSnapshot`, `TimeoutNow`**
Thin wrappers over `makeRPC` that unpack the typed response from the `raft.RPCResponse` union.

**`AppendEntriesPipeline`**
Returns `ErrPipelineReplicationNotSupported`. Pipelining is incompatible with the orchestrator's one-at-a-time delivery model.

**`Consumer()`**
Returns `internalConsumer` — the channel Raft uses to receive incoming RPCs. Raft goroutines blocking here are durably blocked (bubble channel), so the bubble can go idle.

**`Close()`**
Closes `closeCh`, which unblocks the bridge goroutine's `ExternalWait` select and any in-flight `makeRPC` or `forwardResponse` calls, allowing clean shutdown.

**`SetHeartbeatHandler`**
No-op. Heartbeat RPCs travel through the normal consumer channel rather than a dedicated fast path, so no separate handler is needed.

**`EncodePeer` / `DecodePeer`**
Trivial identity encoding: the address string is used directly as the peer identifier.
