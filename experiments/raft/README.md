# Three-voter memory register

`make test-raft` executes real HashiCorp Raft Apply → replication → quorum commit
→ FSM → future completion, pinned by `../dependencies.json`. Two concurrent
register swaps plus a subsequent swap form a tiny history. An independent
sequential-order enumerator checks replies and real-time precedence. After a
bounded healthy catch-up phase all three FSM journals must agree and account for
every acknowledged operation exactly once. Oracle mutation fixtures reject wrong
replies, ignored precedence and duplicate identities.

This INITIAL characterization places the entire three-voter cluster in **one
synctest bubble with one clock**. It uses real `InmemTransport`, `InmemStore` and
`DiscardSnapshotStore`, not the maintained distributed adapter. Cancellation stops
client admission, Shutdown futures are awaited, and client workers are joined.
Memory state is not durable recovery; snapshots, restarts, membership, dropped
RPCs, abrupt failures, retry ambiguity and serialization are outside this model.
An Apply/leader/catch-up bound is inconclusive infrastructure/model evidence, not
a library bug classification.

## Distributed integration seam (pending)

Reuse this same FSM, operation history and oracle with the maintained
`rafttest.NewRaftTransport` / `orchestrator.NodeTransport` bridge, one bubble per
voter, once joined lifecycle and strict replay qualification land. That adapter
must supply explicit request/response delivery, cancellation/join and per-node
local choices. The initial single-bubble test is **not** that integration gate,
not exact distributed replay, and not a replacement for it. No passing or skipped
test is presented as proof of the missing capability.
