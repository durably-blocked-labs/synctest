# Progress Report — Directed Studies

## Last week (Mar 12)

Allan built the global orchestrator and raft transport on top of the runtime primitives, getting a 3-node raft election running deterministically with record/replay/explore. We worked on demonstrating two concurrency bugs:

- **Bug #1 (stale term)**: Injected a `SkipTermCheck` flag in raft's `appendEntries`. Under FIFO delivery the bug is invisible. Under reordered delivery, a stale AppendEntries from an old leader corrupts follower state. [TestStaleTermConcept](https://github.com/durably-blocked-labs/synctest/blob/explore-bugs/rafttest/bug_demo_test.go)

- **Bug #6 (RemoveLeader select race)**: A real bug in hashicorp/raft — after `RemoveServer(leader)` commits, the leader's `select` can process applies before stepping down. We added deterministic select ordering to the runtime (`selectCounter` in `selectgo`). [TestRemoveLeaderConcept](https://github.com/durably-blocked-labs/synctest/blob/explore-bugs/rafttest/bug_demo_test.go)

Bug explanations: [rafttest/BUGS.md](https://github.com/durably-blocked-labs/synctest/blob/explore-bugs/rafttest/BUGS.md)

## This week (Mar 19)

Formalized the model and did research positioning. Read 8 papers (CHESS, DPOR, PCT, Fray, FlyMC, Morpheus, SandTable, MACE), surveyed 20+ related tools.

Updated the formal model with the distributed extension: [docs/model.md](https://github.com/durably-blocked-labs/synctest/blob/explore-bugs/docs/model.md). Key addition: the complete trace is a flat sequence of rounds (node, local decisions, global decision). This structure makes branching for exploration straightforward — change one decision, replay prefix, run fresh.

Found gaps between model and implementation: [TODO_model_vs_reality.md](https://github.com/durably-blocked-labs/synctest/blob/explore-bugs/docs/TODO_model_vs_reality.md). Biggest: idle detection and timer control should be gated on whether a hook is set, not on `externalWait > 0`.

Research positioning: we are CHESS-for-Go at the runtime level + distributed. Unique contributions: synctest bubbles as isolation primitive, two-level exploration (goroutine + message), deterministic select. [docs/research/](https://github.com/durably-blocked-labs/synctest/tree/explore-bugs/docs/research)

Open question: **performance**. Each raft trace takes ~30s (~2000 goroutine decisions). FlyMC operates at message granularity (~50 decisions). We're investigating scoping the bubble to only code under test.

## Branch

https://github.com/durably-blocked-labs/synctest/tree/explore-bugs
