# Related Work — Comprehensive Survey

## Go-Specific

- **GFuzz** (ASPLOS 2022) — channel/select fuzzing for Go. Found bugs in Docker, K8s. Black-box mutation vs our white-box control.
- **Go Concurrency Bug Study** (ASPLOS 2019) — 171 bugs in Docker/K8s/etcd. 58% caused by message passing (channels), not shared memory. Validates our approach.
- **Polar Signals DST** (2024) — independently forked Go runtime, controlled cheaprand with seed. Replay only, no exploration. We go much further.
- **Go Issue #33702** — proposed `ExecuteDeterministically()`. Never accepted. We implemented it.

## Single-Node Tools (Other Languages)

- **CHESS** (OSDI 2008) — Microsoft. API-level thread interposition. Preemption bounding. We are CHESS-for-Go but deeper (runtime-level).
- **Fray** (OOPSLA 2025) — JVM shadow locking. PCT/POS/SURW. Found 18 bugs in Kafka/Lucene/Guava. Closest analog but for JVM.
- **Coyote** (Microsoft) — .NET Task scheduler replacement. PCT/random. Used by Azure.
- **Loom** (Rust) — exhaustive permutation testing. DPOR + preemption bound 2-3. Requires `loom::` types.
- **Shuttle** (AWS, Rust) — randomized (PCT). Trades soundness for scalability.

## Distributed System Testing

- **FlyMC** (EuroSys 2019) — message independence + symmetry + parallel flips. 16x faster. Found 10 new bugs.
- **Morpheus** (ASPLOS 2020) — Erlang. POS + conflict analysis. Found 11 bugs. Closest distributed analog.
- **SandTable** (EuroSys 2024) — specification-level exploration. 23 bugs in Raft/Zab. 114x-2989x faster than impl-level.
- **Mocket** (EuroSys 2023) — TLA+ model checking → test case generation for Java Raft.
- **DSTest** (ISSTA 2024) — language-independent message interception. Random/PCT/POS.
- **Jepsen** — black-box fault injection. Real systems, real networks. Complementary to us.
- **FoundationDB** — gold standard DST. Flow language, single-threaded simulation. We're similar but for standard Go.
- **Antithesis** — deterministic hypervisor. VM-level control. $105M Series A. Language-agnostic.
- **Turmoil** (Rust/Tokio) — simulated networking in single thread. Close to our multi-bubble model.
- **DSLabs** (UW) — educational. BFS model checking of Java distributed systems.
- **Stateright** (Rust) — same code for model checking and production.

## Scheduling Algorithms

- **DPOR** (POPL 2005) — persistent sets, independence. We should do this for messages.
- **Source-DPOR / Optimal DPOR** (POPL 2014) — exactly one execution per Mazurkiewicz trace.
- **Reconciling Preemption Bounding with DPOR** (TACAS 2023) — directly applicable to us.
- **POS** (CAV 2018) — event-level randomization. Better than PCT for distributed.
- **SURW** (ASPLOS 2025) — near-uniform exploration. State-of-the-art randomized.
- **Greybox Fuzzing for Concurrency** (ASPLOS 2024) — reads-from coverage-guided schedule fuzzing.

## Our Unique Contributions

1. Synctest bubbles as scheduling isolation primitive (no other system has this)
2. Two-level exploration: local goroutine scheduling + global message delivery
3. Go-native runtime integration (cheaprand, findRunnable, selectgo)
4. Deterministic select ordering via selectCounter
5. Works on production Go code (hashicorp/raft) not toy programs
