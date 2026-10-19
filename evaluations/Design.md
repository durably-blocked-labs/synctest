# Bubbles Evaluation Framework

## Problem Statement

The goal of this evaluation is to see how effective the bubbles abstraction is at both empirically detecting bugs in distributed programs, but also how ergonomic it is to integrate into existing programs. Our previous evaluations using bubbles were benchmarking it by seeing if we would be able to surface existing bugs (like the existing ones where we try to recreate issues in hashicorp raft). Like other papers and their artifacts (TAMIAS, MODIST, Morpheus) we desire an actual framework to have use bubbles and fuzz distributed programs automatically.

## Structure

I'll work on frameworks surrounding various types of distributed programs in the aims of creating a tool that is generic enough that it is extensible enough to support bubbles practically for as many programs as we can.

These are the main programs that I'm aiming on building abstractions for:

- [HashiCorp Raft](https://github.com/hashicorp/raft): consensus, leader election, and log replication.
- [NATS JetStream](https://github.com/nats-io/nats-server): asynchronous messaging, fan-out, acknowledgements, and replicated consumer state.
- [HashiCorp Memberlist](https://github.com/hashicorp/memberlist): peer-to-peer gossip, membership changes, and failure detection.
- [Kubernetes controllers](https://github.com/kubernetes/kubernetes): concurrent work queues, retries, and reconciliation of observed and desired state.
- [IPFS Kubo](https://github.com/ipfs/kubo): peer-to-peer content-addressed storage and retrieval using cryptographic identifiers and Merkle-DAG links.

### Raft

The existing `rafttest` integration runs HashiCorp Raft nodes inside separate bubbles using in-memory stores and an orchestrator-controlled transport. It includes three-node leader-election, replay, and exploration tests, plus bug demonstrations: one intended to inject a stale-term fault and another described as a leader-removal issue in the real implementation. The transport already routes requests and responses through Bubbles, although it disables replication pipelining and uses the regular RPC path for heartbeats. We can build on this integration by extracting its repeated cluster setup and adding workloads that submit commands to the real Raft implementation; the bug demonstrations still need validation before treating them as evaluation results.

See [HashiCorp Raft Evaluation](raft-evaluation.md) for workload design and open questions.
