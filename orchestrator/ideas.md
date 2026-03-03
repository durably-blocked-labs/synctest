# Orchestrator Ideas
 
## What kind of concurrency semantics do we want to test?
- Two types of interleavings:
  - Intra-node
  - Inter-node

Intra-node
- This is concurrency semantics that occur within a single node.
- Race detectors, and synctest.RunExplore already enable us to test these interleavings.

Inter-node
- This is concurrency semantics that occur between multiple nodes.
- Inter-node contains the cross product of two or more nodes. 
- RPCs or any communication mechanism between nodes creates inter-node concurrency.

The big questions:
- What type of concurrency semantics is most meaningful to test?

Let  G_E be a set of Goroutines that are responsible for inter-node communication (RPCs). Let G_I be a set of Goroutines that are responsible for intra-node communication.

There are two types of schedules that the orchestrator needs to test:
1. Schedules that involve only inter-node communication.
2. Schedules that involve both inter-node and intra-node communication.

The tradeoffs:
- Testing (1) doesn't satisfy the completeness requirement as RPCs can also interleave with intranode goroutines.
- Testing (2) would satisfy completeness requirement, but is likely much more challenging due to the responsibility of managing intra-node goroutines.

Although (2) is more complete, prioritizing (1) may be more practical due to the complexity of managing intra-node goroutines. We can likely also surface concurrency bugs that arise from RPC interleavings as well, which is a goal of systematic testing of a distributed system.


## Testing Hashicorp Raft

Hashicorp Raft uses an InmemTransport abstraction for the transport layer. Nothing goes over the real network. 
- The "tester" can define a set of RPC names that are used in the system. This is required because their transport just uses channels to deliver RPCs. We have to distinguish whether a channel is used for inter-node or intra-node communication.

