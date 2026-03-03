### Goal

We want to be able to test the /raft implementation. The ultimate goal is to write a test that uses our implementation to find a concucurrency bug (either through natural testing or mutation testing). Their Raft implementation already allows us to build our own transport layer as there's an easy interface to use. This will be useful. 

## Requirements/Concepts

When all nodes (bubbles) are blocked:
- there are no scheduleable operations
- check if there are any pending timers to be fired so we can advance the virtual clock
- if no pending timers, then set timeout and if still no pending operations/timers, then declare a deadlock.
1. 
When there are pending messages:
- randomly select message (can be FIFO for now) off the queue and process it

Orchestrator Data Structures:
- some global pool of pending scheduable operations (messages to deliver)
- some global pool of pending blocked operations (receive ops)
  - a non-schedulable operation could be a receive Goroutine waiting for a message that only another node can send. 
  - when the orchestrator receives such message, it puts it into this pool

Orchestrator Functions:
- when it receives a message, it puts it into the appropriate pool
- if it gets a receive (maybe rpc listener goroutine does a yield to orchestrator), the orchestrator should look for any pending schedulable operations that can unblock this receive operation. If it finds one, it will add it to the schedulable operations pool. Else the orchestrator sees that this is blocking and the operation is put into the blocked operations pool. However, the order of operations must be preserved. So scanning the operations must be in order of arrival time. 
- The orchestrator should also keep track of all decisions so that replayability is possible. 

Orchestrator Virtual Clock:
- all operation execute in zero virtual time
- orchestrator has a sorted deadline queue with their virtual deadline. Fast forwarding happens when there are no operations available except for pending deadlines.

The bubbles:
- orchestrator should launch the bubbles (nodes).
- they forward RPC messages to the orhcestrator via CallExternal
- The bubbles should also operate on the same virtual time.

Design Decisions:
- Flushing all pending operations before firing timeouts
  - Timeouts are usually defensive. They're there to handle cases where a message never arrives, not cases where it arrives slightly late. A timeout that fires while the other node is mid-computation is much less likely to represent a real bug scenario than one that fires because the other node crashed or got partitioned.

Operations:
- RPC send/receive

Assumptions:
- Go already enforces a strict concurrency model and any intranode concurrency or races would have been tested with other software. 
- The only place where goroutines can interfere is through messaging boundaries.
