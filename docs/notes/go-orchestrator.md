The end goal of the orchestrator is to have it manage, schedule, and explore interleavings between X synctest bubbles (X nodes in a distributed system).

However, for phase one, we just want an MVP; what we want is to be able to explore interleavings for two different nodes.

An example scenario that demonstrates the orchestrator's responsibility for the MVP is highlighted below. 

Two bubbles: Bubble A (RPC Client), Bubble B (RPC Server)

Bubble A would run its test program independently using synctest.Test(). Bubble A would then send an RPC to Bubble B. Bubble A would detect that the RPC is a global goroutine and incremented the external counter. It would then use the Synctest.DecisionHook to notify that Bubble A sent an RPC, and thus becomes blocked. 

Orchestrator would see this RPC and figure out what to do. For now, what it needs to do is that it waits for Bubble B to become blocked. Once Bubble B is blocked (bubble can no longer make progress), it notifies the orchestrator via the Synctest.DecisionHook. When both bubble A and bubble B are blocked, then the RPC is delievered to B. 

B then continues running the synctest test which explores its interleavings as normal and responds to the RPC request. Bubble B should terminate at some point.

Then when bubble B responds to the RPC, the orchestrator can unblock bubble A

When the bubbles talk to the orchestrator through the hook, it gets access to the bubble state, more importantly the decisions it took up to the point of being blocked.This is useful information, but I'm not sure how to use it yet. 


Questions:
- how does bubble B tell orchestrator that it's blocked