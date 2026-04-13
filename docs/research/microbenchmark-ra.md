# Ricart-Agrawala Mutual Exclusion Bug (G+L, Depth 2)

## The Bug

`wanting` flag cleared when grants reach N-1 (should be when CS exits).
Window between `wanting=false` and `inCS=true` allows competing request
to get an unconditional grant.

## Pseudocode (~35 lines)

```go
// 3 nodes: A(0), B(1), C(2). Each bubble has recv + app goroutines.
// BUG: wanting cleared in recv when grants==2, before app sets inCS

// recv goroutine
func (n *Node) recv() {
    for {
        select {
        case m := <-n.reqCh:
            if n.inCS || (n.wanting && before(n.reqTS, n.id, m.TS, m.From)) {
                n.deferred = append(n.deferred, m.From)
            } else {
                nodes[m.From].grantCh <- Msg{From: n.id}
            }
        case <-n.grantCh:
            n.grants++
            if n.grants == 2 && n.wanting {
                n.wanting = false       // ← BUG: cleared here
                n.enterCh <- true       // wake app
            }
        }
    }
}

// app goroutine
func (n *Node) app(shared *int) {
    n.clock++; n.reqTS = n.clock; n.wanting = true; n.grants = 0
    broadcast(REQ)
    <-n.enterCh
    n.inCS = true            // ← set AFTER enterCh (too late)
    v := *shared; *shared = v + 1  // critical section
    n.inCS = false
    flush(n.deferred)
}
```

## State Space (4 traces that matter)

| G (delivery) | L (scheduling) | What happens | Safe? |
|---|---|---|---|
| FIFO (REQ before GRANTs) | FIFO (app first) | A defers B, sequential CS | Yes |
| FIFO | Non-FIFO (recv first) | A defers B (wanting=true), sequential | Yes |
| Non-FIFO (GRANTs before REQ) | FIFO (app first) | app sets inCS=true, B deferred | Yes |
| Non-FIFO | Non-FIFO (recv first) | wanting=false, inCS=false → A grants B → **both in CS** | **BUG** |

Depth 1 (either alone) is safe. Depth 2 (G+L) triggers the bug.

## Real-World Analog

Chubby/ZooKeeper lock recipes, Consul leader election. The grant-acknowledgment
handler clears "requesting" before the lock-acquisition goroutine sets "holding."
Under normal conditions, the window is too small. Under network reorder + GC pause,
a competing request slips through.
