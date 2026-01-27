# Synctest in Vitess: VStream Testing Analysis

## Overview

This document analyzes the use of Go's `testing/synctest` package in Vitess PR #18701, which reduced VStream test execution time from 47 seconds to 1.7 seconds (97.5% reduction). We explore the testing approach at three levels: unit tests with synctest, end-to-end tests, and a hypothetical distributed synctest.

## Table of Contents

1. [Background: Vitess and VStream](#background-vitess-and-vstream)
2. [The PR: Adding Synctest to VStream Tests](#the-pr-adding-synctest-to-vstream-tests)
3. [End-to-End Testing in Vitess](#end-to-end-testing-in-vitess)
4. [Hypothetical: Distributed Synctest](#hypothetical-distributed-synctest)

---

## Background: Vitess and VStream

### What is Vitess?

**Vitess** is a database clustering system for horizontal scaling of MySQL. It acts as middleware that:
- Shards a single logical MySQL database across multiple physical MySQL instances
- Makes sharded databases appear as a single database to applications
- Handles query routing, distributed transactions, and data movement

```
Application
     ↓
  VTgate (query router)
     ↓
  ┌──────┬──────┬──────┐
  │Shard │Shard │Shard │  ← Each is a MySQL instance
  │ -80  │80-c0 │ c0-  │     managed by VTTablet
  └──────┴──────┴──────┘
```

### What is VStream?

**VStream** is Vitess's distributed change data capture (CDC) system. It streams MySQL binlog events from multiple shards and aggregates them into a single, ordered stream.

#### Key Distributed Systems Challenges VStream Solves

**1. Change Event Ordering Across Shards**

In a sharded database, events happen simultaneously on different shards with no global clock. VStream provides approximate ordering through:

- **Skew Detection & Minimization**: When `MinimizeSkew: true`, VStream monitors event timestamps across shards
- **Delay Mechanism**: If streams diverge by more than 2 seconds, faster shards are paused until slower ones catch up
- **Heartbeat Events**: Shards send periodic heartbeats to prove liveness

**2. Resharding Transparency**

When a keyspace is resharded (e.g., 2 shards → 4 shards), VStream must seamlessly transition:

- **Journal Events**: Mark the resharding point with information about old/new shards
- **Automatic Failover**: By default, VStream migrates streams from old to new shards automatically
- **VGtid Migration**: Position tracking updates from old shard positions to new shard positions

**3. Position Tracking with VGtid**

Standard MySQL GTIDs only track position per shard. VStream uses **VGtid** (Vitess GTID):

```protobuf
message VGtid {
  repeated ShardGtid shard_gtids = 1;  // Vector of positions
}

message ShardGtid {
  string keyspace = 1;
  string shard = 2;
  string gtid = 3;  // MySQL GTID position for this shard
}
```

This is essentially a **vector clock** - it tracks position on each shard independently, enabling:
- Exactly-once delivery semantics
- Consistent resume across all shards
- Distributed snapshots

**4. Event Types**

VStream delivers structured events:
- **ROW**: Data changes (INSERT/UPDATE/DELETE)
- **FIELD**: Schema metadata
- **GTID**: Transaction boundaries
- **VGTID**: Combined positions across all shards
- **JOURNAL**: Resharding notifications
- **HEARTBEAT**: Liveness signals

---

## The PR: Adding Synctest to VStream Tests

### PR #18701 Details

- **Title**: "test: showcase using `synctest`"
- **URL**: https://github.com/vitessio/vitess/pull/18701
- **Performance Impact**: 47.349s → 1.171s (97.5% reduction)
- **Approach**: Wrap time-dependent unit tests in `synctest.Test()`

### What is Synctest?

`testing/synctest` is a Go 1.23+ package that enables testing concurrent, time-dependent code without actual time delays.

#### How Synctest Works

**1. Fake Clock System**

Each test runs in a "bubble" with a simulated clock:
- Clock starts at midnight UTC 2000-01-01
- Time only advances when all goroutines are blocked
- No actual wall-clock time passes

**2. Example**

```go
func TestTime(t *testing.T) {
    synctest.Test(t, func(t *testing.T) {
        start := time.Now()  // Returns simulated time: 2000-01-01 00:00:00

        go func() {
            time.Sleep(1 * time.Second)  // Doesn't actually sleep!
            t.Log(time.Since(start))     // logs "1s"
        }()

        time.Sleep(2 * time.Second)      // Also instant!
        t.Log(time.Since(start))         // logs "2s"
    })
    // Entire test runs in milliseconds
}
```

**3. Durably Blocked Operations**

Synctest advances time when goroutines are "durably blocked" on:
- Channel send/receive
- `sync.Cond.Wait()`
- `sync.WaitGroup.Wait()`
- `time.Sleep()`, `time.After()`, `context.WithTimeout()`

### The Test Case: TestVStreamSkew

This test validates VStream's **skew minimization logic** - the algorithm that prevents streams from diverging.

#### Test Setup (Main Branch - Before Synctest)

```go
func TestVStreamSkew(t *testing.T) {
    stream := func(conn *sandboxconn.SandboxConn, keyspace, shard string, count, idx int64) {
        vevents := getVEvents(keyspace, shard, count, idx)
        for _, ev := range vevents {
            conn.VStreamCh <- ev
            time.Sleep(time.Duration(idx*100) * time.Millisecond)  // ACTUAL WAITING!
        }
    }

    // Test cases with different stream speeds
    tcases := []*skewTestCase{
        // Shard0: 100ms/event, Shard1: 200ms/event → 2 delays expected
        {numEventsPerShard: 4, shard0idx: 1, shard1idx: 2, expectedDelays: 2},

        // Both at same speed → no delays
        {numEventsPerShard: 4, shard0idx: 1, shard1idx: 1, expectedDelays: 0},
    }

    for idx, tcase := range tcases {
        // ... setup shards and VStream ...

        // Start streaming with delays
        go stream(sbc0, ks, "-20", tcase.numEventsPerShard, tcase.shard0idx)
        go stream(sbc1, ks, "20-40", tcase.numEventsPerShard, tcase.shard1idx)

        // Wait for events with 1-minute timeout
        vstreamCtx, cancel := context.WithTimeout(ctx, 1*time.Minute)
        defer cancel()

        err := vsm.VStream(vstreamCtx, /* ... */)

        // Verify delays happened
        require.Equal(t, tcase.expectedDelays, vsm.GetTotalStreamDelay()-previousDelays)
    }
}
```

#### What's Being Tested

The test validates this scenario:

```
Time     Shard0 (-20)        Shard1 (20-40)      VStream Action
─────────────────────────────────────────────────────────────────
t=0      event1 sent
t=100ms  event2 sent         event1 sent         ✓ Both arrive
t=200ms  event3 sent                             ⚠ Shard0 ahead by >200ms
t=300ms  event4 sent         event2 sent         🛑 DELAY Shard0 until Shard1 catches up
t=400ms                      event3 sent         ✓ Allow Shard0 event3
t=500ms                      event4 sent         ✓ Streams aligned
```

**Expected Outcome**: 2 delay events (when Shard0 had to wait for Shard1)

#### Time Breakdown (Without Synctest)

For one test case with `idx=2`:
- 4 events × 200ms delay = 800ms per shard
- Multiple test cases
- 1-minute timeout overhead
- Context coordination
- **Total: 47.349 seconds**

#### The Synctest Change

```diff
  for idx, tcase := range tcases {
      t.Run("", func(t *testing.T) {
+         synctest.Test(t, func(t *testing.T) {
              ctx, cancel := context.WithCancel(context.Background())
              defer cancel()

              // ... IDENTICAL test code ...
              // time.Sleep() is now instant!
              // context.WithTimeout() is now instant!
+         })
      })
  }
```

**That's it!** Just wrapping in `synctest.Test()` makes all time operations instant.

#### Why It Works

1. `time.Sleep(200*time.Millisecond)` in `stream()` → **instant**, clock jumps +200ms
2. `context.WithTimeout(ctx, 1*time.Minute)` → **instant** timeout tracking
3. All goroutine coordination → **deterministic**
4. Test behavior → **identical**
5. Execution time → **1.171 seconds** (mostly test setup overhead)

### Key Insight

Synctest doesn't mock or fake the logic. It accelerates **time itself**. The VStream code runs exactly as it would in production, just without waiting for real time to pass.

---

## End-to-End Testing in Vitess

While synctest validates the **algorithm**, end-to-end tests validate **actual distributed behavior**.

### E2E Test Infrastructure

Vitess e2e tests spin up **real processes**:

```go
type VitessCluster struct {
    Topo          *TopoProcess      // Real etcd/consul/zookeeper
    Vtctld        *VtctldProcess    // Real control plane
    Cells         map[string]*Cell  // Multiple data centers
}

type Cell struct {
    Keyspaces map[string]*Keyspace
    Vtgates   []*VtgateProcess     // Real VTgate instances
}

type Keyspace struct {
    Shards map[string]*Shard       // Real sharded keyspace
}

type Shard struct {
    Tablets map[string]*Tablet     // Real MySQL + VTtablet
}
```

### Example: TestMultiVStreamsKeyspaceReshard

Location: `/go/test/endtoend/vreplication/vstream_test.go:708`

This test validates VStream during an **actual resharding operation**.

#### Phase 1: Setup (Lines 717-746)

```go
vc = NewVitessCluster(t, nil)

// Create 2-shard keyspace
oldShards := "-80,80-"
keyspace, err := vc.AddKeyspace(t, []*Cell{defaultCell}, ks, oldShards, ...)

// Add 4 new target shards
newShards := "-40,40-80,80-c0,c0-"
err = vc.AddShards(t, []*Cell{defaultCell}, keyspace, newShards, ...)
```

**What actually runs**:
- 6 MySQL instances (2 old + 4 new shards)
- 6 VTtablet processes managing them
- 1 VTgate routing queries
- Topology service (etcd) tracking everything

#### Phase 2: Stream from Old Shards (Lines 755-841)

```go
// Continuously insert data
go func() {
    id := 1
    for {
        select {
        case <-streamCtx.Done():
            close(done)
            return
        default:
            insertRow(ks, "customer", id)
            time.Sleep(250 * time.Millisecond)  // Real inserts every 250ms!
            id++
        }
    }
}()

// Start VStream
vgtid := &binlogdatapb.VGtid{
    ShardGtids: []*binlogdatapb.ShardGtid{{
        Keyspace: "/.*",  // Match all keyspaces
    }},
}

reader, err := vstreamConn.VStream(ctx, topodatapb.TabletType_PRIMARY, vgtid, filter, flags)

// Consume events until we have positions on old shards
for {
    evs, err := reader.Recv()
    for _, ev := range evs {
        if ev.Type == binlogdatapb.VEventType_ROW {
            shard := ev.GetRowEvent().GetShard()
            if shard == "-80" || shard == "80-" {
                oldShardRowEvents++
            }
        }
        if ev.Type == binlogdatapb.VEventType_VGTID {
            newVGTID = ev.GetVgtid()  // Save current position
        }
    }
}
```

#### Phase 3: Perform Actual Reshard (Line 774)

```go
// Start reshard workflow - this is REAL!
reshardAction(t, "Create", wf, ks, oldShards, newShards, ...)

// VReplication now:
// 1. Copies all existing data from 2 shards to 4 shards
// 2. Streams ongoing changes to keep them in sync
// 3. Waits until target is caught up
```

**What's happening under the hood**:
- VReplication creates 4 VStream connections (one per new shard)
- Each streams a filtered subset of data from old shards
- Data is transformed based on new sharding key ranges
- Both historical (copy) and ongoing (streaming) data

#### Phase 4: Switch Traffic (Line 849)

```go
// Atomically switch traffic from old → new shards
reshardAction(t, "SwitchTraffic", wf, ks, oldShards, newShards, ...)

// Now:
// - Old shards (-80, 80-) become non-serving
// - New shards (-40, 40-80, 80-c0, c0-) are now serving
// - VStream must detect and adapt!
```

#### Phase 5: Resume Stream & Verify (Lines 852-900)

```go
// Resume VStream from saved VGtid (which only has OLD shard positions!)
reader, err := vstreamConn.VStream(ctx, topodatapb.TabletType_PRIMARY, newVGTID, filter, flags)

// Continue consuming - VStream should handle reshard transparently
for {
    evs, err := reader.Recv()
    for _, ev := range evs {
        switch ev.Type {
        case binlogdatapb.VEventType_ROW:
            shard := ev.RowEvent.Shard
            switch shard {
            case "-80", "80-":
                oldShardRowEvents++    // Still getting from old shards
            case "-40", "40-80", "80-c0", "c0-":
                newShardRowEvents++    // Now also from new shards!
            }

        case binlogdatapb.VEventType_JOURNAL:
            // Got notification of the reshard!
            require.True(t, ev.Journal.MigrationType == binlogdatapb.MigrationType_SHARDS)
            journalEvents++
        }
    }
}

// VERIFICATION: Distributed correctness properties

// 1. Events from both phases
require.Greater(t, oldShardRowEvents, 0)    // Saw events before reshard
require.Greater(t, newShardRowEvents, 0)    // Saw events after reshard

// 2. Resharding was detected
require.Greater(t, journalEvents, 0)

// 3. CRITICAL: No data loss across 6 MySQL instances!
customerResult := execQuery("select count(*) from customer")
customerCount, err := customerResult.Rows[0][0].ToInt64()
require.Equal(t, customerCount, int64(oldShardRowEvents + newShardRowEvents))
```

### What This E2E Test Validates

From a distributed systems perspective:

1. **Transparent Topology Changes**
   - VStream automatically follows data as it moves between shards
   - Clients resume from old VGtid, get events from new shards
   - No manual intervention required

2. **Exactly-Once Delivery**
   - Every row inserted appears exactly once in the stream
   - No duplicates despite data moving across 6 MySQL instances
   - No gaps despite topology changing mid-stream

3. **Vector Clock Semantics**
   - VGtid with `["-80": gtid1, "80-": gtid2]`
   - Automatically transitions to `["-40": gtid3, "40-80": gtid4, "80-c0": gtid5, "c0-": gtid6]`
   - Resume semantics preserved across topology changes

4. **Causal Consistency**
   - Events maintain causal order within each shard
   - Cross-shard ordering respects JOURNAL events
   - No "time travel" - once you see event E, you won't see earlier events

5. **Liveness Under Concurrent Mutations**
   - Inserts continue during entire reshard process
   - Stream catches all changes
   - No blocking or stalling

### Why Both Test Levels Matter

| Aspect | Unit Test (synctest) | E2E Test (real) |
|--------|---------------------|-----------------|
| **What it tests** | Skew detection algorithm | Full distributed behavior |
| **Speed** | 1.7 seconds | 5-10 minutes |
| **Determinism** | Perfect | Good (real timing) |
| **Coverage** | Timing/coordination logic | Integration across components |
| **MySQL** | Mocked (channels) | Real binlog parsing |
| **Network** | In-process | Real TCP/gRPC |
| **Failures** | Controlled scenarios | Harder to inject |
| **CI Cost** | Negligible | Expensive |

**Complementary Strategy**:
- Synctest proves the **algorithm is correct**
- E2E proves **everything integrates correctly**
- Together: high confidence in production correctness

---

## Hypothetical: Distributed Synctest

### The Vision

What if we could combine synctest's speed with e2e test's realism? A **distributed synctest** would simulate the entire distributed environment, not just time.

### Hypothetical API

```go
func TestVStreamReshardWithClockSkew(t *testing.T) {
    dsynctest.RunDistributed(t, func(sim *dsynctest.Simulator) {
        // Create virtual nodes with independent clocks
        shard1 := sim.NewNode("shard-80")
        shard2 := sim.NewNode("shard80-")
        vtgate := sim.NewNode("vtgate")

        // CLOCK SKEW: A real distributed systems problem
        shard1.Clock().SetSkew(+500 * time.Millisecond)  // 500ms ahead
        shard2.Clock().SetSkew(-300 * time.Millisecond)  // 300ms behind

        // Simulated network with controllable properties
        network := sim.Network()
        network.SetLatency(shard1, vtgate, 10*time.Millisecond)
        network.SetLatency(shard2, vtgate, 50*time.Millisecond)  // Asymmetric!

        // Run actual VStream code (simulated environment)
        vstream := vtgate.VStream(ctx, vgtid, filter)

        // Inject events at specific virtual times
        sim.At(1*time.Second, func() {
            shard1.InsertRow("user", 1)
        })
        sim.At(1*time.Second, func() {  // Same virtual time!
            shard2.InsertRow("user", 2)
        })

        // Fast-forward to t=10s instantly
        sim.AdvanceTo(10 * time.Second)

        // Verify behavior
        assert.Equal(t, 2, vstream.ReceivedEvents())
    })
}
```

### Key Capabilities

#### 1. Simulated Network

```go
// Network partition
sim.Network().Partition(shard1, vtgate)
sim.AdvanceBy(5 * time.Second)
// VStream should detect failure and retry

// Packet loss
sim.Network().SetPacketLoss(shard2, vtgate, 0.1)  // 10% loss

// Message reordering
sim.Network().SetReorderProbability(0.05)  // 5% chance

// Latency distribution
sim.Network().SetLatency(shard1, vtgate,
    dist.Normal{Mean: 10*time.Millisecond, StdDev: 2*time.Millisecond})
```

#### 2. Independent Clocks (Clock Skew)

```go
// Each node has its own clock that can drift
shard1.Clock().SetDrift(+50*time.Microsecond/time.Second)  // Fast clock
shard2.Clock().SetDrift(-30*time.Microsecond/time.Second)  // Slow clock

// After 1 simulated hour
sim.AdvanceBy(1 * time.Hour)

// shard1 is now 180ms ahead of shard2
// Perfect for testing skew detection!
```

#### 3. Deterministic Execution

```go
// Run same test 1000 times with same seed = same result
sim.SetRandomSeed(12345)

// Or explore the state space
for seed := 0; seed < 10000; seed++ {
    sim.SetRandomSeed(seed)
    runTest()
    // Finds rare race conditions!
}
```

#### 4. Failure Injection

```go
// Node crash at specific time
sim.At(5*time.Second, func() {
    shard1.Crash()  // Simulates process death
})

// Slow node (tail latency)
shard2.SetProcessingDelay(500 * time.Millisecond)

// Byzantine behavior
shard1.SetBehavior(dsynctest.Byzantine{
    DuplicateMessages: true,
    CorruptData: 0.01,  // 1% corruption
})

// Disk full
shard2.Disk().SetSpace(0)  // Triggers ENOSPC errors
```

#### 5. Time-Travel Debugging

```go
sim.RecordHistory()

// Run until bug found
sim.RunUntil(func() bool {
    return vstream.HasInconsistency()
})

// Rewind and inspect
sim.RewindTo(5 * time.Second)
fmt.Println("Shard1 state:", shard1.Inspect())
fmt.Println("Network queue:", network.InspectQueue(shard1, vtgate))

// Single-step through events
sim.Step()  // Process next event in event queue
sim.Step()
```

### Example Scenarios

#### Scenario 1: Clock Skew Causes Reordering

```go
func TestVStreamSkewCausesReordering(t *testing.T) {
    dsynctest.RunDistributed(t, func(sim *dsynctest.Simulator) {
        shard1 := sim.NewShard("-80")
        shard2 := sim.NewShard("80-")

        // EXTREME clock skew (exaggerated for testing)
        shard1.Clock().SetSkew(+10 * time.Second)

        // Insert order (wall clock time):
        // t=1s: user1 on shard1
        // t=2s: user2 on shard2
        sim.At(1*time.Second, func() { shard1.Insert("user", 1) })
        sim.At(2*time.Second, func() { shard2.Insert("user", 2) })

        // But shard1's timestamp will be t=11s, shard2's will be t=2s
        // Without MinimizeSkew, events arrive as [user2, user1] - WRONG!

        vstream := vtgate.VStream(ctx, vgtid, &VStreamFlags{
            MinimizeSkew: false,  // Bug reproducer
        })

        sim.AdvanceTo(5 * time.Second)

        // Verify bug
        events := vstream.GetEvents()
        assert.Equal(t, 2, events[0].RowID, "user2 received first - BUG!")
        assert.Equal(t, 1, events[1].RowID, "user1 received second")
    })

    // Now test with MinimizeSkew enabled
    dsynctest.RunDistributed(t, func(sim *dsynctest.Simulator) {
        // ... same setup ...

        vstream := vtgate.VStream(ctx, vgtid, &VStreamFlags{
            MinimizeSkew: true,  // Fix enabled
        })

        sim.AdvanceTo(5 * time.Second)

        // Verify fix - order should be closer to wall-clock time
        events := vstream.GetEvents()
        // Events may not be perfectly ordered, but skew should be bounded
        for i := 1; i < len(events); i++ {
            timeDiff := events[i].Timestamp - events[i-1].Timestamp
            assert.LessOrEqual(t, timeDiff, 2*time.Second,
                "Skew exceeded threshold at event %d", i)
        }
    })
}
```

#### Scenario 2: Network Partition During Reshard

```go
func TestVStreamReshardWithPartition(t *testing.T) {
    dsynctest.RunDistributed(t, func(sim *dsynctest.Simulator) {
        // Setup: initial shard being split
        oldShard := sim.NewShard("-")
        newShard1 := sim.NewShard("-80")
        newShard2 := sim.NewShard("80-")
        vtgate := sim.NewVTGate()

        // Start VStream
        vstream := vtgate.VStream(ctx, vgtid, filter, &VStreamFlags{})

        // Start reshard at t=5s
        sim.At(5*time.Second, func() {
            sim.StartReshard("-", []string{"-80", "80-"})
        })

        // NETWORK PARTITION at critical moment (t=5.5s)
        sim.At(5.5*time.Second, func() {
            sim.Network().Partition(newShard1, vtgate)
        })

        // Insert continues on partitioned shard
        sim.At(6*time.Second, func() {
            newShard1.Insert("user", 100)  // VTgate can't see this!
        })

        // Heal partition at t=8s
        sim.At(8*time.Second, func() {
            sim.Network().HealPartition(newShard1, vtgate)
        })

        // Run to completion
        sim.AdvanceTo(10 * time.Second)

        // VERIFY: VStream should have retried and caught up
        events := vstream.GetAllEvents()
        assert.Contains(t, events, RowEvent{ID: 100}, "No data loss!")

        // Verify monotonic VGTID updates
        vgtids := vstream.GetVGtidHistory()
        for i := 1; i < len(vgtids); i++ {
            assert.True(t, vgtids[i].IsAfter(vgtids[i-1]),
                "VGtid went backwards at position %d", i)
        }
    })
}
```

#### Scenario 3: Slow Shard Causes Skew

```go
func TestVStreamSlowShardHandling(t *testing.T) {
    dsynctest.RunDistributed(t, func(sim *dsynctest.Simulator) {
        fastShard := sim.NewShard("-80")
        slowShard := sim.NewShard("80-")

        // Slow shard has 500ms processing delay per event
        slowShard.SetProcessingDelay(500 * time.Millisecond)

        vtgate := sim.NewVTGate()
        vstream := vtgate.VStream(ctx, vgtid, &VStreamFlags{
            MinimizeSkew: true,
        })

        // Fast shard gets 100 events
        for i := 0; i < 100; i++ {
            sim.At(time.Duration(i)*10*time.Millisecond, func() {
                fastShard.Insert("user", i)
            })
        }

        // Slow shard gets 10 events
        for i := 0; i < 10; i++ {
            sim.At(time.Duration(i)*100*time.Millisecond, func() {
                slowShard.Insert("user", 1000+i)
            })
        }

        sim.AdvanceTo(10 * time.Second)

        // VERIFY: Fast shard should have been delayed
        delays := vstream.GetSkewDelays()
        assert.Greater(t, len(delays), 0, "Fast shard should have been delayed")

        // VERIFY: All events delivered
        assert.Equal(t, 110, vstream.EventCount())

        // VERIFY: Events roughly in timestamp order
        events := vstream.GetAllEvents()
        maxSkew := time.Duration(0)
        for i := 1; i < len(events); i++ {
            skew := events[i].Timestamp - events[i-1].Timestamp
            if skew > maxSkew {
                maxSkew = skew
            }
        }
        assert.LessOrEqual(t, maxSkew, 2*time.Second, "Max skew exceeded threshold")

        // PERFORMANCE: This ran in milliseconds, not 10+ seconds!
    })
}
```

#### Scenario 4: Tablet Failover During Stream

```go
func TestVStreamTabletFailover(t *testing.T) {
    dsynctest.RunDistributed(t, func(sim *dsynctest.Simulator) {
        // Shard with primary and replica
        shard := sim.NewShard("-80")
        primary := shard.AddTablet("primary", TabletTypePRIMARY)
        replica := shard.AddTablet("replica", TabletTypeREPLICA)

        vtgate := sim.NewVTGate()
        vstream := vtgate.VStream(ctx, vgtid, filter, &VStreamFlags{})

        // Insert 50 events
        for i := 0; i < 50; i++ {
            sim.At(time.Duration(i)*100*time.Millisecond, func() {
                primary.Insert("user", i)
            })
        }

        // Primary crashes at t=3s
        sim.At(3*time.Second, func() {
            primary.Crash()
        })

        // Replica promoted to primary at t=3.5s
        sim.At(3.5*time.Second, func() {
            shard.PromoteToPrimary(replica)
        })

        // Continue inserting to new primary
        for i := 50; i < 100; i++ {
            sim.At(time.Duration(i)*100*time.Millisecond, func() {
                replica.Insert("user", i)
            })
        }

        sim.AdvanceTo(15 * time.Second)

        // VERIFY: All 100 events received
        assert.Equal(t, 100, vstream.EventCount())

        // VERIFY: Stream detected failure and reconnected
        connections := vstream.GetConnectionHistory()
        assert.Len(t, connections, 2, "Should have reconnected once")
        assert.Equal(t, primary.ID, connections[0].TabletID)
        assert.Equal(t, replica.ID, connections[1].TabletID)

        // VERIFY: No duplicate events
        eventIDs := vstream.GetEventIDs()
        assert.Len(t, eventIDs, 100, "Should have unique events")
    })
}
```

### Real-World Inspiration: FoundationDB

FoundationDB actually uses simulation testing extensively:

```cpp
// Simplified from FoundationDB's approach
class Simulation {
    // Simulated network
    void send(Address from, Address to, Packet p) {
        double latency = randomLatency();
        scheduleAt(now() + latency, [=]() {
            if (!isPartitioned(from, to)) {
                deliver(to, p);
            }
        });
    }

    // Simulated disk
    Future<Data> read(Disk d, Offset o) {
        double latency = randomDiskLatency();
        scheduleAt(now() + latency, [=]() {
            return diskData[d][o];
        });
    }

    // Simulated time - doesn't actually wait!
    double now() { return virtualTime; }
    void sleep(double duration) {
        scheduleAt(now() + duration, continuation);
    }
};

// Run billions of simulated hours in CI
for (int test = 0; test < 100000; test++) {
    Simulation sim(randomSeed());
    sim.run(100 * HOURS);  // Takes seconds, not hours!
    assert(sim.isConsistent());
}
```

**Results**: FoundationDB found **thousands of bugs** that would be nearly impossible to find with traditional testing, including:
- Rare race conditions (1 in 10,000 runs)
- Byzantine failure scenarios
- Clock skew interactions
- Network partition edge cases

### Benefits for Vitess

| Benefit | Description | Impact |
|---------|-------------|--------|
| **Speed** | Run 10 simulated hours in 1 second | E2E: 10 min → Sim: 100ms |
| **Coverage** | Test impossible scenarios | Clock skew, partition timing, Byzantine faults |
| **Determinism** | Same seed = same execution | No flaky tests, perfect reproduction |
| **Debugging** | Time-travel debugging | Rewind, inspect, single-step |
| **Exhaustiveness** | Run 10,000 seeds overnight | Find 1-in-10,000 bugs |
| **Cost** | Cheap CI resources | Run 1000x more tests |

### Implementation Challenges

**Challenge 1: MySQL is External**

Current architecture:
```
VTgate → gRPC → VTtablet → MySQL binlog
```

Solutions:
- **Option A**: Mock MySQL binlog at protocol level
- **Option B**: Build simulated MySQL (huge effort)
- **Option C**: Intercept at VTtablet vstreamer layer (more feasible)

**Challenge 2: Process Boundaries**

Current: Multiple processes (VTgate, VTtablet, MySQL)
Needed: All in one process with simulated IPC

**Challenge 3: Go Runtime Limitations**

- `synctest` works because Go runtime is controllable
- Network I/O crosses into OS kernel - harder to intercept
- Would need custom network stack or eBPF-level interception

### Practical Implementation Path

**Phase 1: Protocol-Level Simulation**

```go
// Intercept at gRPC layer
type SimulatedTabletConn struct {
    sim *Simulator
    realTablet *TabletSimulator
}

func (c *SimulatedTabletConn) VStream(ctx, req) (stream, error) {
    // Goes through simulator's network
    msg := &VStreamRequest{...}
    c.sim.Network().Send(c.Address, c.TabletAddress, msg)
    return c.sim.ReceiveStream(ctx)
}
```

**Phase 2: Composable Simulations**

```go
// Combine synctest + network simulation
synctest.Test(t, func(t *testing.T) {
    sim := dsynctest.New()

    // Goroutines get synctest time
    // Network calls get simulated network
    // Best of both worlds!
})
```

**Phase 3: Full Stack Simulation**

Implement simulated versions of:
- MySQL binlog protocol
- VTtablet vstreamer
- VTgate VStream coordinator
- Network layer (gRPC)
- Time layer (clocks)

All running in one process with full control.

---

## Summary

### Three Levels of Testing

| Level | What | Speed | Coverage | Best For |
|-------|------|-------|----------|----------|
| **Unit + Synctest** | Algorithm logic | 1-2s | Timing scenarios | Algorithm correctness |
| **E2E** | Full integration | 5-10min | Real behavior | Integration correctness |
| **Distributed Sim** | Simulated distributed | 100ms | Edge cases | Distributed correctness |

### Key Takeaways

1. **Synctest (PR #18701)**
   - Accelerates time without changing behavior
   - 97.5% test time reduction (47s → 1.7s)
   - Perfect for testing time-dependent algorithms

2. **E2E Tests**
   - Validates real distributed behavior
   - Tests actual resharding, failover, consistency
   - Expensive but essential for confidence

3. **Distributed Synctest (Future)**
   - Would combine speed of synctest with realism of e2e
   - Enable testing rare distributed systems bugs
   - Inspired by FoundationDB's success
   - Significant engineering investment required

### The Testing Pyramid for Distributed Systems

```
        ┌──────────────┐
        │  E2E Tests   │  ← Slow, expensive, high confidence
        │   (Real)     │
        ├──────────────┤
        │ Dist Synctest│  ← Fast, comprehensive, high coverage
        │ (Simulated)  │
        ├──────────────┤
        │Unit+Synctest │  ← Fastest, algorithm validation
        │  (Mocked)    │
        └──────────────┘
```

Each level serves a purpose. The combination provides comprehensive validation of distributed systems correctness.

---

## References

- **PR #18701**: https://github.com/vitessio/vitess/pull/18701
- **Go synctest package**: https://pkg.go.dev/testing/synctest
- **Go blog on synctest**: https://go.dev/blog/synctest
- **FoundationDB Simulation**: https://apple.github.io/foundationdb/testing.html
- **Vitess VStream docs**: https://vitess.io/docs/design-docs/vreplication/vstream/

---

*Document created: 2026-01-13*
*Author: Analysis of Vitess PR #18701 and distributed systems testing approaches*
