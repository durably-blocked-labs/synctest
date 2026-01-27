# PCT: Probabilistic Concurrency Testing

**Paper**: "A Randomized Scheduler with Probabilistic Guarantees of Finding Bugs"
**Authors**: Sebastian Burckhardt, Pravesh Kothari, Madanlal Musuvathi, Santosh Nagarakatte
**Conference**: ASPLOS 2010
**URL**: https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/asplos277-pct.pdf

## Key Contribution

PCT provides a randomized scheduling algorithm with probabilistic guarantees for finding concurrency bugs. Unlike systematic testing approaches that exhaustively explore thread interleavings, PCT uses randomization with rigorous mathematical bounds on bug detection probability.

The fundamental insight: most concurrency bugs have small "bug depth" (few ordering constraints needed to trigger them), making randomized testing with the right guarantees highly effective.

## Bug Depth Concept

**Bug depth** (d): Minimum number of ordering constraints required to trigger a concurrency bug.

- Atomicity violations, race conditions, and deadlocks typically have small depth (d = 1-3)
- Deeper bugs are exponentially harder to find with naive random testing
- PCT provides bounded probability guarantees as a function of bug depth

## Algorithm and Methodology

### Core Mechanism

1. **Priority Assignment**: Each thread receives a randomly assigned initial priority
2. **Priority Change Points**: The algorithm selects d-1 random priority change points during execution
3. **Scheduling Rule**: Always run the highest priority thread
4. **Priority Lowering**: When a thread hits a priority change point, its priority is lowered

### Random Decisions

Total random decisions: n + d - 1
- n initial thread priorities
- d - 1 priority change points

### Probabilistic Guarantee

For a program with:
- n threads
- k execution steps
- Bug of depth d

**Detection probability per run**: ≥ 1 / (n × k^(d-1))

This is exponentially better than naive random testing for bugs with small depth.

## Advantages Over Systematic Testing

- **Scalability**: No state space explosion—runs complete in bounded time
- **Simplicity**: Easier to implement than DPOR or systematic exploration
- **Practical effectiveness**: Most real bugs have small depth
- **Parallelizable**: Multiple PCT runs are independent
- **No instrumentation overhead**: Can run at near-native speed

## Relation to Synctest

### Direct Applications

**Deterministic replay foundation**: PCT needs to control thread scheduling, similar to synctest's deterministic scheduler. The techniques for intercepting scheduling decisions transfer directly.

**Priority-based scheduling**: Go's goroutine scheduler already uses priorities internally. PCT's priority manipulation could be implemented by:
- Assigning random priorities to goroutines at runtime.Gosched() points
- Instrumenting synchronization primitives (channels, mutexes) as priority change points
- Modifying the runtime scheduler to respect PCT priorities

**Bug depth as test quality metric**: Synctest could report the "depth" of explored schedules, helping users understand test coverage quality.

### Implementation Considerations for Go

**Goroutine creation points**: `go` statements are natural priority assignment points

**Synchronization operations as change points**:
- Channel send/receive
- Mutex lock/unlock
- WaitGroup operations
- Context cancellation

**Runtime integration**: synctest's control over the scheduler enables PCT implementation without modifying user code

**Yield point instrumentation**: Go's compiler could insert runtime.Gosched() calls at function boundaries, loop backs, and blocking operations to create more scheduling opportunities.

## Relevant Techniques

### For Synctest Implementation

1. **Controlled randomization**: Even with deterministic replay, tests can use PCT mode for bug-finding phase, then replay specific schedules
2. **Depth-bounded exploration**: Combine PCT with systematic testing—use PCT to find bugs, then systematically explore all d-depth schedules around them
3. **Adaptive testing**: Run PCT with increasing depth bounds (d=1, then d=2, etc.) until bugs found or timeout
4. **Preemption points**: PCT's priority changes correspond to preemption points—synctest needs similar instrumentation

### Scheduling Interception

PCT requires intercepting all scheduler decisions:
- Thread creation/destruction
- Synchronization primitives
- Blocking operations
- Voluntary yields

Go's runtime already tracks these—synctest extends this for deterministic control.

## Important Insights

### Theoretical Results

**Probabilistic completeness**: Given enough runs, PCT will find any bug of known depth with high probability. Number of runs needed: O(n × k^(d-1))

**Comparison to random testing**: For depth-d bugs, PCT is exponentially more effective than uniform random testing

**Depth distribution**: Empirical studies show most real concurrency bugs have d ≤ 3

### Practical Results from Paper

- Found bugs in production Windows device drivers that had passed months of stress testing
- Effective on programs with thousands of threads and millions of steps
- Low overhead compared to systematic testing (10-100x faster for equivalent coverage)
- Composability: PCT works with existing unit tests without modification

### Limitations

**Unknown bug depth**: If bugs have depth d > 3, PCT may require impractical numbers of runs

**Coverage blind spots**: PCT doesn't provide guarantees about code path coverage, only scheduling coverage

**No happens-before exploitation**: Unlike DPOR, PCT doesn't prune equivalent interleavings

## Connections to Go's Scheduler

### Goroutine Scheduling Model

Go's cooperative scheduling with preemption points aligns well with PCT:
- Function calls are preemption points
- Channel operations are explicit synchronization
- Runtime yields at blocking operations

### Potential Enhancements

**Compiler-assisted instrumentation**: The Go compiler could insert PCT-aware yield points automatically in test builds

**Runtime modes**: `go test -race` mode could be complemented with `go test -pct` mode

**Seed control**: PCT's random choices should be seedable for reproducibility—Go's testing framework already supports this via `-test.seed`

## References and Further Reading

- Original paper provides formal proofs of probabilistic bounds
- PCT has been extended to weak memory models and distributed systems
- Microsoft's Coyote framework implements PCT for .NET/C# (similar to what synctest aims for Go)
- Academic follow-up work includes optimal PCT variants and depth estimation techniques
