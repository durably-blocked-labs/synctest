CPSC 448 Directed Studies Proposal
**Deterministic Scheduling for Systematic Exploration of Concurrent Interleavings in Go**

**Shubhaankar Sharma**
Term: Winter 2025 Term 2
Supervisors: Arpan Gujarati, Ivan Beschastnikh

## Introduction

Concurrency bugs are difficult to reproduce and test. A test might pass thousands of times and fail once due to a specific thread interleaving. Production systems hit race conditions that never appeared during development. The core challenge is that concurrent execution order is non-deterministic.

This project investigates whether deterministic scheduling is feasible in Go by validating whether controlling the scheduler's randomness source is sufficient for reproducible execution. If validation succeeds, we will prototype systematic exploration techniques to find concurrency bugs that traditional testing misses.

## Background: Synctest & Scheduler

### Synctest and Time Determinism

Go 1.23 introduced [testing/synctest](https://github.com/golang/go/blob/master/src/testing/synctest/synctest.go), a package that creates isolated execution environments called "bubbles." Every goroutine in a bubble shares virtual time and is tracked by the runtime. The key concept is "durable blocking": a goroutine is durably blocked when it can only be unblocked by another goroutine in the same bubble.

Durable blocks include:

- Channel send/receive operations (channels inherit bubble membership)  
- Select statements (when all cases involve bubble channels)  
- Timers and time.Sleep()  
- sync.WaitGroup.Wait() and sync.Cond.Wait()

Non-durable blocks include mutex waits, network I/O, and syscalls, because these can be affected from outside the bubble.

When all goroutines are durably blocked, the bubble advances virtual time to the next timer and wakes the appropriate goroutines. This is how synctest makes tests run instantly instead of waiting for real time.

Synctest is already being adopted by major Go projects. Vitess, a distributed database system, recently [migrated their VStream tests to synctest](https://github.com/vitessio/vitess/pull/18701), reducing test execution time from minutes to seconds while improving test reliability.

The implementation spans 11 runtime files. Key components:

- [runtime/runtime2.go](https://github.com/golang/go/blob/master/src/runtime/runtime2.go): Adds bubble \*synctestBubble field to goroutine (g) and channel (hchan) structs  
- [runtime/synctest.go](https://github.com/golang/go/blob/master/src/runtime/synctest.go): Core bubble logic, status tracking, wake logic  
- [runtime/proc.go](https://github.com/golang/go/blob/master/src/runtime/proc.go): Scheduler integration, hooks into casgstatus() to track state changes  
- [runtime/chan.go](https://github.com/golang/go/blob/master/src/runtime/chan.go): Associates channels with bubbles, marks channel ops as durable blocks  
- [runtime/time.go](https://github.com/golang/go/blob/master/src/runtime/time.go): Virtualizes time, returns bubble.now instead of real time for goroutines in bubbles  
- [runtime/select.go](https://github.com/golang/go/blob/master/src/runtime/select.go): Marks select statements as durable when all cases are bubble channels

### Scheduler Non-Determinism

While synctest controls time, it doesn't control execution order. The scheduler makes random decisions at four key points using cheaprand():

1. **runqput** ([runtime/proc.go:7478](https://github.com/golang/go/blob/master/src/runtime/proc.go#L7478)): Random choice between placing goroutines in runnext (high priority) or regular queue  
2. **runqputslow** ([runtime/proc.go:7541](https://github.com/golang/go/blob/master/src/runtime/proc.go#L7541)): Random shuffle when moving goroutines from local to global queue  
3. **stealWork** ([runtime/proc.go:3837](https://github.com/golang/go/blob/master/src/runtime/proc.go#L3837)): Random starting point when stealing work from other processors  
4. **select** ([runtime/select.go:191](https://github.com/golang/go/blob/master/src/runtime/select.go#L191)): Random ordering of ready cases (required by Go spec)

These are the complete set of randomization points in the scheduler. All of these use cheaprand(), a per-M (OS thread) PRNG with state stored in mp.cheaprand. It's initialized in [mrandinit()](https://github.com/golang/go/blob/master/src/runtime/rand.go#L187) from a cryptographic source. Controlling cheaprand() seeding is the path to deterministic scheduling.

## Problem & Objective

Consider this simple Go program:

func main() {  
    go funcA()  
    go funcB()  
    go funcC()  
}

**Current situation**: Go's scheduler randomly selects which goroutine runs next. On one run, you might get the order B→A→C. On another run, you might get A→C→B.   
This randomness makes concurrency bugs hard to find and reproduce.

**Current testing approach**: Developers run tests with the race detector (`go test -race`) and run the tests multiple times, hoping the scheduler's randomness will eventually trigger the buggy interleaving. This is unreliable, some race conditions only appear in specific orderings that may never occur during testing.

**Our approach** (see Figure 2): Systematic exploration of different interleavings **within synctest bubbles**. By controlling the scheduler deterministically for goroutines in a bubble, we can explore multiple execution orders systematically. If a bug only appears when goroutines execute in the order C→B→A, we can find it by exploring that specific interleaving rather than hoping random scheduling hits it.

### Research Objective

This project investigates the feasibility of deterministic scheduling and systematic exploration in Go.

**Phase 1: Validation (Critical)**
The project's foundation rests on whether controlling Go's cheaprand() PRNG is sufficient for deterministic scheduling within synctest bubbles. This assumption requires validation because:
- Other entropy sources may affect scheduling: system call timing, memory allocator non-determinism, garbage collection interference, stack growth, atomic operations
- Mutex-based synchronization is non-durable and cannot be controlled within bubbles
- Hardware-level non-determinism may affect multi-core execution

Week 1-2 will validate this assumption through experiments. If cheaprand() control proves insufficient, the project will document the entropy sources that prevent deterministic scheduling and pivot to record-replay or happens-before analysis.

**Phase 2: Systematic Exploration (Contingent on Phase 1)**
If Phase 1 succeeds, we investigate how to systematically explore schedules. We will evaluate multiple approaches from the concurrency testing literature:
- PCT: Priority-based scheduling with probabilistic guarantees
- CHESS: Preemption-bounded systematic exploration
- DPOR: Dynamic partial order reduction
- Seed enumeration: Systematically trying different seed values

Based on feasibility analysis of implementation requirements, we will select and prototype ONE approach. Given the 14-week timeline, this is a feasibility study to validate concepts and evaluate tradeoffs, not a production implementation.

Deterministic scheduling enables reproducible testing within synctest bubbles. By scoping control to bubbles, we can test specific subsystems in isolation, making systematic exploration focused and tractable.

### Personal Goals

I want to understand how to explore solution spaces in research. When implementing a research paper's algorithm, there are usually gaps between the theory and practical implementation. I'm interested in learning how to navigate those gaps, what design decisions matter, and when theoretical approaches need modification for real systems.

This project will give me experience with low-level systems programming. I've worked with Go's standard library but never modified a language runtime. Understanding how the scheduler works, how goroutines are implemented, and how the runtime coordinates everything from channel operations to memory allocation requires working at a different level of abstraction.

I also want to work on developer tooling that has practical impact. Testing is a pain point for concurrent systems, and improving the testing experience could help developers catch bugs earlier in development.

## Related Work

Concurrency testing has been an active research area for decades. Three key systems inform this project:

* **DPOR (Dynamic Partial Order Reduction)**: Laid the theoretical foundation for efficient exploration by dynamically pruning equivalent thread interleavings.  
* **CHESS (Systematic Testing with Preemption Bounding)**: Brought practical systematic testing by bounding the number of preemptions, finding bugs that stress testing missed.  
* **PCT (Probabilistic Concurrency Testing)**: Provided probabilistic guarantees for randomized testing by focusing on bug "depth," offering practical bug-finding with rigorous mathematical guarantees.  
* **Microsoft's Coyote**: A production implementation for .NET that combines these techniques, providing validation for systematic concurrency testing at scale.

## Deliverables

**Technical Artifacts**:
- Deterministic scheduler implementation with seed control
- Prototype of ONE exploration approach (selected in Phase 2 based on feasibility analysis)
- Test suite of concurrent Go programs with known bugs
- Integration with synctest bubbles

**Research Documentation**:
- Validation report: entropy sources in Go's scheduler and whether seed control is sufficient
- Feasibility analysis: which algorithmic approaches can be adapted to Go (PCT/DPOR/CHESS)
- Empirical evaluation: bug detection rates, overhead measurements
- Implementation documentation: runtime modifications made and technical challenges encountered

**Deliverables if Phase 1 Fails**:
If seed control proves insufficient for determinism:
- Complete documentation of entropy sources that prevent deterministic scheduling
- Analysis of alternative approaches (record-replay, happens-before tracking)
- Recommendations for future work

## Research Questions

This project investigates deterministic scheduling and systematic exploration of concurrent interleavings in Go:

1. **What are the entropy sources in Go's scheduler, and is seed control sufficient for deterministic scheduling?**
   - Can controlling cheaprand() alone achieve reproducible schedules within synctest bubbles?
   - What other sources of non-determinism exist (syscalls, allocator, GC)?
   - What is the gap between "deterministic given seed" and "systematically explorable"?

2. **What is the relationship between scheduler seeds and reachable concurrent interleavings?**
   - How do different seed values map to different execution interleavings?
   - Is the seed-to-interleaving mapping tractable for systematic exploration?
   - How many distinct interleavings arise from seed variation?

3. **How can algorithmic approaches (PCT/DPOR/CHESS) be adapted or incorporated into Go's runtime?**
   - What runtime modifications are required for each approach?
   - Which components can be prototyped within the 14-week timeline?
   - What are the implementation barriers specific to Go?

4. **How do algorithmic exploration strategies compare to naive seed enumeration in effectiveness?**
   - Bug detection rates on known concurrency bugs
   - State space coverage for equivalent execution time
   - Implementation complexity vs. practical benefit tradeoff

## Evaluation

The testing and evaluation plan will be developed during the research work based on Phase 1 findings. The evaluation will include:

**Test Corpus**: Concurrent Go programs with known concurrency bugs will be selected from the Go issue tracker. Programs will be chosen based on:
- Reproducibility with `go test -race`
- Compatibility with synctest bubbles
- Coverage of different bug types (data races, deadlocks, atomicity violations)

**Metrics**: The evaluation will measure bug detection effectiveness, exploration efficiency, and runtime overhead.

**Baseline**: Comparison against standard approaches like `go test -race` with multiple runs.

**Success Criteria**: The minimum success criterion is validating deterministic scheduling (same seed produces same schedule). If systematic exploration proves feasible, we will evaluate bug-finding effectiveness and identify practical limitations.

## Limitations and Risks

**Critical Risk: Core Assumption May Be Wrong**
The project depends on seed control being sufficient for determinism. If validation fails:
- We pivot to documenting why deterministic scheduling is infeasible
- Alternative: investigate record-replay or happens-before analysis
- This is valid research (negative results matter)

**Scope Limitation: Feasibility Study, Not Production System**
14 weeks is insufficient for production-quality implementation of PCT/DPOR/CHESS. This project:
- Validates core assumptions
- Prototypes concepts
- Evaluates feasibility
- Does NOT deliver production-ready tool

**Technical Limitations**:
- Mutex-based code cannot be controlled (synctest limitation)
- Performance overhead may be prohibitive
- State space may explode for complex programs
- Evaluation limited to small programs (2-10 goroutines)

**Timeline Risk**:
- Phase 1 validation may take longer than expected
- Runtime debugging is notoriously difficult
- Zero buffer for unexpected issues
- Mitigation: Weekly checkpoints with supervisors, clear go/no-go decisions

## Timeline

### Phase 1: Validation (Weeks 1-4, Jan 13 - Feb 7)

**Goals**:
- Validate that cheaprand() seeding enables deterministic scheduling
- Identify other entropy sources if seed control is insufficient

**Week 1-2: Controlled Experiments**:
- Run simple concurrent Go programs (2-3 goroutines) with same seed 1000 times
- Measure: identical execution traces? Same schedule?
- Document any non-determinism observed

**Week 3-4: Entropy Source Investigation and Evaluation Planning**:
- Instrument runtime to identify sources of schedule variation
- Test with synctest bubbles specifically
- Develop testing and evaluation plan based on findings
- **Go/No-Go Decision**: If cheaprand() control insufficient, document findings and pivot

**Milestone**: Validation report with clear determination of feasibility

### Phase 2: Prototype Exploration (Weeks 5-8, Feb 10 - Mar 7)

**Contingent on Phase 1 success**

**Goals**:
- Investigate implementation requirements for different exploration approaches (PCT, DPOR, CHESS, seed enumeration)
- Select and prototype ONE approach based on feasibility analysis

**This is a feasibility study**: Adapt concepts to Go's runtime, not production-quality implementation

**Milestone**: Working prototype that can explore multiple schedules

### Phase 3: Evaluation (Weeks 9-12, Mar 10 - Apr 4)

**Goals**:
- Test on bug corpus
- Measure effectiveness vs. random testing
- Document limitations

**Milestone**: Empirical results on bug detection

### Phase 4: Documentation (Weeks 13-14, Apr 7 - Apr 18)

**Goals**:
- Synthesize findings
- Write report with honest assessment of feasibility
- Identify future work

**Deliverable**: Complete report with evaluation results

## References

\[1\] Sebastian Burckhardt, Pravesh Kothari, Madanlal Musuvathi, and Santosh Nagarakatte. 2010\. A randomized scheduler with probabilistic guarantees of finding bugs. In Proceedings of the fifteenth edition of ASPLOS on Architectural support for programming languages and operating systems (ASPLOS XV). ACM, 167–178.

\[2\] Madanlal Musuvathi, Shaz Qadeer, Thomas Ball, Gerard Basler, Piramanayagam Arumuga Nainar, and Iulian Neamtiu. 2008\. Finding and Reproducing Heisenbugs in Concurrent Programs. In Proceedings of the 8th USENIX Symposium on Operating Systems Design and Implementation (OSDI '08). USENIX Association, 267–280.

\[3\] Cormac Flanagan and Patrice Godefroid. 2005\. Dynamic partial-order reduction for model checking software. In Proceedings of the 32nd ACM SIGPLAN-SIGACT symposium on Principles of programming languages (POPL '05). ACM, 110–121.

\[4\] Microsoft Coyote: [https://microsoft.github.io/coyote/](https://microsoft.github.io/coyote/)

\[5\] Go Programming Language Source Code: [https://github.com/golang/go](https://github.com/golang/go)

\[6\] VictoriaMetrics Blog: Go synctest package overview: [https://victoriametrics.com/blog/go-synctest/](https://victoriametrics.com/blog/go-synctest/)

