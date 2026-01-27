CPSC 448 Directed Studies Proposal  
**Deterministic Scheduling for Exploration of Concurrent Interleavings in Go**

Shubhaankar Sharma  
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

Synctest is already being adopted by major Go projects. Vitess, a distributed database system, recently [migrated their VStream tests to synctest](https://github.com/vitessio/vitess/pull/18701), reducing test execution time from minutes to seconds while improving test reliability. This Pull Request showcases very clearly how synctest is used in practical scenarios.

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

These are the complete set of randomization points in the scheduler. All of these use cheaprand(), a per-M (OS thread) PRNG with state stored in mp.cheaprand. It's initialized in [mrandinit()](https://github.com/golang/go/blob/master/src/runtime/rand.go#L187) from a cryptographic source. From an initial look it seems that cheaprand() seeding is the path to deterministic scheduling.

## Problem

Consider this simple Go program:  
func main() {  
    go funcA()  
    go funcB()  
    go funcC()  
}

**Current situation**: Go's scheduler randomly selects which goroutine runs next. On one run, you might get the order B→A→C. On another run, you might get A→C→B.   
This randomness makes concurrency bugs hard to find and reproduce.

**Current testing approach**: Developers run tests with the race detector (`go test -race`) and run the tests multiple times, hoping the scheduler's randomness will eventually trigger the buggy interleaving. This is unreliable, some race conditions only appear in specific orderings that may never occur during testing.

### 

### Objective

Systematic exploration of different interleavings **within synctest bubbles**. By controlling the scheduler deterministically for goroutines in a bubble, we can explore multiple execution orders systematically. If a bug only appears when goroutines execute in the order C→B→A, we can find it by exploring that specific interleaving rather than hoping random scheduling hits it.

This project investigates the feasibility of deterministic scheduling and systematic exploration in Go.

**Phase 1: Validation** The project's foundation rests on whether controlling Go's cheaprand() PRNG is sufficient for deterministic scheduling within synctest bubbles. This assumption requires validation because:

- Other entropy sources may affect scheduling: system call timing, memory allocator non-determinism, garbage collection interference, stack growth, atomic operations  
- Mutex-based synchronization is non-durable and cannot be controlled within bubbles  
- Hardware-level non-determinism may affect multi-core execution

Week 1-2 will validate this assumption through experiments. If cheaprand() control proves insufficient, the project will document the entropy sources that prevent deterministic scheduling and try to investigate further how we could achieve deterministic scheduling.

**Phase 2: Systematic Exploration** If Phase 1 succeeds, we investigate how to systematically explore schedules. We will evaluate multiple approaches from the following related work:

* **DPOR (Dynamic Partial Order Reduction)**: Laid the theoretical foundation for efficient exploration by dynamically pruning equivalent thread interleavings. \[3\]  
* **CHESS (Systematic Testing with Preemption Bounding)**: Brought practical systematic testing by bounding the number of preemptions, finding bugs that stress testing missed. \[2\]  
* **PCT (Probabilistic Concurrency Testing)**: Provided probabilistic guarantees for randomized testing by focusing on bug "depth," offering practical bug-finding with rigorous mathematical guarantees. \[1\]  
* **Microsoft's Coyote**: A production implementation for .NET that combines these techniques, providing validation for systematic concurrency testing at scale. \[4\]

Based on feasibility analysis of implementation requirements, we will come up with an approach to do systematic exploration.

Deterministic scheduling enables reproducible testing within synctest bubbles. By scoping control to bubbles, we can test specific subsystems in isolation, making systematic exploration focused on components.

### Personal Goals

I want to understand how to explore solution spaces in research. When implementing a research paper's algorithm, there are usually gaps between the theory and practical implementation. I'm interested in learning how to navigate those gaps, what design decisions matter, and when theoretical approaches need modification for real systems.

This project will give me experience with low-level systems programming. I've worked with Go's standard library but never modified a language runtime. Understanding how the scheduler works, how goroutines are implemented, and how the runtime coordinates everything from channel operations to memory allocation requires working at a different level of abstraction.

I also want to work on developer tooling that has practical impact. Testing is a pain point for concurrent systems, and improving the testing experience could help developers catch bugs earlier in development.

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

**Deliverables if Phase 1 Fails**: If seed control proves insufficient for determinism:

- Complete documentation of entropy sources that prevent deterministic scheduling  
- Find alternative approaches to achieve deterministic scheduling

## Research Questions

This project investigates deterministic scheduling and systematic exploration of concurrent interleavings in Go:

1. **What are the entropy sources in Go's scheduler, and is seed control sufficient for deterministic scheduling?**

   

2. **What is the relationship between scheduler seeds and reachable concurrent interleavings?**  
     
   - How do different seed values map to different execution interleavings?  
   - How many distinct interleavings arise from seed variation?

   

3. **How can algorithmic approaches (PCT/DPOR/CHESS) be adapted or incorporated into Go's runtime?**

## Evaluation

The testing and evaluation plan will be developed during the research work based on Phase 1 findings. The evaluation will include:

**Test Corpus**: Existing open source repositories with extensive test suites might make a good candidate for our test corpus, for microbenchmarking I will also explore if its possible to synthetically generate our test corpus to evaluate different approaches. 

**Baseline**: Comparison against standard approaches like `go test -race` with multiple runs.

**Success Criteria**: The minimum success criterion is validating deterministic scheduling (same seed produces same schedule). If systematic exploration proves feasible, we will evaluate bug-finding effectiveness and identify practical limitations.

## Timeline

### Phase 1: Validation (Weeks 1-4, Jan 13 \- Feb 7\)

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
- If cheaprand() control insufficient, document findings and pivot

**Milestone**: Validation report with clear determination of feasibility

### Phase 2: Prototype Exploration (Weeks 5-10, Feb 10 \- Mar 20\)

**Goals**:

- Investigate implementation requirements for different exploration approaches (PCT, DPOR, CHESS, seed enumeration)  
- Select and prototype an approach based on analysis

**Milestone**: Working prototype that can explore multiple schedules

### Phase 3: Evaluation (Weeks 11-12, Mar 23 \- Apr 4\)

**Goals**:

- Test on bug corpus  
- Measure effectiveness vs. random testing  
- Document limitations

**Milestone**: Empirical results on bug detection

### Phase 4: Documentation (Weeks 13-14, Apr 7 \- Apr 18\)

**Goals**:

- Write final report

## References

\[1\] Sebastian Burckhardt, Pravesh Kothari, Madanlal Musuvathi, and Santosh Nagarakatte. 2010\. A randomized scheduler with probabilistic guarantees of finding bugs. In Proceedings of the fifteenth edition of ASPLOS on Architectural support for programming languages and operating systems (ASPLOS XV). ACM, 167–178.

\[2\] Madanlal Musuvathi, Shaz Qadeer, Thomas Ball, Gerard Basler, Piramanayagam Arumuga Nainar, and Iulian Neamtiu. 2008\. Finding and Reproducing Heisenbugs in Concurrent Programs. In Proceedings of the 8th USENIX Symposium on Operating Systems Design and Implementation (OSDI '08). USENIX Association, 267–280.

\[3\] Cormac Flanagan and Patrice Godefroid. 2005\. Dynamic partial-order reduction for model checking software. In Proceedings of the 32nd ACM SIGPLAN-SIGACT symposium on Principles of programming languages (POPL '05). ACM, 110–121.

\[4\] Microsoft Coyote: [https://microsoft.github.io/coyote/](https://microsoft.github.io/coyote/)

\[5\] Go Programming Language Source Code: [https://github.com/golang/go](https://github.com/golang/go)

\[6\] VictoriaMetrics Blog: Go synctest package overview: [https://victoriametrics.com/blog/go-synctest/](https://victoriametrics.com/blog/go-synctest/)

