# Deterministic Scheduling for Go Concurrency Testing

Research project investigating deterministic scheduling and systematic exploration of concurrent interleavings in Go.

**CPSC 448 Directed Studies - Winter 2025 Term 2**
Shubhaankar Sharma
Supervisors: Arpan Gujarati, Ivan Beschastnikh

## Problem

Go's `testing/synctest` package solves time non-determinism but not execution order non-determinism. The scheduler makes random decisions via `cheaprand()`, making concurrency bugs hard to reproduce.

## Objective

Validate whether controlling `cheaprand()` is sufficient for deterministic scheduling within synctest bubbles, then prototype systematic exploration of interleavings.

## Repository Structure

```
.
├── docs/                    # Research documentation
│   ├── 448-proposal.md      # Research proposal
│   ├── synctest-explanation.md
│   ├── dpor-summary.md
│   ├── pct-summary.md
│   ├── chess-summary.md
│   └── vitess-vstream-synctest-analysis.md
├── papers/                  # Reference papers (PDFs)
├── changelog/               # Weekly progress logs
├── go/                      # Forked Go runtime for experiments
└── README.md
```

## Setup

### Prerequisites

- macOS or Linux
- Git

### Building the Modified Go Runtime

```bash
# Clone this repository
git clone <repo-url>
cd synctest

# Build the Go toolchain from source
cd go/src
./make.bash

# The built toolchain is in go/bin/
export PATH=$PWD/../bin:$PATH

# Verify
go version
```

### Running Tests with Modified Runtime

```bash
# Use the local Go build
export GOROOT=/path/to/synctest/go
export PATH=$GOROOT/bin:$PATH

# Run synctest tests
cd $GOROOT/src/testing/synctest
go test -v
```

## Research Phases

| Phase | Weeks | Goal |
|-------|-------|------|
| 1. Validation | 1-4 | Validate cheaprand() control enables deterministic scheduling |
| 2. Exploration | 5-10 | Prototype systematic exploration (PCT/DPOR/CHESS) |
| 3. Evaluation | 11-12 | Test on bug corpus, measure effectiveness |
| 4. Documentation | 13-14 | Final report |

## Key Files in Go Runtime

| File | Purpose |
|------|---------|
| `go/src/runtime/rand.go` | `cheaprand()` - source of scheduler randomness |
| `go/src/runtime/proc.go` | Scheduler, `runqput`, `stealWork` |
| `go/src/runtime/synctest.go` | Synctest bubble implementation |
| `go/src/runtime/select.go` | Select case randomization |

## References

- [Go synctest package](https://pkg.go.dev/testing/synctest)
- [PCT Paper (Burckhardt et al., ASPLOS 2010)](papers/pct-burckhardt-2010.pdf)
- [CHESS Paper (Musuvathi et al., OSDI 2008)](papers/chess-musuvathi-2007.pdf)
- [DPOR Paper (Flanagan & Godefroid, POPL 2005)](papers/dpor-flanagan-2005.pdf)
