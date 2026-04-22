# Chart Plan

Spec for every chart needed for the paper and progress reports. Each entry gives the chart name, data source, output path, generating function, claim supported, and axis/layout description.

Status key: **EXISTS** = already generated and in repo, **NEW** = must be built, **UPDATE** = exists but needs modification.

---

## GoBench Charts

All GoBench data lives under `bugs/gobench/charts/data/` with a copy at `charts/data/` for the original 5 bugs. 14 bugs have data files; the user spec targets the 13 Channel & Lock subset (excludes serving2137 or one other depending on the GoBench taxonomy).

Bug IDs with data: grpc1353, k8s6632, etcd7902, moby33781, etcd7443, etcd6873, grpc1460, istio16224, k8s10182, k8s26980, moby28462, k8s1321, etcd7492, serving2137.

Chart script: `charts/gobench.py`

### Chart 1: Tool Comparison Table

| Field | Value |
|---|---|
| **Status** | UPDATE |
| **Claim** | synctest detects 10/13 Channel & Lock bugs; no existing tool detects more than 5 |
| **Data files** | `bugs/gobench/charts/data/{bug}-chess.jsonl` (all 13 bugs) |
| **Existing tools data** | Hardcoded in `charts/gobench.py` `TOOL_RESULTS` dict (currently 5 bugs) |
| **Script** | `charts/gobench.py` |
| **Function** | `fig_tool_comparison()` |
| **Output** | `charts/figures/gobench/tool_comparison.md` |
| **What to change** | Extend `BUGS` list to all 13 Channel & Lock bugs. Add `TOOL_RESULTS` and `FLAKINESS` entries for each new bug. Add synctest detection status from chess JSONL first_bug_run. |
| **Layout** | Markdown table. Columns: Bug, Flakiness (stress test hit rate), Go DD, Goleak, GCatch, GFuzz, GoAT, synctest (CHESS). Cell values: D = detected, X = missed, run N = synctest result. Bold synctest column. |

### Chart 2: CHESS Exploration Tree for etcd#7443

| Field | Value |
|---|---|
| **Status** | UPDATE |
| **Claim** | CHESS systematically explores 74 interleavings via DFS; bug is at the last leaf of depth 3 |
| **Data files** | `bugs/gobench/charts/data/etcd7443-chess-tree.json` |
| **Script** | `charts/gobench.py` |
| **Function** | `fig_chess_tree()` |
| **Output** | `charts/figures/gobench/chess_tree_etcd7443.png` |
| **What to change** | Bold the root-to-bug path (trace edges from root to run 74 node in red/thick). Annotate depth bands with horizontal shading and labels "k=0 (1 run)", "k=1 (N runs)", "k=2 (N runs)", "k=3 (N runs, bug here)". |
| **Axes** | X: leaf ordering (trace index). Y: context depth (non-FIFO decisions), inverted. Green dots = pass, red X = bug. |

### Chart 3: CDF of Runs-to-Bug Across Seeds

| Field | Value |
|---|---|
| **Status** | NEW |
| **Claim** | CHESS provides a worst-case guarantee (74 runs); Random and PCT have long tails where some seeds need 100+ runs |
| **Data files** | `charts/data/seed-sweep.json` |
| **Script** | `charts/gobench.py` |
| **Function** | `fig_cdf_runs_to_bug()` (new) |
| **Output** | `charts/figures/gobench/cdf_runs_to_bug.png` |
| **Layout** | Single axes. X: runs explored (1 to 120, linear). Y: P(bug found by run x), 0 to 1. Three CDF lines: Random (100 seeds, purple), PCT-d2 (100 seeds, orange), PCT-d3 (100 seeds, green). Vertical dashed blue line at x=74 labeled "CHESS (deterministic)". |
| **Data format** | `seed-sweep.json` is an array of `{seed, strategy, first_bug, runs}`. Group by strategy, compute empirical CDF from `first_bug` values. |
| **Annotations** | Label each line with median and P95. Caption: "CHESS always finds the bug by run 74. Random median = M, P95 = P." |

### Chart 4: Decision Anatomy of etcd#7443 Bug Trace

| Field | Value |
|---|---|
| **Status** | NEW |
| **Claim** | Only 3 non-FIFO choices out of 7 scheduling steps caused the deadlock |
| **Data files** | `bugs/gobench/charts/data/etcd7443-chess-trace.jsonl` (last record = bug trace) |
| **Script** | `charts/gobench.py` |
| **Function** | `fig_decision_anatomy()` (new) |
| **Output** | `charts/figures/gobench/decision_anatomy_etcd7443.png` |
| **Layout** | Vertical stack of 7 rows (one per decision step), top to bottom. Each row is a colored rectangle: blue = FIFO (index 0), red = non-FIFO (index != 0). Width proportional to alternatives count. Only the 3 red rows get text labels: goroutine role (from `BGID_ROLES` dict) + what the non-FIFO choice does. Right margin annotation: "3 non-FIFO choices out of 7 steps caused deadlock". |
| **Data format** | Last record in trace JSONL. Field `steps[]` with `{chosen_bgid, index, alternatives, runq_bgids}`. |
| **Notes** | Uses `BGID_ROLES` mapping already in `gobench.py`. Tufte-style: minimal ink, no gridlines, rely on color contrast. |

### Chart 5: Per-Bug Results Table as Figure

| Field | Value |
|---|---|
| **Status** | NEW |
| **Claim** | synctest detects 10/13 GoBench Channel & Lock bugs; the 3 misses are due to instruction-level races beyond goroutine scheduling granularity |
| **Data files** | `bugs/gobench/charts/data/{bug}-chess.jsonl` (all 13 bugs) |
| **Script** | `charts/gobench.py` |
| **Function** | `fig_per_bug_table()` (new) |
| **Output (markdown)** | `charts/figures/gobench/per_bug_results.md` |
| **Output (image)** | `charts/figures/gobench/per_bug_results.png` |
| **Layout** | Matplotlib table figure. 13 rows, one per bug. Columns: Bug ID, Project, Mechanism (channel/mutex/waitgroup), Goroutines (from trace data), CHESS Runs to Bug, Detected? 10 rows with green background for detected bugs. 3 rows with red background + reason column (e.g., "instruction-level data race"). |
| **Data notes** | Goroutine count: derive from `max(chosen_bgid)` in chess trace data. Mechanism: hardcoded metadata per bug (needs manual classification from GoBench paper). |

---

## ra-gate Charts (Distributed Bugs)

Primary data: `bugs/ra-gate/charts/data/` (ra-gate) and `bugs/ra-stale-reply/benchmarking/data/` (ra-stale-reply). Other ra-* bugs (ra-premature-defer, ra-deferred-storm, ra-duplicate-request) have figures in `benchmarking/figures/` but data files need to be regenerated.

Chart script: `charts/charts.py` (the generic multi-policy chart generator).

Invocation pattern:
```
python3 charts/charts.py --data bugs/ra-gate/charts/data --out bugs/ra-gate/charts/figures
python3 charts/charts.py --data bugs/ra-stale-reply/benchmarking/data --out bugs/ra-stale-reply/benchmarking/figures
```

### Chart 6: Runs-to-First-Bug Bar Chart

| Field | Value |
|---|---|
| **Status** | EXISTS |
| **Claim** | CHESS G+L finds the ra-gate bug faster than G-only, PCT, or Random |
| **Data files** | `bugs/ra-gate/charts/data/{chess-global,chess-gl,pct-d2,pct-d3,random}.jsonl` |
| **Script** | `charts/charts.py` |
| **Function** | `fig_runs_to_bug()` (Figure 1 in charts.py) |
| **Output** | `charts/figures/runs_to_bug.png` (when run with ra-gate data dir) |
| **Axes** | X: strategy (5 bars: Random, PCT-d2, CHESS G+L, PCT-d3, CHESS G-only). Y: average runs to first bug. Color: blue = all attempts found, orange = some missed, gray = none found. Found-rate annotation above each bar. |
| **Notes** | ra-gate data files are currently empty (0 bytes for chess-gl.jsonl and chess-global.jsonl). Data must be regenerated before this chart can be produced. Existing output at `charts/figures/runs_to_bug.png` is from older data and may be stale. |

### Chart 7: CHESS Exploration Trees for ra-gate

| Field | Value |
|---|---|
| **Status** | EXISTS |
| **Claim** | G+L tree (47 nodes) is 3x smaller than G-only tree (142 nodes) because local scheduling constraints prune branches |
| **Data files** | `bugs/ra-gate/charts/data/chess-global-tree.json` (G-only), `bugs/ra-gate/charts/data/chess-gl-tree.json` (G+L) |
| **Script** | `charts/charts.py` |
| **Function** | `fig_chess_tree()` (Figure 5 in charts.py) |
| **Output** | `charts/figures/tree_chess-global.png`, `charts/figures/tree_chess-gl.png` |
| **Axes** | X: exploration frontier. Y: DFS depth (inverted). Green dots = pass, red dots = bug. Node count and bug count in title. |

### Chart 8: Message Sequence Diagram for ra-gate

| Field | Value |
|---|---|
| **Status** | EXISTS |
| **Claim** | The bug manifests when a specific non-FIFO message delivery reorders AppendEntries before RequestVote reply |
| **Data files** | `bugs/ra-gate/charts/data/{policy}-trace.jsonl` (all policies) |
| **Script** | `charts/charts.py` |
| **Function** | `fig_sequence_diagram()` (Figure 3 in charts.py) |
| **Output** | `charts/figures/sequence_{policy}.png` (one per policy with bug trace) |
| **Layout** | UML-style sequence diagram. Vertical lifelines for nodes A, B, C. Horizontal arrows for message deliveries. Gray = FIFO, red = non-FIFO. Non-FIFO arrows annotated with `[index/alternatives]`. |

### Chart 9: Non-FIFO Decision Comparison

| Field | Value |
|---|---|
| **Status** | EXISTS |
| **Claim** | CHESS G+L finds the bug with fewer total non-FIFO decisions than G-only or random strategies |
| **Data files** | `bugs/ra-gate/charts/data/{policy}-trace.jsonl` |
| **Script** | `charts/charts.py` |
| **Function** | `fig_nonfifo_from_traces()` (Figure 4 in charts.py) |
| **Output** | `charts/figures/nonfifo_comparison.png` |
| **Layout** | Stacked bar chart. X: strategy. Y: non-FIFO decisions. Bottom segment (orange) = local non-FIFO. Top segment (blue) = global non-FIFO. Total annotated above each bar. |

### Chart 10: Search Space vs Explored

| Field | Value |
|---|---|
| **Status** | EXISTS |
| **Claim** | Context bounding makes the search tractable: explored runs are orders of magnitude fewer than theoretical interleavings |
| **Data files** | `bugs/ra-gate/charts/data/{policy}.jsonl` and `{policy}-trace.jsonl` |
| **Script** | `charts/charts.py` |
| **Function** | `fig_search_space_vs_explored()` (Figure 7 in charts.py) |
| **Output** | `bugs/ra-gate/benchmarking/figures/search_space_vs_explored.png` |
| **Layout** | Grouped bar chart, log Y scale. X: strategy. Two bars per strategy: light blue = theoretical interleavings (product of per-step alternatives), dark blue = runs actually explored. Theoretical values annotated in scientific notation. |

### Chart 11: Cumulative Unique Traces

| Field | Value |
|---|---|
| **Status** | EXISTS |
| **Claim** | CHESS explores unique traces more efficiently (less repetition) than random/PCT |
| **Data files** | `bugs/ra-gate/charts/data/{policy}.jsonl` (uses `trace_fingerprint` field) |
| **Script** | `charts/charts.py` |
| **Function** | `fig_cumulative_unique_traces()` (Figure 12 in charts.py) |
| **Output** | `bugs/ra-gate/benchmarking/figures/cumulative_unique_traces.png` |
| **Layout** | Line chart. X: run number. Y: cumulative unique delivery traces seen. One line per strategy. Dashed diagonal = ideal no-repetition line. Shaded band = min/max across attempts. |

---

## Cross-Cutting Charts

These combine data from both GoBench and ra-gate evaluations.

### Chart 12: Unified Bug Matrix (Heatmap)

| Field | Value |
|---|---|
| **Status** | NEW |
| **Claim** | synctest explorer detects bugs across both single-process (GoBench) and distributed (Raft) domains; no single strategy dominates for all bugs |
| **Data files** | GoBench: `bugs/gobench/charts/data/{bug}-{strategy}.jsonl` (13 bugs x 4 strategies). Distributed: `bugs/ra-gate/charts/data/{policy}.jsonl`, `bugs/ra-stale-reply/benchmarking/data/{policy}.jsonl`, plus data for ra-premature-defer, ra-deferred-storm, ra-duplicate-request (need generation). |
| **Script** | `charts/cross_cutting.py` (new file) |
| **Function** | `fig_unified_bug_matrix()` (new) |
| **Output** | `charts/figures/unified_bug_matrix.png` |
| **Layout** | Heatmap. Rows: all bugs (13 GoBench + 5 ra-*), grouped with horizontal divider. Columns: CHESS, PCT-d2, PCT-d3, Random (plus CHESS-gl and CHESS-global for distributed bugs). Cell color: green gradient = runs-to-first-bug (darker = fewer runs = better). Gray = not found within budget. White = no data. Cell text: run number or "X". Row labels: bug ID. Column labels: strategy name. |
| **Data notes** | For GoBench, CHESS = chess (k=3 local-only). For distributed, CHESS = chess-global and chess-gl. Need to read `first_bug_run()` from each JSONL. |

### Chart 13: Strategy Comparison Summary Table

| Field | Value |
|---|---|
| **Status** | NEW |
| **Claim** | CHESS provides completeness guarantees within the context bound; random/PCT offer no worst-case bound but may find easy bugs faster |
| **Data files** | All JSONL summary files from both GoBench and distributed benchmarks |
| **Script** | `charts/cross_cutting.py` (new file) |
| **Function** | `fig_strategy_comparison_table()` (new) |
| **Output (markdown)** | `charts/figures/strategy_comparison.md` |
| **Output (image)** | `charts/figures/strategy_comparison.png` |
| **Layout** | Matplotlib table. Columns: Strategy, Guarantees, Bugs Found (GoBench), Bugs Found (Distributed), Avg Runs (easy bugs, those found in <=5 runs by all strategies), Avg Runs (hard bugs, those requiring >5 runs by at least one strategy), Worst Case. |
| **Rows** | CHESS (k=3), CHESS G+L (k=2), CHESS G-only (k=2), PCT (d=2), PCT (d=3), Random. |

### Chart 14: Runtime Overhead

| Field | Value |
|---|---|
| **Status** | NEW |
| **Claim** | synctest explorer overhead per run is small enough for practical use; hook-based scheduling adds ~Xms per run |
| **Data files** | `elapsed_ns` field in all JSONL files: `bugs/gobench/charts/data/{bug}-{strategy}.jsonl`, `bugs/ra-gate/charts/data/{policy}.jsonl`, `bugs/ra-stale-reply/benchmarking/data/{policy}.jsonl` |
| **Script** | `charts/cross_cutting.py` (new file) |
| **Function** | `fig_runtime_overhead()` (new) |
| **Output** | `charts/figures/runtime_overhead.png` |
| **Layout** | Grouped bar chart. X: bug (grouped). Y: wall-clock time per run (ms). One bar per strategy. Error bars showing min/max across runs. Separate panel or color band for GoBench vs distributed bugs. |
| **Baseline** | Need to collect plain `go test` wall-clock time for each bug (no explorer, no hook). Store in a new file `charts/data/baseline-runtime.json` with format `{bug: elapsed_ns}`. |
| **Notes** | `elapsed_ns` is already recorded in every JSONL record. Baseline data does not exist yet and needs to be measured. |

---

## Data File Inventory

### GoBench data (`bugs/gobench/charts/data/`)

| Pattern | Description | Record count |
|---|---|---|
| `{bug}-chess.jsonl` | CHESS summary per run | 14 bugs |
| `{bug}-chess-trace.jsonl` | Per-run detailed trace (steps array) | 14 bugs |
| `{bug}-chess-tree.json` | DFS tree structure (Parent, Passed, NonFIFO, Run, BranchStep, TraceLen) | 14 bugs |
| `{bug}-pct-d2.jsonl` | PCT d=2 summary | 14 bugs |
| `{bug}-pct-d2-trace.jsonl` | PCT d=2 traces | 14 bugs |
| `{bug}-pct-d3.jsonl` | PCT d=3 summary | 14 bugs |
| `{bug}-pct-d3-trace.jsonl` | PCT d=3 traces | 14 bugs |
| `{bug}-random.jsonl` | Random summary | 14 bugs |
| `{bug}-random-trace.jsonl` | Random traces | 14 bugs |

### ra-gate data (`bugs/ra-gate/charts/data/`)

| File | Description | Status |
|---|---|---|
| `chess-global.jsonl` | CHESS G-only summary | EMPTY (needs regen) |
| `chess-global-trace.jsonl` | CHESS G-only traces | present |
| `chess-global-tree.json` | CHESS G-only DFS tree | present |
| `chess-gl.jsonl` | CHESS G+L summary | EMPTY (needs regen) |
| `chess-gl-trace.jsonl` | CHESS G+L traces | present |
| `chess-gl-tree.json` | CHESS G+L DFS tree | present |
| `pct-d2.jsonl` | PCT d=2 summary | modified (unstaged) |
| `pct-d2-trace.jsonl` | PCT d=2 traces | modified (unstaged) |
| `pct-d3.jsonl` | PCT d=3 summary | present |
| `pct-d3-trace.jsonl` | PCT d=3 traces | present |
| `random.jsonl` | Random summary | modified (unstaged) |
| `random-trace.jsonl` | Random traces | modified (unstaged) |

### ra-stale-reply data (`bugs/ra-stale-reply/benchmarking/data/`)

| File | Description | Status |
|---|---|---|
| `chess-gl.jsonl` | CHESS G+L summary | present |
| `chess-gl-trace.jsonl` | CHESS G+L traces | present |
| `chess-gl-tree.json` | CHESS G+L DFS tree | present |
| `chess-global.jsonl` | CHESS G-only summary | present |
| `chess-global-trace.jsonl` | CHESS G-only traces | present |
| `chess-global-tree.json` | CHESS G-only DFS tree | present |
| `pct-d2.jsonl` | PCT d=2 summary | present |
| `pct-d2-trace.jsonl` | PCT d=2 traces | present |
| `pct-d3.jsonl` | PCT d=3 summary | present |
| `pct-d3-trace.jsonl` | PCT d=3 traces | present |
| `random.jsonl` | Random summary | present |
| `random-trace.jsonl` | Random traces | present |

### Other distributed bugs

| Bug | Data location | Status |
|---|---|---|
| ra-premature-defer | `bugs/ra-premature-defer/benchmarking/` | figures exist, data files missing (need regen) |
| ra-deferred-storm | `bugs/ra-deferred-storm/` | no benchmarking dir, no data |
| ra-duplicate-request | `bugs/ra-duplicate-request/` | no benchmarking dir, no data |
| chain-replication | `bugs/chain-replication/` | no benchmarking dir, no data |

### Seed sweep data (`charts/data/`)

| File | Description | Format |
|---|---|---|
| `seed-sweep.json` | 100 seeds x 3 strategies (random, pct-d2, pct-d3) for etcd#7443 | JSON array of `{seed, strategy, first_bug, runs}` |

---

## Script File Inventory

| Script | Location | Scope |
|---|---|---|
| `charts/charts.py` | `charts/charts.py` | Generic multi-policy charts (ra-gate, ra-stale-reply). 15 figure functions. Invoked with `--data` and `--out`. |
| `charts/gobench.py` | `charts/gobench.py` | GoBench-specific charts. 9 figure functions. Hardcoded `DATA_DIR = charts/data`, `FIG_DIR = charts/figures/gobench`. |
| `charts/cross_cutting.py` | `charts/cross_cutting.py` | **NEW**. Cross-cutting charts (unified heatmap, strategy summary, runtime overhead). Must read from both GoBench and distributed data dirs. |

---

## Chart-to-Figure Mapping for Paper

| Paper Section | Chart # | Chart Name | Figure ID |
|---|---|---|---|
| Background / Motivation | 4 | Decision anatomy of etcd#7443 | Fig 1 |
| GoBench Evaluation | 1 | Tool comparison table | Table 1 |
| GoBench Evaluation | 5 | Per-bug results table | Table 2 |
| GoBench Evaluation | 2 | CHESS exploration tree (etcd#7443) | Fig 2 |
| GoBench Evaluation | 3 | CDF of runs-to-bug across seeds | Fig 3 |
| Distributed Evaluation | 8 | Message sequence diagram (ra-gate) | Fig 4 |
| Distributed Evaluation | 6 | Runs-to-first-bug bar chart | Fig 5 |
| Distributed Evaluation | 7 | CHESS exploration trees (G-only vs G+L) | Fig 6 |
| Distributed Evaluation | 9 | Non-FIFO decision comparison | Fig 7 |
| Analysis | 10 | Search space vs explored | Fig 8 |
| Analysis | 11 | Cumulative unique traces | Fig 9 |
| Cross-cutting | 12 | Unified bug matrix heatmap | Fig 10 |
| Cross-cutting | 13 | Strategy comparison summary | Table 3 |
| Cross-cutting | 14 | Runtime overhead | Fig 11 |

---

## Generation Commands

```bash
# GoBench charts (existing + new)
python3 charts/gobench.py

# ra-gate charts (requires non-empty data files)
python3 charts/charts.py --data bugs/ra-gate/charts/data --out bugs/ra-gate/charts/figures

# ra-stale-reply charts
python3 charts/charts.py --data bugs/ra-stale-reply/benchmarking/data --out bugs/ra-stale-reply/benchmarking/figures

# Cross-cutting charts (new script)
python3 charts/cross_cutting.py
```

---

## Blocking Issues

1. **ra-gate summary JSONLs are empty.** `chess-gl.jsonl` and `chess-global.jsonl` are 0 bytes. Must re-run benchmarks or recover data before Charts 6, 7, 9, 10, 12 can include ra-gate CHESS data.
2. **ra-premature-defer, ra-deferred-storm, ra-duplicate-request have no benchmark data.** Chart 12 (unified bug matrix) needs at least summary JSONLs for these bugs. Must run benchmarks.
3. **Baseline runtime data does not exist.** Chart 14 needs plain `go test` elapsed time per bug. Must measure and store in `charts/data/baseline-runtime.json`.
4. **GoBench bug metadata (mechanism, project) is not in data files.** Chart 5 needs per-bug classification (channel vs mutex vs waitgroup, project name). Must be hardcoded or stored in a metadata file.
5. **Tool comparison data for bugs 6-13.** Chart 1 currently has `TOOL_RESULTS` for only 5 bugs. Must look up GoBench paper Table 2 for the remaining 8 bugs.
