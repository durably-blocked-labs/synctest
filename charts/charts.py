#!/usr/bin/env python3
"""
charts.py — Generate evaluation charts from orchestratorv2 JSONL exploration data.

DATA FORMAT
-----------
Each JSONL file corresponds to one call to Explore or ExploreRandom with a
NewJSONLObserver attached. Each line is a JSON object for one run:

  {"run_num": 1, "non_fifo": 0, "elapsed_ns": 4200000, "passed": true,
   "global_decision_count": 8, "local_decision_total": 42,
   "queue_sizes": [1, 3, 1, 2], "trace_fingerprint": "n1->n2(AE) n2->n1(AER)",
   "deliver_seq": [{"from":"n1","to":"n2","op_type":"AE","index":0,"queue_size":1}]}

QUICK START
-----------
1. Add to your Go test:
     orch.Explore(t, setup,
         GlobalBound(2),
         orchestratorv2.BoundFromEnv(),
         orchestratorv2.MaxRunsFromEnv(),
         orchestratorv2.ObserverFromEnv(t),
     )

2. Collect data:
     make charts-sweep TEST=TestRemoveLeaderBug          # k=0..3 systematic
     make charts-random TEST=TestRemoveLeaderBug SEEDS=20 # random baseline

3. Plot:
     make charts-plot

ANNOTATED USAGE
---------------
Annotate each file with @key=value pairs to override auto-detection:

  python charts.py \\
    --input "data/run_k0.jsonl@k=0,mode=sys,scenario=RemoveLeader" \\
    --input "data/run_k1.jsonl@k=1,mode=sys,scenario=RemoveLeader" \\
    --input "data/run_k2.jsonl@k=2,mode=sys,scenario=RemoveLeader" \\
    --input "data/rand_s1.jsonl@k=-1,mode=rand,scenario=RemoveLeader,seed=1" \\
    --outdir figures/

Auto-detection from filenames (no annotation needed):
  - *_k0*, *_k1*, *_k2*, *_k3*  → k extracted
  - *_sys*                       → mode=sys
  - *_rand*                      → mode=rand
  - *_seed[N]*                   → seed extracted

FIGURES GENERATED
-----------------
  fig1_bug_discovery.png     — bugs found + interleavings explored vs k (needs k-sweep)
  fig2_physical_vs_logical_time.png — physical time vs logical time advanced
  fig3_decisions_at_bug.png — local/global decisions in the failing run
  fig4_theoretical_vs_actual.png — actual DFS runs vs theoretical global deliveries
  fig5_unique_interleavings_until_bug.png — unique delivery sequences until first bug
  fig_cdf_first_failure.png  — CDF of first-failure run (random baseline, needs rand files)
"""

import argparse
import json
import math
import os
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def parse_annotation(spec: str) -> Tuple[str, dict]:
    """Parse 'path[@k=N,mode=sys,scenario=X,seed=N]' into (path, metadata)."""
    if "@" not in spec:
        return spec, {}
    path, ann = spec.split("@", 1)
    meta = {}
    for part in ann.split(","):
        if "=" in part:
            k, v = part.split("=", 1)
            meta[k.strip()] = v.strip()
    return path, meta


def detect_metadata(path: str) -> dict:
    """Auto-detect k, mode, scenario, seed from filename."""
    name = Path(path).stem
    meta = {}
    m = re.search(r"_k(-?\d+)", name)
    if m:
        meta["k"] = int(m.group(1))
    if "_sys" in name:
        meta["mode"] = "sys"
    elif "_rand" in name:
        meta["mode"] = "rand"
    m = re.search(r"_seed(\d+)", name)
    if m:
        meta["seed"] = int(m.group(1))
    # scenario: everything before first _k or _sys or _rand
    sc = re.split(r"_k\d+|_sys|_rand|_seed", name)[0]
    if sc:
        meta.setdefault("scenario", sc)
    return meta


def load_file(spec: str) -> pd.DataFrame:
    """Load a JSONL file (with optional @annotations) into a DataFrame."""
    path, ann_meta = parse_annotation(spec)
    file_meta = detect_metadata(path)
    meta = {**file_meta, **ann_meta}  # annotation overrides auto-detect

    records = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            obj.update(meta)
            records.append(obj)

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    # Normalise column types.
    for col in ("k", "seed"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "k" not in df.columns:
        df["k"] = -1
    if "mode" not in df.columns:
        df["mode"] = "sys"
    if "scenario" not in df.columns:
        df["scenario"] = Path(path).stem
    if "seed" not in df.columns:
        df["seed"] = 0

    # Derived columns.
    df["elapsed_ms"] = df["elapsed_ns"] / 1e6
    if "logical_time_ns" not in df.columns:
        df["logical_time_ns"] = 0
    df["logical_time_ms"] = df["logical_time_ns"] / 1e6
    if "local_trace_by_node" in df.columns:
        has_trace = df["local_trace_by_node"].apply(lambda traces: isinstance(traces, dict))
        if has_trace.any():
            df.loc[has_trace, "local_decision_by_node"] = df.loc[has_trace, "local_trace_by_node"].apply(
                meaningful_local_counts
            )
            df.loc[has_trace, "local_decision_total"] = df.loc[has_trace, "local_decision_by_node"].apply(
                lambda counts: sum(counts.values()) if isinstance(counts, dict) else 0
            )
    df["total_decisions"] = df["global_decision_count"] + df["local_decision_total"]

    return df


def load_all(specs: List[str]) -> pd.DataFrame:
    frames = [load_file(s) for s in specs]
    frames = [f for f in frames if not f.empty]
    if not frames:
        raise SystemExit("No data loaded. Check input files.")
    return pd.concat(frames, ignore_index=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

PALETTE = ["#2196F3", "#F44336", "#4CAF50", "#FF9800", "#9C27B0", "#00BCD4"]


def save(fig, outdir: str, name: str):
    os.makedirs(outdir, exist_ok=True)
    p = os.path.join(outdir, name)
    fig.savefig(p, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {p}")


def skip_existing(outdir: str, name: str):
    p = os.path.join(outdir, name)
    if os.path.exists(p):
        os.remove(p)


def first_failure_run(df: pd.DataFrame) -> Optional[int]:
    """Return the run_num of the first failing run, or None."""
    failed = df[~df["passed"]]
    if failed.empty:
        return None
    return int(failed["run_num"].min())


def unique_fingerprints(df: pd.DataFrame) -> int:
    """Count distinct trace fingerprints in df."""
    return df["trace_fingerprint"].nunique()


def meaningful_local_counts(traces: dict) -> Dict[str, int]:
    """Count local choices where more than one non-root goroutine was runnable."""
    counts = {}
    for node, trace in traces.items():
        count = 0
        for d in trace:
            bgids = d.get("runq_bgids", [])
            runq_size = min(int(d.get("runq_size", len(bgids))), len(bgids))
            non_root = sum(1 for bgid in bgids[:runq_size] if bgid != 0)
            if non_root > 1:
                count += 1
        counts[node] = count
    return counts


def theoretical_interleavings(df: pd.DataFrame) -> int:
    """
    Estimate theoretical exhaustive interleavings from the FIFO baseline run.
    Product of QueueSize at each delivery step where QueueSize > 1.
    Uses only the first (FIFO, run_num=1) row.
    """
    fifo = df[df["run_num"] == 1]
    if fifo.empty or "queue_sizes" not in fifo.columns:
        return 1
    sizes = fifo.iloc[0].get("queue_sizes", [])
    if not sizes or not isinstance(sizes, list):
        return 1
    prod = 1
    for s in sizes:
        if isinstance(s, (int, float)) and s > 1:
            prod *= int(s)
    return prod


# ---------------------------------------------------------------------------
# Figure 1 — Bug discovery vs. search cost
# ---------------------------------------------------------------------------

def fig1_bug_discovery(df: pd.DataFrame, outdir: str):
    """
    Dual-axis: for each k, plot (a) total interleavings explored and
    (b) whether a bug was found. One line per scenario.
    Requires systematic files at multiple k values.
    """
    sys_df = df[df["mode"] == "sys"].copy()
    scenarios = sys_df["scenario"].unique()
    ks = sorted(sys_df["k"].dropna().unique().astype(int))
    if len(ks) < 2:
        print("  fig1: skipped (need k-sweep data, got k={})".format(ks))
        return

    fig, ax1 = plt.subplots(figsize=(7, 4))
    ax2 = ax1.twinx()

    for i, sc in enumerate(scenarios):
        sc_df = sys_df[sys_df["scenario"] == sc]
        k_runs = []
        k_bug = []
        for k in ks:
            kd = sc_df[sc_df["k"] == k]
            if kd.empty:
                continue
            k_runs.append((k, len(kd)))
            k_bug.append((k, 1 if (~kd["passed"]).any() else 0))

        if not k_runs:
            continue
        kk, runs = zip(*k_runs)
        color = PALETTE[i % len(PALETTE)]
        ax1.plot(kk, runs, "o-", color=color, label=f"{sc} (runs)")
        kk2, bugs = zip(*k_bug)
        ax2.plot(kk2, bugs, "s--", color=color, alpha=0.6)

    ax1.set_xlabel("Context bound k")
    ax1.set_ylabel("Interleavings explored")
    ax2.set_ylabel("Bug found (1=yes)")
    ax2.set_yticks([0, 1])
    ax2.set_yticklabels(["no", "yes"])
    ax1.legend(loc="upper left", fontsize=8)
    ax1.set_title("Fig 1 — Bug discovery vs. search cost")
    ax1.grid(True, alpha=0.3)
    save(fig, outdir, "fig1_bug_discovery.png")


# ---------------------------------------------------------------------------
# Figure 2 — Physical time vs logical time
# ---------------------------------------------------------------------------

def fig2_physical_vs_logical_time(df: pd.DataFrame, outdir: str):
    """
    Cumulative physical wall-clock time vs cumulative logical time advanced
    by the system under test.
    """
    # Ignore the synthetic 1ns startup barrier used by some tests. If that is
    # the only fake-time movement in the dataset, this chart would be
    # misleading because no protocol-level logical time advanced.
    meaningful = df[df["logical_time_ns"] > 1]
    if meaningful.empty:
        print("  fig2: skipped (no meaningful logical time advancement)")
        skip_existing(outdir, "fig2_physical_vs_logical_time.png")
        skip_existing(outdir, "fig3_physical_vs_logical_time.png")
        return

    fig, ax = plt.subplots(figsize=(7, 4))
    color_idx = 0

    for (sc, k, mode), gdf in meaningful.sort_values("run_num").groupby(["scenario", "k", "mode"]):
        gdf = gdf.sort_values("run_num")
        physical_ms = gdf["elapsed_ms"].cumsum().values
        logical_ns = gdf["logical_time_ns"].cumsum().values
        k_label = f"k={int(k)}" if k >= 0 else "rand"
        label = f"{sc} {k_label} ({mode})"
        color = PALETTE[color_idx % len(PALETTE)]
        ax.plot(physical_ms, logical_ns, color=color, marker="o", label=label)
        # Mark first failure
        first_fail_idx = gdf[~gdf["passed"]]["run_num"].min() if (~gdf["passed"]).any() else None
        if first_fail_idx is not None:
            row = gdf[gdf["run_num"] == first_fail_idx]
            if not row.empty:
                pos = np.flatnonzero(gdf["run_num"].to_numpy() == first_fail_idx)[0]
                ax.scatter([physical_ms[pos]], [logical_ns[pos]], color=color,
                           edgecolor="black", zorder=3, label=f"{sc} first failure")
        color_idx += 1

    ax.set_xlabel("Cumulative physical time (ms)")
    ax.set_ylabel("Cumulative logical time advanced (ns)")
    ax.set_title("Fig 2 — Physical time vs. logical time")
    ax.legend(fontsize=7)
    ax.grid(True, alpha=0.3)
    save(fig, outdir, "fig2_physical_vs_logical_time.png")
    skip_existing(outdir, "fig3_physical_vs_logical_time.png")


# ---------------------------------------------------------------------------
# Figure 3 — Decisions at bug discovery
# ---------------------------------------------------------------------------

def fig3_decisions_at_bug(df: pd.DataFrame, outdir: str):
    """
    Global and local scheduling decisions in the run where the bug is found.
    """
    sys_df = df[df["mode"] == "sys"].copy()
    if sys_df.empty:
        print("  fig3: skipped (no systematic data)")
        skip_existing(outdir, "fig3_decisions_at_bug.png")
        return

    labels, global_counts, local_counts = [], [], []

    for (sc, k, mode), gdf in sys_df.sort_values("run_num").groupby(["scenario", "k", "mode"]):
        gdf = gdf.sort_values("run_num")
        first_fail = first_failure_run(gdf)
        if first_fail is None:
            continue
        row = gdf[gdf["run_num"] == first_fail].iloc[0]
        k_label = f"k={int(k)}" if k >= 0 else "rand"
        labels.append(f"{sc}\n{k_label} run {first_fail}")
        global_counts.append(row["global_decision_count"])
        local_counts.append(row["local_decision_total"])

    if not labels:
        print("  fig3: skipped (no failing runs in systematic data)")
        skip_existing(outdir, "fig3_decisions_at_bug.png")
        return

    fig, ax = plt.subplots(figsize=(max(6, len(labels) * 1.5), 4))
    x = np.arange(len(labels))
    width = 0.35
    global_bars = ax.bar(x - width/2, global_counts, width, label="global decisions", color=PALETTE[0], alpha=0.85)
    local_bars = ax.bar(x + width/2, local_counts, width, label="meaningful local decisions", color=PALETTE[1], alpha=0.85)
    for bar, value in zip(global_bars, global_counts):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height(), f"{int(value)}",
                ha="center", va="bottom", fontsize=8)
    for bar, value in zip(local_bars, local_counts):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height(), f"{int(value)}",
                ha="center", va="bottom", fontsize=8)

    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=8)
    ax.set_ylabel("Scheduling decisions")
    ax.set_title("Fig 3 — Decisions in the bug-discovery run")
    ax.legend(fontsize=8)
    ax.grid(True, axis="y", alpha=0.3)
    save(fig, outdir, "fig3_decisions_at_bug.png")
    skip_existing(outdir, "fig3_decisions_until_bug.png")
    skip_existing(outdir, "fig4_decisions_until_bug.png")


# ---------------------------------------------------------------------------
# Figure 4 — Global delivery search space
# ---------------------------------------------------------------------------

def fig4_theoretical_vs_actual(df: pd.DataFrame, outdir: str):
    """
    Log-scale: actual DFS runs explored vs. theoretical exhaustive global
    delivery orderings, grouped by scenario and k.
    """
    sys_df = df[df["mode"] == "sys"].copy()
    scenarios = sys_df["scenario"].unique()
    ks = sorted(sys_df["k"].dropna().unique().astype(int))
    if not ks:
        print("  fig4: skipped (no systematic data)")
        skip_existing(outdir, "fig4_theoretical_vs_actual.png")
        return

    if len(ks) == 1:
        labels, actual, theory = [], [], []
        for sc in scenarios:
            kd = sys_df[sys_df["scenario"] == sc]
            if kd.empty:
                continue
            k = int(kd["k"].iloc[0])
            labels.append(f"{sc}\nk={k}")
            actual.append(len(kd))
            theory.append(theoretical_interleavings(kd))

        if not labels:
            print("  fig4: skipped (no systematic data)")
            skip_existing(outdir, "fig4_theoretical_vs_actual.png")
            return

        fig, ax = plt.subplots(figsize=(max(6, len(labels) * 1.4), 4))
        x = np.arange(len(labels))
        width = 0.35
        ax.bar(x - width/2, actual, width, label="actual explored", color=PALETTE[0], alpha=0.85)
        ax.bar(x + width/2, theory, width, label="theoretical global deliveries", color=PALETTE[1], alpha=0.85)
        ax.set_xticks(x)
        ax.set_xticklabels(labels, fontsize=8)
    else:
        fig, ax = plt.subplots(figsize=(7, 4))
        color_idx = 0

        for sc in scenarios:
            sc_df = sys_df[sys_df["scenario"] == sc]
            actual_runs, theory = [], []
            for k in ks:
                kd = sc_df[sc_df["k"] == k]
                if kd.empty:
                    continue
                actual_runs.append((k, len(kd)))
                theory.append((k, theoretical_interleavings(kd)))

            if not actual_runs:
                continue
            color = PALETTE[color_idx % len(PALETTE)]
            kk, runs = zip(*actual_runs)
            ax.plot(kk, runs, "o-", color=color, label=f"{sc} (actual explored)")
            kk2, th = zip(*theory)
            ax.plot(kk2, th, "s--", color=color, alpha=0.5, label=f"{sc} (theoretical global)")
            color_idx += 1

        ax.set_xlabel("Context bound k")

    ax.set_yscale("log")
    ax.set_ylabel("Global delivery orderings (log scale)")
    ax.set_title("Fig 4 — Global delivery search space")
    ax.legend(fontsize=7)
    ax.grid(True, which="both", alpha=0.3)
    save(fig, outdir, "fig4_theoretical_vs_actual.png")
    skip_existing(outdir, "fig6_theoretical_vs_actual.png")


# ---------------------------------------------------------------------------
# Figure 5 — Unique interleavings until bug found
# ---------------------------------------------------------------------------

def fig5_unique_interleavings_until_bug(df: pd.DataFrame, outdir: str):
    """
    Unique delivery sequences explored up to and including the first failing run.
    """
    sys_df = df[df["mode"] == "sys"].copy()
    if sys_df.empty:
        print("  fig5: skipped (no systematic data)")
        skip_existing(outdir, "fig5_unique_interleavings_until_bug.png")
        return

    labels, unique_counts, run_counts, colors = [], [], [], []
    for (sc, k, mode), gdf in sys_df.sort_values("run_num").groupby(["scenario", "k", "mode"]):
        gdf = gdf.sort_values("run_num")
        first_fail = first_failure_run(gdf)
        if first_fail is None:
            continue
        subset = gdf[gdf["run_num"] <= first_fail]
        k_label = f"k={int(k)}" if k >= 0 else "rand"
        labels.append(f"{sc}\n{k_label}")
        unique_counts.append(unique_fingerprints(subset))
        run_counts.append(len(subset))
        colors.append(PALETTE[int(k) % len(PALETTE)] if k >= 0 else "#888888")

    if not labels:
        print("  fig5: skipped (no failing runs in systematic data)")
        skip_existing(outdir, "fig5_unique_interleavings_until_bug.png")
        return

    fig, ax = plt.subplots(figsize=(max(6, len(labels) * 1.3), 4))
    x = np.arange(len(labels))
    bars = ax.bar(x, unique_counts, color=colors, alpha=0.85)
    for bar, unique, runs in zip(bars, unique_counts, run_counts):
        label = f"{unique}" if unique == runs else f"{unique}\n({runs} runs)"
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height(), label,
                ha="center", va="bottom", fontsize=8)

    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=8)
    ax.set_ylabel("Unique delivery sequences")
    ax.set_title("Fig 5 — Unique interleavings until first bug")
    ax.grid(True, axis="y", alpha=0.3)
    save(fig, outdir, "fig5_unique_interleavings_until_bug.png")


# ---------------------------------------------------------------------------
# Figure CDF — First failure run (random baseline CDF)
# ---------------------------------------------------------------------------

def fig_cdf_first_failure(df: pd.DataFrame, outdir: str):
    """
    CDF of the run number at which the first failure is detected across
    multiple random seeds. Compares with a vertical line showing the
    deterministic DFS first-failure run.

    Requires: multiple rand files (each = one seed) for the same scenario.
    """
    rand_df = df[df["mode"] == "rand"].copy()
    if rand_df.empty:
        print("  fig_cdf_first_failure: skipped (no random baseline data)")
        return

    fig, ax = plt.subplots(figsize=(7, 4))
    color_idx = 0

    for sc, sc_rand in rand_df.groupby("scenario"):
        seeds = sc_rand["seed"].unique()
        first_failures = []
        for seed in seeds:
            seed_df = sc_rand[sc_rand["seed"] == seed].sort_values("run_num")
            ff = first_failure_run(seed_df)
            # None = bug not found within budget; use inf as sentinel
            first_failures.append(ff if ff is not None else np.inf)

        found = [x for x in first_failures if not np.isinf(x)]
        total = len(first_failures)
        if not found:
            print(f"  fig_cdf_first_failure: {sc}: bug never found in random runs")
            continue

        sorted_ff = np.sort(found)
        cdf = np.arange(1, len(sorted_ff) + 1) / total
        color = PALETTE[color_idx % len(PALETTE)]
        ax.step(sorted_ff, cdf, color=color, label=f"{sc} ({len(found)}/{total} seeds found bug)",
                where="post")

        # Overlay systematic DFS first-failure as a vertical line if available.
        sys_sc = df[(df["scenario"] == sc) & (df["mode"] == "sys")]
        if not sys_sc.empty:
            sys_ff = first_failure_run(sys_sc)
            if sys_ff is not None:
                ax.axvline(sys_ff, color=color, linestyle=":",
                           label=f"{sc} DFS finds at run {sys_ff}")
        color_idx += 1

    max_runs = int(rand_df["run_num"].max())
    ax.set_xlim(0, max_runs)
    ax.set_ylim(0, 1.05)
    ax.set_xlabel("Run number")
    ax.set_ylabel("Fraction of seeds that found bug")
    ax.set_title("CDF — First failure run (random baseline)")
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3)
    save(fig, outdir, "fig_cdf_first_failure.png")


# ---------------------------------------------------------------------------
# Summary table (text)
# ---------------------------------------------------------------------------

def print_summary(df: pd.DataFrame):
    """Print a concise summary table to stdout."""
    print("\n=== Exploration Summary ===")
    for (sc, k, mode), gdf in df.groupby(["scenario", "k", "mode"]):
        total = len(gdf)
        failed = (~gdf["passed"]).sum()
        ff = first_failure_run(gdf)
        unique = unique_fingerprints(gdf)
        k_label = f"k={int(k)}" if k >= 0 else "rand"
        print(f"  {sc:30s} {k_label:6s} {mode:4s} | "
              f"runs={total:4d}  failed={failed:3d}  "
              f"first_fail={ff if ff is not None else '-':>5}  "
              f"unique_traces={unique:4d}")
    print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Generate evaluation charts from orchestratorv2 JSONL data.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--input", "-i", action="append", metavar="FILE[@k=N,mode=sys|rand,...]",
        help="JSONL file with optional @k=N,mode=sys|rand,scenario=X annotations. "
             "May be repeated for multiple files. Glob patterns accepted.",
        required=True,
    )
    parser.add_argument(
        "--outdir", "-o", default="charts/figures",
        help="Output directory for figures (default: charts/figures)",
    )
    parser.add_argument(
        "--figures", nargs="*",
        choices=["fig1", "fig2", "fig3", "fig4", "fig5", "cdf"],
        help="Which figures to generate (default: all)",
    )
    args = parser.parse_args()

    # Expand glob patterns in --input.
    import glob as _glob
    specs = []
    for raw in args.input:
        if "@" in raw:
            path, ann = raw.split("@", 1)
            expanded = _glob.glob(path)
            if not expanded:
                print(f"Warning: no files matched '{path}'", file=sys.stderr)
            for p in expanded:
                specs.append(f"{p}@{ann}")
        else:
            expanded = _glob.glob(raw)
            if not expanded:
                print(f"Warning: no files matched '{raw}'", file=sys.stderr)
            specs.extend(expanded)

    if not specs:
        raise SystemExit("No input files found.")

    print(f"Loading {len(specs)} file(s)...")
    df = load_all(specs)
    print(f"  Loaded {len(df)} runs across "
          f"{df['scenario'].nunique()} scenario(s), "
          f"k=[{', '.join(str(int(k)) for k in sorted(df['k'].dropna().unique()))}]")

    print_summary(df)

    want = set(args.figures) if args.figures else {"fig1","fig2","fig3","fig4","fig5","cdf"}
    outdir = args.outdir
    print(f"Generating figures → {outdir}/")

    if "fig1" in want: fig1_bug_discovery(df, outdir)
    if "fig2" in want: fig2_physical_vs_logical_time(df, outdir)
    if "fig3" in want: fig3_decisions_at_bug(df, outdir)
    if "fig4" in want: fig4_theoretical_vs_actual(df, outdir)
    if "fig5" in want: fig5_unique_interleavings_until_bug(df, outdir)
    if "cdf"  in want: fig_cdf_first_failure(df, outdir)

    print("Done.")


if __name__ == "__main__":
    main()
