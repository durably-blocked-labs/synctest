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
  fig2_unique_interleavings.png — unique traces before first bug per scenario/k
  fig3_time_vs_runs.png      — cumulative wall-clock time vs cumulative runs
  fig4_decision_distribution.png — box plot: decisions per run grouped by k
  fig5_decisions_at_bug.png  — CDF of decisions in failing runs
  fig6_theoretical_vs_actual.png — actual DFS runs vs theoretical (log scale, needs k-sweep)
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


def first_failure_run(df: pd.DataFrame) -> Optional[int]:
    """Return the run_num of the first failing run, or None."""
    failed = df[~df["passed"]]
    if failed.empty:
        return None
    return int(failed["run_num"].min())


def unique_fingerprints(df: pd.DataFrame) -> int:
    """Count distinct trace fingerprints in df."""
    return df["trace_fingerprint"].nunique()


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
# Figure 2 — Unique interleavings until bug found
# ---------------------------------------------------------------------------

def fig2_unique_interleavings(df: pd.DataFrame, outdir: str):
    """
    Bar chart: unique delivery sequences explored before the first failing run,
    grouped by scenario × k.
    """
    sys_df = df[df["mode"] == "sys"].copy()
    groups = sys_df.groupby(["scenario", "k"])

    labels, unique_counts, bar_colors = [], [], []
    for (sc, k), gdf in sorted(groups, key=lambda x: (x[0][0], x[0][1])):
        first_fail = first_failure_run(gdf)
        subset = gdf if first_fail is None else gdf[gdf["run_num"] <= first_fail]
        uc = unique_fingerprints(subset)
        k_label = f"k={int(k)}" if k >= 0 else "rand"
        labels.append(f"{sc}\n{k_label}")
        unique_counts.append(uc)
        bar_colors.append(PALETTE[int(k) % len(PALETTE)] if k >= 0 else "#888")

    if not labels:
        print("  fig2: skipped (no systematic data)")
        return

    fig, ax = plt.subplots(figsize=(max(6, len(labels) * 1.2), 4))
    x = np.arange(len(labels))
    bars = ax.bar(x, unique_counts, color=bar_colors, alpha=0.85)
    ax.bar_label(bars, fmt="%d", padding=3, fontsize=8)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=8)
    ax.set_ylabel("Unique delivery sequences")
    ax.set_title("Fig 2 — Unique interleavings until bug found")
    ax.grid(True, axis="y", alpha=0.3)
    save(fig, outdir, "fig2_unique_interleavings.png")


# ---------------------------------------------------------------------------
# Figure 3 — Physical time over logical time (cumulative)
# ---------------------------------------------------------------------------

def fig3_time_vs_runs(df: pd.DataFrame, outdir: str):
    """
    Cumulative wall-clock time vs. cumulative run number, one line per
    (scenario, k, mode) group. Slope = time-per-run cost.
    """
    fig, ax = plt.subplots(figsize=(7, 4))
    color_idx = 0

    for (sc, k, mode), gdf in df.sort_values("run_num").groupby(["scenario", "k", "mode"]):
        gdf = gdf.sort_values("run_num")
        cum_runs = np.arange(1, len(gdf) + 1)
        cum_ms = gdf["elapsed_ms"].cumsum().values
        k_label = f"k={int(k)}" if k >= 0 else "rand"
        label = f"{sc} {k_label} ({mode})"
        ax.plot(cum_runs, cum_ms, color=PALETTE[color_idx % len(PALETTE)], label=label)
        # Mark first failure
        first_fail_idx = gdf[~gdf["passed"]]["run_num"].min() if (~gdf["passed"]).any() else None
        if first_fail_idx is not None:
            row = gdf[gdf["run_num"] == first_fail_idx]
            if not row.empty:
                fi = int(row.index[0] - gdf.index[0])
                ax.axvline(fi + 1, color=PALETTE[color_idx % len(PALETTE)],
                           linestyle=":", alpha=0.7, linewidth=1)
        color_idx += 1

    ax.set_xlabel("Cumulative runs explored")
    ax.set_ylabel("Cumulative wall-clock time (ms)")
    ax.set_title("Fig 3 — Physical time over logical time\n(dotted line = first failure)")
    ax.legend(fontsize=7)
    ax.grid(True, alpha=0.3)
    save(fig, outdir, "fig3_time_vs_runs.png")


# ---------------------------------------------------------------------------
# Figure 4 — Decision count distribution per context bound
# ---------------------------------------------------------------------------

def fig4_decision_distribution(df: pd.DataFrame, outdir: str):
    """
    Box plot of total scheduling decisions per run, grouped by k.
    Shows whether higher k leads to longer/more complex traces.
    """
    sys_df = df[df["mode"] == "sys"].copy()
    ks = sorted(sys_df["k"].dropna().unique().astype(int))
    if not ks:
        print("  fig4: skipped (no systematic data)")
        return

    fig, ax = plt.subplots(figsize=(max(5, len(ks) * 1.5), 4))
    data_by_k = [sys_df[sys_df["k"] == k]["total_decisions"].dropna().values for k in ks]
    bp = ax.boxplot(data_by_k, tick_labels=[f"k={k}" for k in ks], patch_artist=True)
    for patch, color in zip(bp["boxes"], PALETTE):
        patch.set_facecolor(color)
        patch.set_alpha(0.7)

    ax.set_xlabel("Context bound k")
    ax.set_ylabel("Total scheduling decisions per run\n(global + local)")
    ax.set_title("Fig 4 — Decision count distribution per context bound")
    ax.grid(True, axis="y", alpha=0.3)
    save(fig, outdir, "fig4_decision_distribution.png")


# ---------------------------------------------------------------------------
# Figure 5 — Scheduling decisions at bug discovery (CDF)
# ---------------------------------------------------------------------------

def fig5_decisions_at_bug(df: pd.DataFrame, outdir: str):
    """
    CDF of total scheduling decisions in failing runs, split by mode.
    Shows whether bugs are shallow (low decision count) or deep.
    """
    failed = df[~df["passed"]].copy()
    if failed.empty:
        print("  fig5: skipped (no failing runs in data)")
        return

    fig, ax = plt.subplots(figsize=(7, 4))
    color_idx = 0

    for (sc, mode), gdf in failed.groupby(["scenario", "mode"]):
        decisions = np.sort(gdf["total_decisions"].values)
        cdf = np.arange(1, len(decisions) + 1) / len(decisions)
        label = f"{sc} ({mode})"
        ax.step(decisions, cdf, color=PALETTE[color_idx % len(PALETTE)], label=label, where="post")
        color_idx += 1

    ax.set_xlabel("Total scheduling decisions in failing run")
    ax.set_ylabel("Cumulative fraction of bugs found")
    ax.set_title("Fig 5 — Scheduling decisions at bug discovery")
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3)
    ax.set_ylim(0, 1.05)
    save(fig, outdir, "fig5_decisions_at_bug.png")


# ---------------------------------------------------------------------------
# Figure 6 — Explored vs. theoretical search space
# ---------------------------------------------------------------------------

def fig6_theoretical_vs_actual(df: pd.DataFrame, outdir: str):
    """
    Log-scale: actual DFS runs explored vs. theoretical exhaustive interleavings,
    both as a function of k. The gap shows context bounding's pruning power.
    """
    sys_df = df[df["mode"] == "sys"].copy()
    scenarios = sys_df["scenario"].unique()
    ks = sorted(sys_df["k"].dropna().unique().astype(int))
    if len(ks) < 2:
        print("  fig6: skipped (need k-sweep data)")
        return

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
        ax.plot(kk, runs, "o-", color=color, label=f"{sc} (actual)")
        kk2, th = zip(*theory)
        ax.plot(kk2, th, "s--", color=color, alpha=0.5, label=f"{sc} (theoretical)")
        color_idx += 1

    ax.set_yscale("log")
    ax.set_xlabel("Context bound k")
    ax.set_ylabel("Interleavings (log scale)")
    ax.set_title("Fig 6 — Explored vs. theoretical search space")
    ax.legend(fontsize=7)
    ax.grid(True, which="both", alpha=0.3)
    save(fig, outdir, "fig6_theoretical_vs_actual.png")


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
        choices=["fig1", "fig2", "fig3", "fig4", "fig5", "fig6", "cdf"],
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

    want = set(args.figures) if args.figures else {"fig1","fig2","fig3","fig4","fig5","fig6","cdf"}
    outdir = args.outdir
    print(f"Generating figures → {outdir}/")

    if "fig1" in want: fig1_bug_discovery(df, outdir)
    if "fig2" in want: fig2_unique_interleavings(df, outdir)
    if "fig3" in want: fig3_time_vs_runs(df, outdir)
    if "fig4" in want: fig4_decision_distribution(df, outdir)
    if "fig5" in want: fig5_decisions_at_bug(df, outdir)
    if "fig6" in want: fig6_theoretical_vs_actual(df, outdir)
    if "cdf"  in want: fig_cdf_first_failure(df, outdir)

    print("Done.")


if __name__ == "__main__":
    main()
