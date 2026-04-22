#!/usr/bin/env python3
"""Visualize GoBench kernel benchmark results.

Usage:
    python3 charts/gobench.py

Reads JSONL from charts/data/{bug}-{strategy}.jsonl and produces
figures in charts/figures/gobench/.

Figures:
  1. runs_to_first_bug.png     — bar chart: all bugs × strategies
  2. tool_comparison.md        — markdown table: synctest vs existing tools
  3. summary_table.md          — full results table
  4. chess_tree_etcd7443.png   — CHESS DFS exploration tree
  5. trace_comparison.png      — FIFO (safe) vs bug-triggering trace
  6. nonfifo_over_runs.png     — non-FIFO decisions across CHESS runs
  7. seed_variance.png         — seed sweep: Random/PCT variance vs CHESS
  8. decision_space.png        — branching factor at each step
  9. intrinsic_metrics.png     — decisions, branches, runq sizes
"""

import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

GOBENCH_DATA_DIR = Path(__file__).resolve().parent.parent / "bugs" / "gobench" / "charts" / "data"
DATA_DIR = GOBENCH_DATA_DIR  # alias used by load helpers
FIG_DIR = Path(__file__).parent / "figures" / "gobench"

# 13 Channel & Lock bugs + moby33781 (Channel & Context, included for completeness).
# The 10/13 claim counts the 13 Ch+Lock bugs; moby33781 is bonus (14 total).
BUGS = [
    "grpc1353", "k8s6632", "etcd7902", "moby33781", "etcd7443",
    "etcd6873", "grpc1460", "istio16224", "k8s10182", "k8s26980",
    "moby28462", "k8s1321", "etcd7492", "serving2137",
]
STRATEGIES = ["chess", "pct-d2", "pct-d3", "random"]
STRATEGY_LABELS = {
    "chess": "CHESS(k=3)",
    "pct-d2": "PCT(d=2)",
    "pct-d3": "PCT(d=3)",
    "random": "Random",
}
STRATEGY_COLORS = {
    "chess": "#2196F3",
    "pct-d2": "#FF9800",
    "pct-d3": "#4CAF50",
    "random": "#9C27B0",
}

BUG_LABELS = {
    "grpc1353":    "grpc#1353",
    "k8s6632":     "k8s#6632",
    "etcd7902":    "etcd#7902",
    "moby33781":   "moby#33781",
    "etcd7443":    "etcd#7443",
    "etcd6873":    "etcd#6873",
    "grpc1460":    "grpc#1460",
    "istio16224":  "istio#16224",
    "k8s10182":    "k8s#10182",
    "k8s26980":    "k8s#26980",
    "moby28462":   "moby#28462",
    "k8s1321":     "k8s#1321",
    "etcd7492":    "etcd#7492",
    "serving2137": "serving#2137",
}

# Per-bug tool detection results from Li (Edinburgh, 2023) Table 4.2.
# D = detected, X = missed.
TOOL_RESULTS = {
    "grpc1353":   {"Go DD": "X", "Goleak": "D", "GCatch": "X", "GFuzz": "D", "GoAT": "D"},
    "k8s6632":    {"Go DD": "X", "Goleak": "X", "GCatch": "D", "GFuzz": "X", "GoAT": "X"},
    "etcd7902":   {"Go DD": "X", "Goleak": "D", "GCatch": "D", "GFuzz": "X", "GoAT": "X"},
    "moby33781":  {"Go DD": "X", "Goleak": "D", "GCatch": "D", "GFuzz": "X", "GoAT": "X"},
    "etcd7443":   {"Go DD": "X", "Goleak": "X", "GCatch": "X", "GFuzz": "X", "GoAT": "X"},
    "etcd6873":   {"Go DD": "X", "Goleak": "D", "GCatch": "D", "GFuzz": "X", "GoAT": "D"},
    "grpc1460":   {"Go DD": "X", "Goleak": "D", "GCatch": "X", "GFuzz": "D", "GoAT": "D"},
    "istio16224": {"Go DD": "X", "Goleak": "D", "GCatch": "X", "GFuzz": "X", "GoAT": "D"},
    "k8s10182":   {"Go DD": "X", "Goleak": "D", "GCatch": "D", "GFuzz": "X", "GoAT": "D"},
    "k8s26980":   {"Go DD": "X", "Goleak": "X", "GCatch": "X", "GFuzz": "D", "GoAT": "X"},
    "moby28462":  {"Go DD": "X", "Goleak": "X", "GCatch": "X", "GFuzz": "D", "GoAT": "X"},
    "k8s1321":    {"Go DD": "X", "Goleak": "X", "GCatch": "X", "GFuzz": "X", "GoAT": "X"},
    "etcd7492":    {"Go DD": "X", "Goleak": "X", "GCatch": "X", "GFuzz": "X", "GoAT": "X"},
    "serving2137": {"Go DD": "X", "Goleak": "X", "GCatch": "X", "GFuzz": "X", "GoAT": "X"},
}

FLAKINESS = {
    "grpc1353":   "100/100",
    "k8s6632":    "4/100",
    "etcd7902":   "100/100",
    "moby33781":  "25/100",
    "etcd7443":   "N/A",
    "etcd6873":   "N/A",
    "grpc1460":   "N/A",
    "istio16224": "N/A",
    "k8s10182":   "N/A",
    "k8s26980":   "N/A",
    "moby28462":  "N/A",
    "k8s1321":    "N/A",
    "etcd7492":    "N/A",
    "serving2137": "N/A",
}

# Per-bug metadata for the results table (Chart 5).
BUG_METADATA = {
    "grpc1353":   {"project": "gRPC-Go",     "mechanism": "mutex + chan",      "goroutines": 3},
    "k8s6632":    {"project": "Kubernetes",   "mechanism": "RWMutex + chan",    "goroutines": 3},
    "etcd7902":   {"project": "etcd",         "mechanism": "mutex + chan",      "goroutines": 3},
    "moby33781":  {"project": "Moby",         "mechanism": "chan + context",    "goroutines": 3},
    "etcd7443":   {"project": "etcd",         "mechanism": "RWMutex + buf chan","goroutines": 5},
    "etcd6873":   {"project": "etcd",         "mechanism": "mutex + chan",      "goroutines": 3},
    "grpc1460":   {"project": "gRPC-Go",      "mechanism": "chan + mutex",      "goroutines": 3},
    "istio16224": {"project": "Istio",        "mechanism": "mutex + chan",      "goroutines": 2},
    "k8s10182":   {"project": "Kubernetes",   "mechanism": "chan + mutex",      "goroutines": 5},
    "k8s26980":   {"project": "Kubernetes",   "mechanism": "mutex + chan",      "goroutines": 4},
    "moby28462":  {"project": "Moby",         "mechanism": "chan + mutex",      "goroutines": 3},
    "k8s1321":    {"project": "Kubernetes",   "mechanism": "instruction-level", "goroutines": 3},
    "etcd7492":    {"project": "etcd",         "mechanism": "timer + mutex",     "goroutines": 5},
    "serving2137": {"project": "Knative",      "mechanism": "WG + buf chan",     "goroutines": 3},
}

# Miss reasons for the 3 undetected bugs.
MISS_REASONS = {
    "k8s1321":     "instruction-level preemption needed",
    "etcd7492":    "timer-scheduling gap",
    "serving2137": "non-blocking-sync atomicity",
}

# Goroutine role map for etcd#7443 (from trace analysis)
BGID_ROLES = {
    1: "tRunner",
    2: "test root",
    3: "sb.Close",
    4: "conn.Close",
    5: "lbWatcher",
    6: "resetTransport",
    7: "resetTransport",
    8: "resetTransport",
}


def load_summary(bug, strategy):
    path = DATA_DIR / f"{bug}-{strategy}.jsonl"
    if not path.exists():
        return []
    records = []
    with open(path) as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))
    return records


def load_traces(bug, strategy):
    path = DATA_DIR / f"{bug}-{strategy}-trace.jsonl"
    if not path.exists():
        return []
    records = []
    with open(path) as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))
    return records


def load_tree(bug, strategy):
    path = DATA_DIR / f"{bug}-{strategy}-tree.json"
    if not path.exists():
        return []
    return json.load(open(path))


def load_seed_sweep():
    # seed-sweep.json lives in charts/data/, not the GoBench data dir
    path = Path(__file__).parent / "data" / "seed-sweep.json"
    if not path.exists():
        return []
    return json.load(open(path))


def first_bug_run(records):
    for r in records:
        if not r.get("passed", True):
            return r.get("run_num", 0)
    return None


# ── Figure 1: Runs to first bug ──

def fig_runs_to_first_bug():
    FIG_DIR.mkdir(parents=True, exist_ok=True)

    # Horizontal grouped bar chart — avoids label overlap with 14 bugs
    fig, ax = plt.subplots(figsize=(8, 8))
    y = np.arange(len(BUGS))
    height = 0.18
    n_strats = len(STRATEGIES)

    for i, strat in enumerate(STRATEGIES):
        vals = []
        for bug in BUGS:
            records = load_summary(bug, strat)
            fb = first_bug_run(records)
            vals.append(fb if fb is not None else 0)
        offset = (i - n_strats / 2 + 0.5) * height
        bars = ax.barh(y + offset, vals, height,
                       label=STRATEGY_LABELS[strat], color=STRATEGY_COLORS[strat],
                       edgecolor="white", linewidth=0.3)
        for bar, v in zip(bars, vals):
            if v > 0:
                ax.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height() / 2,
                        str(v), ha="left", va="center", fontsize=7, color="#333")

    ax.set_yticks(y)
    ax.set_yticklabels([BUG_LABELS[b] for b in BUGS], fontsize=9)
    ax.set_xlabel("Runs to First Bug", fontsize=10)
    ax.set_title("GoBench: Runs to First Bug by Strategy", fontsize=12)
    ax.legend(loc="lower right", fontsize=8)
    ax.set_xlim(left=0)
    ax.invert_yaxis()
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    fig.tight_layout()
    fig.savefig(FIG_DIR / "runs_to_first_bug.png", dpi=150)
    plt.close(fig)
    print(f"  -> runs_to_first_bug.png")


# ── Figure 2: Tool comparison table ──

def fig_tool_comparison():
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    lines = [
        "| Bug | Flakiness | Go DD | Goleak | GCatch | GFuzz | GoAT | synctest (CHESS) |",
        "|-----|-----------|-------|--------|--------|-------|------|------------------|",
    ]
    for bug in BUGS:
        tools = TOOL_RESULTS[bug]
        records = load_summary(bug, "chess")
        fb = first_bug_run(records)
        synctest = f"run {fb}" if fb else "not found"
        lines.append(
            f"| {BUG_LABELS[bug]} | {FLAKINESS[bug]} "
            f"| {tools['Go DD']} | {tools['Goleak']} | {tools['GCatch']} "
            f"| {tools['GFuzz']} | {tools['GoAT']} | **{synctest}** |"
        )
    table = "\n".join(lines)
    with open(FIG_DIR / "tool_comparison.md", "w") as f:
        f.write(table + "\n")
    print(f"  -> tool_comparison.md")
    print(table)


# ── Figure 3: Summary table ──

def fig_summary_table():
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    lines = [
        "| Bug | Strategy | Runs to Bug | Decisions | Branch Points | Non-FIFO |",
        "|-----|----------|-------------|-----------|---------------|----------|",
    ]
    for bug in BUGS:
        for strat in STRATEGIES:
            records = load_summary(bug, strat)
            fb = first_bug_run(records)
            total = len(records)
            decisions = branches = nonfifo = "-"
            if fb is not None and fb <= len(records):
                rec = records[fb - 1]
                decisions = str(rec.get("total_decisions", "-"))
                branches = str(rec.get("branch_points", "-"))
                nonfifo = str(rec.get("non_fifo", "-"))
            fb_str = str(fb) if fb else f">{total}"
            lines.append(
                f"| {BUG_LABELS[bug]} | {STRATEGY_LABELS[strat]} "
                f"| {fb_str} | {decisions} | {branches} | {nonfifo} |"
            )
    with open(FIG_DIR / "summary_table.md", "w") as f:
        f.write("\n".join(lines) + "\n")
    print(f"  -> summary_table.md")


# ── Figure 4: CHESS exploration tree for etcd#7443 ──

def fig_chess_tree():
    tree = load_tree("etcd7443", "chess")
    if not tree:
        print("  [skip] chess_tree_etcd7443.png (no tree data)")
        return

    FIG_DIR.mkdir(parents=True, exist_ok=True)

    # Build adjacency: parent -> children
    children = {i: [] for i in range(len(tree))}
    for i, node in enumerate(tree):
        if node["Parent"] >= 0:
            children[node["Parent"]].append(i)

    # Find root-to-bug path
    bug_idx = None
    for i, node in enumerate(tree):
        if not node["Passed"]:
            bug_idx = i
            break
    bug_path = set()
    bug_path_edges = set()
    if bug_idx is not None:
        cur = bug_idx
        path_list = [cur]
        while tree[cur]["Parent"] >= 0:
            parent = tree[cur]["Parent"]
            bug_path_edges.add((parent, cur))
            cur = parent
            path_list.append(cur)
        bug_path = set(path_list)

    # Assign x positions via DFS (leaf ordering)
    x_pos = {}
    y_pos = {}
    leaf_counter = [0]

    def layout(idx, depth):
        y_pos[idx] = -depth
        kids = children[idx]
        if not kids:
            x_pos[idx] = leaf_counter[0]
            leaf_counter[0] += 1
        else:
            for c in kids:
                layout(c, depth + 1)
            x_pos[idx] = np.mean([x_pos[c] for c in kids])

    layout(0, 0)

    # Depth band info
    depth_counts = {}
    for node in tree:
        d = node["NonFIFO"]
        depth_counts[d] = depth_counts.get(d, 0) + 1

    fig, ax = plt.subplots(figsize=(14, 6))
    x_min, x_max = min(x_pos.values()) - 1, max(x_pos.values()) + 1

    # Draw horizontal depth band shading
    band_colors = ["#f5f5f5", "#eaeaea"]
    for i, d in enumerate(sorted(depth_counts.keys())):
        ax.axhspan(-d - 0.4, -d + 0.4, color=band_colors[i % 2], zorder=0)
        ax.text(x_max + 1, -d, f"k={d} ({depth_counts[d]} runs)",
                ha="left", va="center", fontsize=9, color="#666")

    # Draw edges (normal)
    for i, node in enumerate(tree):
        if node["Parent"] >= 0:
            px, py = x_pos[node["Parent"]], y_pos[node["Parent"]]
            cx, cy = x_pos[i], y_pos[i]
            if (node["Parent"], i) in bug_path_edges:
                continue  # draw these on top
            ax.plot([px, cx], [py, cy], color="#ccc", linewidth=0.5, zorder=1)

    # Draw bold root-to-bug path edges
    for (p, c) in bug_path_edges:
        px, py = x_pos[p], y_pos[p]
        cx, cy = x_pos[c], y_pos[c]
        ax.plot([px, cx], [py, cy], color="#e53935", linewidth=2.5, zorder=3)

    # Draw nodes
    for i, node in enumerate(tree):
        if not node["Passed"]:
            color, size, marker = "#e53935", 60, "X"
        elif i in bug_path:
            color, size, marker = "#e53935", 30, "o"
        else:
            color, size, marker = "#4CAF50", 12, "o"
        ax.scatter(x_pos[i], y_pos[i], c=color, s=size, marker=marker,
                   zorder=4 if i in bug_path else 2, edgecolors="none")

    ax.set_title(f"etcd#7443 CHESS Exploration Tree ({len(tree)} runs, bug at run {tree[-1]['Run']})",
                 fontsize=12)
    ax.set_xlabel("Trace (leaf ordering)")
    ax.set_ylabel("Context depth (non-FIFO decisions)")
    ax.set_yticks([-d for d in sorted(depth_counts.keys())])
    ax.set_yticklabels([f"k={d}" for d in sorted(depth_counts.keys())])
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    legend_elements = [
        plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='#4CAF50',
                   markersize=6, label=f'Pass ({sum(1 for n in tree if n["Passed"])})'),
        plt.Line2D([0], [0], marker='X', color='w', markerfacecolor='#e53935',
                   markersize=8, label=f'Bug (run {tree[-1]["Run"]})'),
        plt.Line2D([0], [0], color='#e53935', linewidth=2.5,
                   label='Root \u2192 bug path'),
    ]
    ax.legend(handles=legend_elements, loc="upper right")

    fig.tight_layout()
    fig.savefig(FIG_DIR / "chess_tree_etcd7443.png", dpi=150)
    plt.close(fig)
    print(f"  -> chess_tree_etcd7443.png")


# ── Figure 5: Trace comparison (FIFO vs bug) ──

def fig_trace_comparison():
    traces = load_traces("etcd7443", "chess")
    if len(traces) < 2:
        print("  [skip] trace_comparison.png (not enough trace data)")
        return

    safe_trace = traces[0]   # run 1 (FIFO, passed)
    bug_trace = traces[-1]   # last run (failed)

    FIG_DIR.mkdir(parents=True, exist_ok=True)
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

    def draw_swimlane(ax, trace_rec, title):
        steps = trace_rec["steps"]
        all_bgids = sorted(set(
            bgid for s in steps for bgid in s.get("runq_bgids", [s["chosen_bgid"]])
        ))
        bgid_y = {bg: i for i, bg in enumerate(all_bgids)}

        for step_i, s in enumerate(steps):
            chosen = s["chosen_bgid"]
            runq = s.get("runq_bgids", [chosen])
            # Draw all runnable as gray dots
            for bg in runq:
                if bg != chosen:
                    ax.scatter(step_i, bgid_y[bg], c="#ddd", s=60, zorder=1,
                               edgecolors="#bbb", linewidth=0.5)
            # Draw chosen as colored dot
            is_nonfifo = s["index"] != 0
            color = "#e53935" if is_nonfifo else "#2196F3"
            ax.scatter(step_i, bgid_y[chosen], c=color, s=80, zorder=2,
                       edgecolors="black", linewidth=0.5)

        ax.set_yticks(range(len(all_bgids)))
        ax.set_yticklabels([f"B{bg} ({BGID_ROLES.get(bg, '?')})" for bg in all_bgids],
                           fontsize=8)
        ax.set_xlabel("Decision step")
        ax.set_title(title, fontsize=10)
        ax.set_xlim(-0.5, len(steps) - 0.5)
        ax.invert_yaxis()

    draw_swimlane(ax1, safe_trace,
                  f"Run 1 (FIFO) — {len(safe_trace['steps'])} steps, PASS")
    draw_swimlane(ax2, bug_trace,
                  f"Run {bug_trace['run_num']} — {len(bug_trace['steps'])} steps, DEADLOCK")

    legend_elements = [
        plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='#2196F3',
                   markersize=8, label='FIFO choice'),
        plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='#e53935',
                   markersize=8, label='Non-FIFO choice'),
        plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='#ddd',
                   markersize=8, markeredgecolor='#bbb', label='Runnable (not chosen)'),
    ]
    fig.legend(handles=legend_elements, loc="lower center", ncol=3, fontsize=9)
    fig.suptitle("etcd#7443: Safe vs Deadlocking Interleaving", fontsize=12, y=1.02)
    fig.tight_layout()
    fig.savefig(FIG_DIR / "trace_comparison.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> trace_comparison.png")


# ── Figure 6: Non-FIFO over runs (CHESS etcd#7443) ──

def fig_nonfifo_over_runs():
    records = load_summary("etcd7443", "chess")
    if not records:
        print("  [skip] nonfifo_over_runs.png (no data)")
        return

    FIG_DIR.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(12, 4))

    runs = [r["run_num"] for r in records]
    nonfifo = [r["non_fifo"] for r in records]
    passed = [r["passed"] for r in records]

    colors = ["#4CAF50" if p else "#e53935" for p in passed]
    ax.bar(runs, nonfifo, color=colors, width=1.0, edgecolor="none")

    ax.set_xlabel("Run number")
    ax.set_ylabel("Non-FIFO decisions")
    ax.set_title("etcd#7443 CHESS: Non-FIFO Decisions per Run (DFS progression)")
    ax.axhline(y=3, color="#999", linestyle="--", linewidth=0.8, label="Context bound k=3")
    ax.legend()

    legend_elements = [
        mpatches.Patch(color='#4CAF50', label='Pass'),
        mpatches.Patch(color='#e53935', label='Bug (deadlock)'),
        plt.Line2D([0], [0], color='#999', linestyle='--', label='Bound k=3'),
    ]
    ax.legend(handles=legend_elements, loc="upper left")

    fig.tight_layout()
    fig.savefig(FIG_DIR / "nonfifo_over_runs.png", dpi=150)
    plt.close(fig)
    print(f"  -> nonfifo_over_runs.png")


# ── Figure 7: Seed variance (etcd#7443) ──

def fig_seed_variance():
    sweep = load_seed_sweep()
    if not sweep:
        print("  [skip] seed_variance.png (no seed-sweep.json)")
        return

    FIG_DIR.mkdir(parents=True, exist_ok=True)

    # CHESS is deterministic
    chess_records = load_summary("etcd7443", "chess")
    chess_runs = first_bug_run(chess_records) or 74

    fig, ax = plt.subplots(figsize=(10, 5))

    strat_data = {}
    for r in sweep:
        s = r["strategy"]
        if s not in strat_data:
            strat_data[s] = []
        fb = r["first_bug"] if r["first_bug"] > 0 else r["runs"]
        strat_data[s].append(fb)

    positions = []
    labels = []
    colors_list = []

    strat_order = ["random", "pct-d2", "pct-d3"]
    color_map = {"random": "#9C27B0", "pct-d2": "#FF9800", "pct-d3": "#4CAF50"}
    label_map = {"random": "Random", "pct-d2": "PCT(d=2)", "pct-d3": "PCT(d=3)"}

    for i, s in enumerate(strat_order):
        if s in strat_data:
            vals = strat_data[s]
            bp = ax.boxplot([vals], positions=[i], widths=0.5, patch_artist=True,
                           showfliers=True, flierprops=dict(markersize=3))
            bp["boxes"][0].set_facecolor(color_map[s])
            bp["boxes"][0].set_alpha(0.6)
            bp["medians"][0].set_color("black")
            positions.append(i)
            labels.append(f"{label_map[s]}\n(n={len(vals)})")

    # CHESS baseline
    ax.axhline(y=chess_runs, color="#2196F3", linewidth=2, linestyle="-",
               label=f"CHESS k=3 ({chess_runs} runs, deterministic)")

    ax.set_xticks(range(len(strat_order)))
    ax.set_xticklabels(labels)
    ax.set_ylabel("Runs to First Bug")
    ax.set_title("etcd#7443: Strategy Comparison (100 seeds each)")
    ax.legend(loc="upper right")
    ax.set_ylim(bottom=0)

    # Annotate stats
    for i, s in enumerate(strat_order):
        if s in strat_data:
            vals = strat_data[s]
            found = [v for v in vals if v <= 500]
            ax.text(i, max(vals) + 5,
                    f"avg={np.mean(found):.0f}\nmax={max(found)}",
                    ha="center", va="bottom", fontsize=8, color="#666")

    fig.tight_layout()
    fig.savefig(FIG_DIR / "seed_variance.png", dpi=150)
    plt.close(fig)
    print(f"  -> seed_variance.png")


# ── Figure 8: Decision space (etcd#7443 bug trace) ──

def fig_decision_space():
    traces = load_traces("etcd7443", "chess")
    if not traces:
        print("  [skip] decision_space.png (no trace data)")
        return

    bug_trace = traces[-1]
    steps = bug_trace["steps"]

    FIG_DIR.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(10, 4))

    x = range(len(steps))
    alts = [s["alternatives"] for s in steps]
    chosen_idx = [s["index"] for s in steps]
    is_nonfifo = [s["index"] != 0 for s in steps]

    bars = ax.bar(x, alts, color=["#e53935" if nf else "#2196F3" for nf in is_nonfifo],
                  edgecolor="black", linewidth=0.5)

    # Mark the chosen index on each bar
    for i, (a, idx) in enumerate(zip(alts, chosen_idx)):
        if a > 1:
            chosen_bgid = steps[i]["chosen_bgid"]
            role = BGID_ROLES.get(chosen_bgid, "?")
            ax.text(i, a + 0.15, f"B{chosen_bgid}\n({role})",
                    ha="center", va="bottom", fontsize=7, color="#333")

    # Theoretical space
    total_space = 1
    for a in alts:
        total_space *= a
    ax.set_xlabel("Decision step")
    ax.set_ylabel("Alternatives (runq size)")
    ax.set_title(f"etcd#7443 Bug Trace: Decision Space "
                 f"({len(steps)} steps, {total_space} total paths, 3 non-FIFO)")
    ax.set_xticks(x)
    ax.set_xticklabels([f"Step {i}" for i in x], fontsize=8)
    ax.set_ylim(0, max(alts) + 2)

    legend_elements = [
        mpatches.Patch(color='#e53935', label='Non-FIFO (divergent)'),
        mpatches.Patch(color='#2196F3', label='FIFO (default)'),
    ]
    ax.legend(handles=legend_elements)

    fig.tight_layout()
    fig.savefig(FIG_DIR / "decision_space.png", dpi=150)
    plt.close(fig)
    print(f"  -> decision_space.png")


# ── Figure 9: Bug depth vs exploration cost (council-designed) ──

def fig_bug_depth_vs_cost():
    """Scatter: non-FIFO depth required to trigger each bug vs CHESS runs to find it.
    Core scientific claim: depth is the dominant cost predictor."""
    FIG_DIR.mkdir(parents=True, exist_ok=True)

    depths = []
    runs = []
    labels = []
    trace_lens = []

    for bug in BUGS:
        tree = load_tree(bug, "chess")
        if not tree:
            continue
        for node in tree:
            if not node["Passed"]:
                depths.append(node["NonFIFO"])
                runs.append(node["Run"])
                labels.append(BUG_LABELS[bug])
                trace_lens.append(node.get("TraceLen", 5))
                break

    if not depths:
        print("  [skip] bug_depth_vs_cost.png (no tree data)")
        return

    fig, ax = plt.subplots(figsize=(7, 5))

    # Scatter — size by trace length
    sizes = [max(40, tl * 15) for tl in trace_lens]
    ax.scatter(depths, runs, s=sizes, c="#2196F3", alpha=0.7,
               edgecolors="#333", linewidth=0.8, zorder=3)

    # Label each point
    for d, r, lbl in zip(depths, runs, labels):
        # Offset to avoid overlap
        offset_x, offset_y = 0.08, 0
        if r > 10:
            offset_y = 3
        ax.annotate(lbl, (d, r), textcoords="offset points",
                    xytext=(5, offset_y + 4), fontsize=7, color="#333")

    # Fit exponential trend if enough points
    unique_depths = sorted(set(depths))
    if len(unique_depths) >= 3:
        # Average runs per depth
        depth_avg = {}
        for d, r in zip(depths, runs):
            depth_avg.setdefault(d, []).append(r)
        xs = sorted(depth_avg.keys())
        ys = [np.mean(depth_avg[x]) for x in xs]
        # Fit: runs ~ a * b^depth
        if ys[-1] > ys[0] and all(y > 0 for y in ys):
            from numpy.polynomial import polynomial as P
            log_ys = [np.log(y) for y in ys]
            coeffs = np.polyfit(xs, log_ys, 1)
            x_fit = np.linspace(min(xs) - 0.2, max(xs) + 0.3, 50)
            y_fit = np.exp(np.polyval(coeffs, x_fit))
            ax.plot(x_fit, y_fit, "--", color="#e53935", alpha=0.5, linewidth=1.5,
                    label=f"Exponential trend")

    ax.set_xlabel("Bug depth (minimum non-FIFO decisions required)", fontsize=11)
    ax.set_ylabel("CHESS runs to first bug", fontsize=11)
    ax.set_title("Context Bound Determines Exploration Cost", fontsize=12)
    ax.set_xticks(range(max(depths) + 1))
    ax.set_xticklabels([f"k={d}" for d in range(max(depths) + 1)])
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    # Annotate insight
    n_k2 = sum(1 for d in depths if d <= 2)
    ax.text(0.02, 0.95, f"{n_k2}/{len(depths)} bugs found at k\u22642",
            transform=ax.transAxes, fontsize=9, color="#666", va="top")

    if any(d >= 3 for d in depths):
        ax.legend(loc="center right", fontsize=8)

    fig.tight_layout()
    fig.savefig(FIG_DIR / "bug_depth_vs_cost.png", dpi=150)
    plt.close(fig)
    print(f"  -> bug_depth_vs_cost.png")


# ── Figure 9b: Exploration efficiency (council-designed) ──

def fig_exploration_efficiency():
    """Two-panel: (a) forced-step ratio per bug, (b) unique trace ratio per strategy."""
    FIG_DIR.mkdir(parents=True, exist_ok=True)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))

    # Panel 1: Forced-step ratio (alternatives==1) per bug × strategy
    detected_bugs = [b for b in BUGS if first_bug_run(load_summary(b, "chess")) is not None]
    if not detected_bugs:
        detected_bugs = BUGS[:5]

    forced_data = {}  # {bug: {strat: ratio}}
    for bug in detected_bugs:
        forced_data[bug] = {}
        for strat in STRATEGIES:
            traces = load_traces(bug, strat)
            if not traces:
                continue
            total_steps = 0
            forced_steps = 0
            for t in traces:
                for s in t["steps"]:
                    total_steps += 1
                    if s["alternatives"] == 1:
                        forced_steps += 1
            if total_steps > 0:
                forced_data[bug][strat] = forced_steps / total_steps

    # Horizontal grouped bar
    y = np.arange(len(detected_bugs))
    height = 0.18
    for i, strat in enumerate(STRATEGIES):
        vals = [forced_data.get(b, {}).get(strat, 0) for b in detected_bugs]
        offset = (i - len(STRATEGIES) / 2 + 0.5) * height
        ax1.barh(y + offset, vals, height, label=STRATEGY_LABELS[strat],
                 color=STRATEGY_COLORS[strat], edgecolor="white", linewidth=0.3)

    ax1.set_yticks(y)
    ax1.set_yticklabels([BUG_LABELS[b] for b in detected_bugs], fontsize=8)
    ax1.set_xlabel("Fraction of forced steps (alternatives=1)", fontsize=10)
    ax1.set_title("Wasted Work: Forced Steps", fontsize=11)
    ax1.set_xlim(0, 1)
    ax1.invert_yaxis()
    ax1.legend(fontsize=7, loc="lower right")
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)

    # Panel 2: Unique trace ratio per strategy for etcd#7443
    ax = ax2
    bug = "etcd7443"
    strat_names = []
    total_runs_list = []
    unique_runs_list = []

    for strat in STRATEGIES:
        traces = load_traces(bug, strat)
        if not traces:
            continue
        # Fingerprint: tuple of (chosen_bgid, index) per step
        fingerprints = set()
        for t in traces:
            fp = tuple((s["chosen_bgid"], s["index"]) for s in t["steps"])
            fingerprints.add(fp)
        strat_names.append(STRATEGY_LABELS[strat])
        total_runs_list.append(len(traces))
        unique_runs_list.append(len(fingerprints))

    if strat_names:
        x = np.arange(len(strat_names))
        w = 0.35
        bars1 = ax.bar(x - w / 2, total_runs_list, w, label="Total runs",
                        color="#90CAF9", edgecolor="#333", linewidth=0.5)
        bars2 = ax.bar(x + w / 2, unique_runs_list, w, label="Unique traces",
                        color="#2196F3", edgecolor="#333", linewidth=0.5)

        # Annotate duplicate %
        for xi, (total, unique) in enumerate(zip(total_runs_list, unique_runs_list)):
            if total > 0:
                dup_pct = (1 - unique / total) * 100
                ax.text(xi, max(total, unique) + 1, f"{dup_pct:.0f}% dup",
                        ha="center", fontsize=8, color="#e53935" if dup_pct > 20 else "#666")

        ax.set_xticks(x)
        ax.set_xticklabels(strat_names, fontsize=9)
        ax.set_ylabel("Number of runs/traces")
        ax.set_title(f"Search Redundancy: {BUG_LABELS[bug]}", fontsize=11)
        ax.legend(fontsize=8)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    fig.suptitle("Exploration Efficiency Metrics", fontsize=13, y=1.02)
    fig.tight_layout()
    fig.savefig(FIG_DIR / "exploration_efficiency.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> exploration_efficiency.png")


# ── Figure 10: CDF of runs-to-bug across seeds (Chart 3) ──

def fig_cdf_runs_to_bug():
    sweep = load_seed_sweep()
    if not sweep:
        print("  [skip] cdf_runs_to_bug.png (no seed-sweep.json)")
        return

    FIG_DIR.mkdir(parents=True, exist_ok=True)

    chess_records = load_summary("etcd7443", "chess")
    chess_runs = first_bug_run(chess_records) or 74

    strat_data = {}
    for r in sweep:
        s = r["strategy"]
        fb = r["first_bug"] if r["first_bug"] > 0 else None
        strat_data.setdefault(s, []).append(fb)

    fig, ax = plt.subplots(figsize=(8, 4.5))

    strat_order = ["random", "pct-d2", "pct-d3"]
    strat_meta = {
        "random": ("#9C27B0", "Random"),
        "pct-d2": ("#FF9800", "PCT(d=2)"),
        "pct-d3": ("#4CAF50", "PCT(d=3)"),
    }

    y_positions = {}
    for i, strat in enumerate(strat_order):
        vals = strat_data.get(strat, [])
        if not vals:
            continue
        found = [v for v in vals if v is not None]
        not_found = sum(1 for v in vals if v is None)
        color, label = strat_meta[strat]
        y = i

        # Jitter each seed dot vertically so they don't stack
        jitter = np.random.default_rng(42).uniform(-0.15, 0.15, len(found))
        ax.scatter(found, [y + j for j in jitter[:len(found)]],
                   c=color, s=18, alpha=0.5, edgecolors="none", zorder=3)

        # Median and P95 markers
        if found:
            med = float(np.median(found))
            p95 = float(np.percentile(found, 95))
            worst = max(found)
            ax.plot(med, y, '|', color=color, markersize=20, markeredgewidth=2.5, zorder=5)
            ax.text(med, y + 0.28, f"med={med:.0f}", ha="center", fontsize=8,
                    color=color, fontweight="bold")
            ax.plot(worst, y, 'x', color=color, markersize=8, markeredgewidth=1.5, zorder=5)
            ax.text(worst, y + 0.28, f"worst={worst}", ha="center", fontsize=7,
                    color="#999")

        y_positions[strat] = y

    # CHESS line — always exactly 74
    ax.axvline(x=chess_runs, color="#2196F3", linewidth=2, linestyle="-",
               zorder=4, alpha=0.8)
    ax.text(chess_runs + 1.5, len(strat_order) / 2,
            f"CHESS\n({chess_runs} runs)",
            fontsize=9, color="#1565C0", fontweight="bold", va="center")

    ax.set_yticks(range(len(strat_order)))
    ax.set_yticklabels([strat_meta[s][1] for s in strat_order], fontsize=10)
    ax.set_xlabel("Runs to find etcd#7443 bug", fontsize=11)
    ax.set_title("etcd#7443: Each dot = one seed (100 seeds per strategy)", fontsize=12)
    ax.set_xlim(0, max(130, chess_runs + 20))
    ax.set_ylim(-0.5, len(strat_order) - 0.3)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(False)
    ax.grid(axis="x", alpha=0.2)

    fig.tight_layout()
    fig.savefig(FIG_DIR / "cdf_runs_to_bug.png", dpi=150)
    plt.close(fig)
    print(f"  -> cdf_runs_to_bug.png")


# ── Figure 11: Decision anatomy of etcd#7443 bug trace (Chart 4) ──

def fig_decision_anatomy():
    traces = load_traces("etcd7443", "chess")
    if not traces:
        print("  [skip] decision_anatomy_etcd7443.png (no trace data)")
        return

    bug_trace = traces[-1]
    if bug_trace.get("passed", True):
        print("  [skip] decision_anatomy_etcd7443.png (last trace is not a bug)")
        return

    steps = bug_trace["steps"]
    FIG_DIR.mkdir(parents=True, exist_ok=True)

    # Swimlane layout: each goroutine gets a row, each step is a column.
    # Show the full runq at each step, highlight the chosen goroutine.
    all_bgids = sorted(set(
        bg for s in steps for bg in s.get("runq_bgids", [s["chosen_bgid"]])
    ))
    bgid_y = {bg: i for i, bg in enumerate(all_bgids)}
    n_steps = len(steps)
    non_fifo_count = sum(1 for s in steps if s["index"] != 0)

    fig, ax = plt.subplots(figsize=(max(8, n_steps * 1.1), max(4, len(all_bgids) * 0.7)))

    # Draw goroutine lifelines
    for bg in all_bgids:
        ax.axhline(y=bgid_y[bg], color="#f0f0f0", linewidth=0.8, zorder=0)

    for step_i, s in enumerate(steps):
        chosen = s["chosen_bgid"]
        runq = s.get("runq_bgids", [chosen])
        idx = s["index"]
        is_nonfifo = idx != 0

        # Draw all runnable goroutines as gray circles
        for bg in runq:
            if bg != chosen:
                ax.scatter(step_i, bgid_y[bg], c="#ddd", s=120, zorder=1,
                           edgecolors="#bbb", linewidth=0.5)

        # Draw chosen goroutine — red if non-FIFO, blue if FIFO
        color = "#e53935" if is_nonfifo else "#2196F3"
        ax.scatter(step_i, bgid_y[chosen], c=color, s=160, zorder=3,
                   edgecolors="#333", linewidth=0.8)

        # Label the chosen with its index
        ax.text(step_i, bgid_y[chosen], str(idx), ha="center", va="center",
                fontsize=7, color="white", fontweight="bold", zorder=4)

        # Non-FIFO annotation — alternate above/below to avoid overlap
        if is_nonfifo:
            role = BGID_ROLES.get(chosen, "?")
            # Alternate direction based on step index
            if step_i % 2 == 0:
                offset_y, va = -18, "top"
            else:
                offset_y, va = 18, "bottom"
            ax.annotate(f"{role} [{idx}/{len(runq)}]",
                        (step_i, bgid_y[chosen]),
                        textcoords="offset points", xytext=(0, offset_y),
                        ha="center", va=va, fontsize=7, color="#e53935",
                        fontweight="bold", zorder=5)

        # Step number below
        ax.text(step_i, -0.7, f"S{step_i}", ha="center", fontsize=7, color="#999")

    ax.set_yticks([bgid_y[bg] for bg in all_bgids])
    ax.set_yticklabels([f"B{bg} ({BGID_ROLES.get(bg, '?')})" for bg in all_bgids],
                       fontsize=8)
    ax.set_xlim(-0.8, n_steps - 0.2)
    ax.set_ylim(-1.2, len(all_bgids) - 0.3)
    ax.invert_yaxis()
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["bottom"].set_visible(False)

    ax.set_title(f"etcd#7443 Bug Trace: {n_steps} scheduling decisions, "
                 f"{non_fifo_count} non-FIFO \u2192 deadlock",
                 fontsize=11, pad=10)

    legend_elements = [
        plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='#2196F3',
                   markeredgecolor='#333', markersize=10, label='FIFO choice'),
        plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='#e53935',
                   markeredgecolor='#333', markersize=10, label='Non-FIFO choice'),
        plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='#ddd',
                   markeredgecolor='#bbb', markersize=8, label='Runnable (not chosen)'),
    ]
    ax.legend(handles=legend_elements, loc="lower right", fontsize=8,
              framealpha=0.9)

    fig.tight_layout()
    fig.savefig(FIG_DIR / "decision_anatomy_etcd7443.png", dpi=150)
    plt.close(fig)
    print(f"  -> decision_anatomy_etcd7443.png")


# ── Figure 12: Per-bug results table (Chart 5) ──

def fig_per_bug_table():
    FIG_DIR.mkdir(parents=True, exist_ok=True)

    rows = []
    for bug in BUGS:
        meta = BUG_METADATA[bug]
        records = load_summary(bug, "chess")
        fb = first_bug_run(records)
        detected = fb is not None
        reason = MISS_REASONS.get(bug, "")
        rows.append({
            "bug": BUG_LABELS[bug],
            "project": meta["project"],
            "mechanism": meta["mechanism"],
            "goroutines": meta["goroutines"],
            "chess_run": fb,
            "detected": detected,
            "reason": reason,
        })

    # Markdown output
    md_lines = [
        "| Bug | Project | Mechanism | Goroutines | CHESS Run | Detected | Notes |",
        "|-----|---------|-----------|------------|-----------|----------|-------|",
    ]
    for r in rows:
        run_str = str(r["chess_run"]) if r["chess_run"] else "--"
        det_str = "**Y**" if r["detected"] else "N"
        md_lines.append(
            f"| {r['bug']} | {r['project']} | {r['mechanism']} "
            f"| {r['goroutines']} | {run_str} | {det_str} | {r['reason']} |"
        )
    md_lines.append("")
    detected_count = sum(1 for r in rows if r["detected"])
    md_lines.append(f"**{detected_count}/{len(rows)}** detected.")
    md_text = "\n".join(md_lines)
    with open(FIG_DIR / "per_bug_results.md", "w") as f:
        f.write(md_text + "\n")
    print(f"  -> per_bug_results.md")

    # Matplotlib table figure
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.axis("off")

    col_labels = ["Bug", "Project", "Mechanism", "Goroutines", "CHESS Run", "Detected"]
    cell_text = []
    cell_colors = []
    for r in rows:
        run_str = str(r["chess_run"]) if r["chess_run"] else "--"
        det_str = "Y" if r["detected"] else "N"
        if r["reason"]:
            det_str += f" ({r['reason'][:20]})"
        cell_text.append([r["bug"], r["project"], r["mechanism"],
                          str(r["goroutines"]), run_str, det_str])
        if r["detected"]:
            cell_colors.append(["#e8f5e9"] * 6)
        else:
            cell_colors.append(["#ffebee"] * 6)

    table = ax.table(cellText=cell_text, colLabels=col_labels,
                     cellColours=cell_colors, loc="center",
                     colColours=["#e0e0e0"] * 6)
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1.0, 1.4)

    ax.set_title(f"GoBench Channel & Lock: {detected_count}/{len(rows)} Detected",
                 fontsize=13, fontweight="bold", pad=20)

    fig.tight_layout()
    fig.savefig(FIG_DIR / "per_bug_results.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> per_bug_results.png")


# ── Figure 13: Cumulative detection by context bound k (council-designed) ──

def fig_context_bound_sufficiency():
    """Step function: cumulative bugs detected as context bound k increases."""
    FIG_DIR.mkdir(parents=True, exist_ok=True)

    # Get depth for each detected bug from CHESS tree
    bug_depths = {}
    for bug in BUGS:
        tree = load_tree(bug, "chess")
        if not tree:
            continue
        for node in tree:
            if not node["Passed"]:
                bug_depths[bug] = node["NonFIFO"]
                break

    detected = [(bug, d) for bug, d in bug_depths.items()]
    undetected = [b for b in BUGS if b not in bug_depths]

    if not detected:
        print("  [skip] context_bound_sufficiency.png (no tree data)")
        return

    max_k = max(d for _, d in detected)

    fig, ax = plt.subplots(figsize=(7, 4.5))

    # Count bugs at each depth
    depth_bugs = {}
    for bug, d in detected:
        depth_bugs.setdefault(d, []).append(bug)

    # Step function: cumulative
    ks = list(range(max_k + 1))
    cumulative = []
    cum = 0
    bug_names_at_k = []
    for k in ks:
        bugs_here = depth_bugs.get(k, [])
        cum += len(bugs_here)
        cumulative.append(cum)
        bug_names_at_k.append(bugs_here)

    total_detected = len(detected)
    total_bugs = len(BUGS)

    # Draw step function
    ax.step(ks, cumulative, where="post", color="#2196F3", linewidth=2.5, zorder=3)
    ax.fill_between(ks, cumulative, step="post", alpha=0.1, color="#2196F3")

    # Dots at each k
    for i, k in enumerate(ks):
        ax.plot(k, cumulative[i], 'o', color="#2196F3", markersize=8, zorder=4)
        pct = cumulative[i] / total_bugs * 100
        # Bug names annotation
        names = ", ".join(BUG_LABELS[b] for b in bug_names_at_k[i])
        if len(bug_names_at_k[i]) <= 3:
            label = names
        else:
            label = f"{len(bug_names_at_k[i])} bugs"
        # Position label to avoid overlap
        offset_y = 12 if k % 2 == 0 else -16
        va = "bottom" if offset_y > 0 else "top"
        ax.annotate(f"+{len(bug_names_at_k[i])}: {label}\n({pct:.0f}% of {total_bugs})",
                    (k, cumulative[i]),
                    textcoords="offset points", xytext=(8, offset_y),
                    fontsize=7, color="#333", va=va,
                    arrowprops=dict(arrowstyle="-", color="#ccc", lw=0.5))

    # Undetected bugs line
    ax.axhline(y=total_bugs, color="#e53935", linewidth=1, linestyle="--", alpha=0.5)
    ax.text(max_k, total_bugs + 0.3,
            f"{len(undetected)} bugs outside model granularity",
            ha="right", fontsize=8, color="#e53935")

    ax.set_xlabel("Context bound k (max non-FIFO decisions per trace)", fontsize=11)
    ax.set_ylabel("Cumulative bugs detected", fontsize=11)
    ax.set_title("Most bugs need few non-FIFO decisions", fontsize=12)
    ax.set_xticks(ks)
    ax.set_xticklabels([f"k={k}" for k in ks])
    ax.set_ylim(0, total_bugs + 1.5)
    ax.set_xlim(-0.3, max_k + 0.5)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    fig.tight_layout()
    fig.savefig(FIG_DIR / "context_bound_sufficiency.png", dpi=150)
    plt.close(fig)
    print(f"  -> context_bound_sufficiency.png")


if __name__ == "__main__":
    print("Generating GoBench charts...")
    fig_tool_comparison()
    fig_chess_tree()
    fig_context_bound_sufficiency()
    fig_cdf_runs_to_bug()
    fig_decision_anatomy()
    print("Done.")
