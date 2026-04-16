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

DATA_DIR = Path(__file__).parent / "data"
FIG_DIR = Path(__file__).parent / "figures" / "gobench"

BUGS = ["grpc1353", "k8s6632", "etcd7902", "moby33781", "etcd7443"]
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
    "grpc1353": "grpc#1353",
    "k8s6632": "k8s#6632",
    "etcd7902": "etcd#7902",
    "moby33781": "moby#33781",
    "etcd7443": "etcd#7443",
}

TOOL_RESULTS = {
    "grpc1353":  {"Go DD": "X", "Goleak": "D", "GCatch": "X", "GFuzz": "D", "GoAT": "D"},
    "k8s6632":   {"Go DD": "X", "Goleak": "X", "GCatch": "D", "GFuzz": "X", "GoAT": "X"},
    "etcd7902":  {"Go DD": "X", "Goleak": "D", "GCatch": "D", "GFuzz": "X", "GoAT": "X"},
    "moby33781": {"Go DD": "X", "Goleak": "D", "GCatch": "D", "GFuzz": "X", "GoAT": "X"},
    "etcd7443":  {"Go DD": "X", "Goleak": "X", "GCatch": "X", "GFuzz": "X", "GoAT": "X"},
}

FLAKINESS = {
    "grpc1353": "100/100",
    "k8s6632": "4/100",
    "etcd7902": "100/100",
    "moby33781": "25/100",
    "etcd7443": "N/A",
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
    path = DATA_DIR / "seed-sweep.json"
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
    fig, ax = plt.subplots(figsize=(10, 5))
    x = np.arange(len(BUGS))
    width = 0.2

    for i, strat in enumerate(STRATEGIES):
        vals = []
        for bug in BUGS:
            records = load_summary(bug, strat)
            fb = first_bug_run(records)
            vals.append(fb if fb is not None else 0)
        bars = ax.bar(x + i * width, vals, width,
                      label=STRATEGY_LABELS[strat], color=STRATEGY_COLORS[strat])
        for bar, v in zip(bars, vals):
            if v > 0:
                ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                        str(v), ha="center", va="bottom", fontsize=8)

    ax.set_xlabel("Bug")
    ax.set_ylabel("Runs to First Bug")
    ax.set_title("GoBench Kernels: Runs to First Bug by Strategy")
    ax.set_xticks(x + width * 1.5)
    ax.set_xticklabels([BUG_LABELS[b] for b in BUGS])
    ax.legend()
    ax.set_ylim(bottom=0)
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

    fig, ax = plt.subplots(figsize=(14, 6))

    # Draw edges
    for i, node in enumerate(tree):
        if node["Parent"] >= 0:
            px, py = x_pos[node["Parent"]], y_pos[node["Parent"]]
            cx, cy = x_pos[i], y_pos[i]
            ax.plot([px, cx], [py, cy], color="#ccc", linewidth=0.5, zorder=1)

    # Draw nodes
    for i, node in enumerate(tree):
        color = "#e53935" if not node["Passed"] else "#4CAF50"
        size = 30 if not node["Passed"] else 12
        marker = "X" if not node["Passed"] else "o"
        ax.scatter(x_pos[i], y_pos[i], c=color, s=size, marker=marker,
                   zorder=2, edgecolors="none")

    # Depth labels
    depth_counts = {}
    for node in tree:
        d = node["NonFIFO"]
        depth_counts[d] = depth_counts.get(d, 0) + 1

    for d, count in sorted(depth_counts.items()):
        ax.text(-2, -d, f"k={d}\n({count})", ha="right", va="center",
                fontsize=9, color="#666")

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
                   markersize=8, label='Bug found (run 74)'),
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


# ── Figure 9: Intrinsic metrics ──

def fig_intrinsic_metrics():
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    fig, axes = plt.subplots(1, 3, figsize=(15, 4))

    # Panel 1: Decisions per run across all bugs × strategies
    ax = axes[0]
    data_by_bug = {}
    for bug in BUGS:
        for strat in STRATEGIES:
            records = load_summary(bug, strat)
            for r in records:
                key = BUG_LABELS[bug]
                data_by_bug.setdefault(key, []).append(r.get("total_decisions", 0))

    bug_names = [BUG_LABELS[b] for b in BUGS]
    bp = ax.boxplot([data_by_bug.get(b, []) for b in bug_names],
                    labels=bug_names, patch_artist=True, showfliers=False)
    for patch in bp["boxes"]:
        patch.set_facecolor("#2196F3")
        patch.set_alpha(0.5)
    ax.set_ylabel("Decisions per run")
    ax.set_title("Scheduling Decisions per Run")
    ax.tick_params(axis='x', rotation=30)

    # Panel 2: Branch points (decisions with >1 alternative)
    ax = axes[1]
    data_by_bug = {}
    for bug in BUGS:
        for strat in STRATEGIES:
            records = load_summary(bug, strat)
            for r in records:
                key = BUG_LABELS[bug]
                data_by_bug.setdefault(key, []).append(r.get("branch_points", 0))

    bp = ax.boxplot([data_by_bug.get(b, []) for b in bug_names],
                    labels=bug_names, patch_artist=True, showfliers=False)
    for patch in bp["boxes"]:
        patch.set_facecolor("#FF9800")
        patch.set_alpha(0.5)
    ax.set_ylabel("Branch points per run")
    ax.set_title("Branch Points (>1 alternative)")
    ax.tick_params(axis='x', rotation=30)

    # Panel 3: Runq size distribution from etcd#7443 traces
    ax = axes[2]
    traces = load_traces("etcd7443", "chess")
    all_runq_sizes = []
    for t in traces:
        for s in t["steps"]:
            all_runq_sizes.append(s["alternatives"])

    if all_runq_sizes:
        counts = {}
        for s in all_runq_sizes:
            counts[s] = counts.get(s, 0) + 1
        sizes = sorted(counts.keys())
        ax.bar(sizes, [counts[s] for s in sizes], color="#4CAF50", alpha=0.7,
               edgecolor="black", linewidth=0.5)
        ax.set_xlabel("Runq size (alternatives)")
        ax.set_ylabel("Frequency")
        ax.set_title("etcd#7443: Runq Size Distribution\n(all CHESS runs)")
        ax.set_xticks(sizes)

    fig.suptitle("Intrinsic Exploration Metrics", fontsize=13, y=1.02)
    fig.tight_layout()
    fig.savefig(FIG_DIR / "intrinsic_metrics.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> intrinsic_metrics.png")


if __name__ == "__main__":
    print("Generating GoBench charts...")
    fig_runs_to_first_bug()
    fig_tool_comparison()
    fig_summary_table()
    fig_chess_tree()
    fig_trace_comparison()
    fig_nonfifo_over_runs()
    fig_seed_variance()
    fig_decision_space()
    fig_intrinsic_metrics()
    print("Done.")
