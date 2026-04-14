#!/usr/bin/env python3
"""
Visualization for synctest exploration benchmarks.

Usage:
    python3 charts/charts.py --data charts/data --out charts/figures

Reads JSONL summary files, detailed trace files, and CHESS tree JSON
from --data and produces figures in --out.
"""

import argparse
import json
import os
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.lines import Line2D
import numpy as np

# ── Style ──

COLORS = {
    "global_fifo": "#4A90D9",   # blue
    "local_fifo":  "#F5A623",   # orange
    "non_fifo":    "#D0021B",   # red
    "found":       "#4A90D9",   # blue
    "not_found":   "#BDBDBD",   # gray
    "passed":      "#7ED321",   # green
    "failed":      "#D0021B",   # red
    "node_A":      "#4A90D9",   # blue
    "node_B":      "#F5A623",   # orange
    "node_C":      "#7ED321",   # green
    "edge":        "#CCCCCC",
}

ALGO_LABELS = {
    "chess-gl":     "CHESS\n(G+L, k=2)",
    "chess-global": "CHESS\n(G-only, k=2)",
    "pct-d2":       "PCT\n(d=2)",
    "pct-d3":       "PCT\n(d=3)",
    "random":       "Random",
}

def algo_label(name):
    return ALGO_LABELS.get(name, name)


# ── Data loading ──

def load_jsonl(path):
    records = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records

def load_json(path):
    with open(path) as f:
        return json.load(f)

def find_files(data_dir, suffix):
    return sorted(Path(data_dir).glob(f"*{suffix}"))


def load_all_summaries(data_dir):
    """Load all summary JSONL files, return dict of policy -> records."""
    summaries = {}
    for f in find_files(data_dir, ".jsonl"):
        if "-trace" in f.name:
            continue
        records = load_jsonl(f)
        if not records:
            continue
        policy = records[0].get("policy", f.stem)
        summaries[policy] = records
    return summaries


def load_all_traces(data_dir):
    """Load all detailed trace JSONL files, return dict of policy -> records."""
    traces = {}
    for f in find_files(data_dir, "-trace.jsonl"):
        records = load_jsonl(f)
        if not records:
            continue
        policy = records[0].get("policy", f.stem.replace("-trace", ""))
        traces[policy] = records
    return traces


def find_bug_run(records):
    """Find the first run with a real user-asserted failure (not just deadlock).
    If user_failed field is present, only count user_failed=True as bugs.
    Falls back to first non-passed run only for old data without the field."""
    has_user_failed_field = any("user_failed" in r for r in records)
    for r in records:
        if has_user_failed_field:
            if r.get("user_failed", False):
                return r
        else:
            # Old data without user_failed — fall back to passed=False
            if not r.get("passed", True):
                return r
    return None


def bug_trace_metrics(policy, summaries, traces):
    """Return paired summary/trace metrics for a bug-finding run, if present."""
    summary_bug = find_bug_run(summaries.get(policy, []))
    trace_bug = find_bug_run(traces.get(policy, []))
    if trace_bug is None:
        return None

    steps = trace_bug.get("steps", [])
    local_steps = [s for s in steps if s["kind"] == "local"]
    global_steps = [s for s in steps if s["kind"] == "global"]

    local_sizes = [len(s.get("runq_bgids", [])) or s.get("alternatives", 0)
                   for s in local_steps]
    if summary_bug and summary_bug.get("queue_sizes"):
        global_sizes = list(summary_bug["queue_sizes"])
    else:
        global_sizes = [s.get("alternatives", 0) for s in global_steps]

    explored_runs = summary_bug["run_num"] if summary_bug else len(summaries.get(policy, []))

    return {
        "summary_bug": summary_bug,
        "trace_bug": trace_bug,
        "steps": steps,
        "local_steps": local_steps,
        "global_steps": global_steps,
        "local_sizes": local_sizes,
        "global_sizes": global_sizes,
        "explored_runs": explored_runs,
    }


def search_space_upper_bound(local_sizes, global_sizes):
    """Approximate interleaving upper bound from local runqs and global queues."""
    total = 1
    for size in local_sizes + global_sizes:
        total *= max(1, size)
    return total


def human_large_int(n):
    if n < 1000:
        return str(n)
    exp = int(np.floor(np.log10(n)))
    mantissa = n / (10 ** exp)
    return f"{mantissa:.1f}e{exp}"


# ── Figure 1: Runs to first bug (bar chart) ──

def fig_runs_to_bug(summaries, out_dir):
    """Bar chart: how many runs each algorithm needed to find the bug.
    Clearly distinguishes 'found' from 'not found'."""
    results = {}
    found_bug = {}
    for policy, records in summaries.items():
        bug = find_bug_run(records)
        if bug:
            results[policy] = bug["run_num"]
            found_bug[policy] = True
        else:
            results[policy] = len(records)
            found_bug[policy] = False

    if not results:
        return

    # Sort: found bugs first (ascending), then not-found
    names = sorted(results.keys(),
                   key=lambda n: (not found_bug[n], results[n]))
    runs = [results[n] for n in names]
    colors = [COLORS["found"] if found_bug[n] else COLORS["not_found"] for n in names]
    labels = [algo_label(n) for n in names]

    fig, ax = plt.subplots(figsize=(8, 4.5))
    bars = ax.bar(labels, runs, color=colors, edgecolor='#333', linewidth=0.5, width=0.6)

    for bar, val, name in zip(bars, runs, names):
        if found_bug[name]:
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 2,
                    str(val), ha='center', va='bottom', fontsize=11, fontweight='bold')
        else:
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() / 2,
                    "not\nfound", ha='center', va='center',
                    fontsize=8, fontweight='bold', color='#888')
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 2,
                    str(val), ha='center', va='bottom', fontsize=9, color='#999')

    ax.set_ylabel("Runs", fontsize=11)
    ax.set_title("Runs to First Bug Detection", fontsize=13, fontweight='bold', pad=15)
    ax.set_ylim(0, max(runs) * 1.15)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

    found_patch = mpatches.Patch(color=COLORS["found"], label="Bug found")
    nf_patch = mpatches.Patch(color=COLORS["not_found"], label="No bug found")
    ax.legend(handles=[found_patch, nf_patch], loc='upper right', fontsize=9)

    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "runs_to_bug.png"), dpi=200, bbox_inches='tight')
    plt.close(fig)
    print("  runs_to_bug.png")


# ── Figure 2: Bug-finding trace swimlane (per-node timeline) ──

def fig_swimlane(traces, out_dir):
    """Swimlane diagram of the bug-finding trace. Each node gets a horizontal
    lane. Messages shown as arrows between lanes. Non-FIFO decisions highlighted."""
    for policy, records in traces.items():
        bug_run = find_bug_run(records)
        if bug_run is None:
            continue
        steps = bug_run.get("steps", [])
        if len(steps) < 2:
            continue

        # Collect node names
        nodes = sorted(set(s.get("node", "") for s in steps if s["kind"] == "local" and s.get("node")))
        if not nodes:
            continue

        node_y = {n: i for i, n in enumerate(nodes)}
        global_y = len(nodes)  # global lane at top

        fig_height = max(3.5, 1.2 * (len(nodes) + 1))
        fig, ax = plt.subplots(figsize=(max(10, len(steps) * 0.25), fig_height))

        # Draw horizontal lane lines
        for n, y in node_y.items():
            ax.axhline(y=y, color='#eee', linewidth=0.8, zorder=0)
        ax.axhline(y=global_y, color='#eee', linewidth=0.8, zorder=0)

        # Plot each step
        last_global_label_x = -999  # track to avoid overlapping global labels
        global_label_above = True
        for i, s in enumerate(steps):
            alts = s["alternatives"]
            idx = s["index"]
            is_nonfifo = idx != 0

            if s["kind"] == "global":
                y = global_y
                color = COLORS["non_fifo"] if is_nonfifo else COLORS["global_fifo"]
                size = max(30, alts * 18)
                ax.scatter(i, y, c=color, s=size, alpha=0.85, edgecolors='#333',
                          linewidth=0.4, zorder=3)

                # Draw arrow from sender to receiver
                fr = s.get("from", "")
                to = s.get("to", "")
                if fr in node_y and to in node_y:
                    y_from = node_y[fr]
                    y_to = node_y[to]
                    ax.annotate("", xy=(i, y_to), xytext=(i, y_from),
                               arrowprops=dict(arrowstyle="->", color=color,
                                              alpha=0.4, lw=0.8),
                               zorder=1)

                # Label non-FIFO global decisions, alternating above/below
                if is_nonfifo:
                    mt = s.get("msg_type", "")
                    fr_label = s.get("from", "?")
                    to_label = s.get("to", "?")
                    label = f"{fr_label}\u2192{to_label} {mt[:3]}"
                    # Alternate above/below when labels are close together
                    if i - last_global_label_x < 3:
                        global_label_above = not global_label_above
                    else:
                        global_label_above = True
                    offset_y = 10 if global_label_above else -12
                    va = 'bottom' if global_label_above else 'top'
                    ax.annotate(label, (i, y), textcoords="offset points",
                               xytext=(0, offset_y), ha='center', fontsize=5.5,
                               color=COLORS["non_fifo"], fontweight='bold',
                               va=va)
                    last_global_label_x = i
            else:
                node = s.get("node", "")
                y = node_y.get(node, 0)
                color = COLORS["non_fifo"] if is_nonfifo else COLORS["local_fifo"]
                size = max(15, alts * 12)
                ax.scatter(i, y, c=color, s=size, alpha=0.85, edgecolors='#333',
                          linewidth=0.3, zorder=3)

                # Label non-FIFO local decisions
                if is_nonfifo and alts > 1:
                    bgid = s.get("chosen_bgid", "?")
                    ax.annotate(f"B{bgid}", (i, y), textcoords="offset points",
                               xytext=(0, -10), ha='center', fontsize=5.5,
                               color=COLORS["non_fifo"], fontweight='bold')

        # Y-axis
        yticks = list(range(len(nodes))) + [global_y]
        yticklabels = [f"Node {n}" for n in nodes] + ["Global\nDelivery"]
        ax.set_yticks(yticks)
        ax.set_yticklabels(yticklabels, fontsize=9)
        ax.set_xlabel("Decision Step", fontsize=10)
        run_num = bug_run.get("run_num", "?")
        ax.set_title(f"Bug-Finding Trace Swimlane \u2014 {algo_label(policy).replace(chr(10), ' ')} "
                     f"(run {run_num}, {len(steps)} steps)",
                     fontsize=11, fontweight='bold', pad=12)
        ax.set_ylim(-0.5, global_y + 0.8)
        ax.set_xlim(-1, len(steps))
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)

        legend_elements = [
            Line2D([0], [0], marker='o', color='w', markerfacecolor=COLORS["global_fifo"],
                   markersize=7, label='Global (FIFO)'),
            Line2D([0], [0], marker='o', color='w', markerfacecolor=COLORS["local_fifo"],
                   markersize=7, label='Local (FIFO)'),
            Line2D([0], [0], marker='o', color='w', markerfacecolor=COLORS["non_fifo"],
                   markersize=7, label='Non-FIFO'),
        ]
        ax.legend(handles=legend_elements, loc='upper right', fontsize=8,
                 framealpha=0.9)

        fig.tight_layout()
        fig.savefig(os.path.join(out_dir, f"swimlane_{policy}.png"), dpi=200,
                   bbox_inches='tight')
        plt.close(fig)
        print(f"  swimlane_{policy}.png")


# ── Figure 3: Message sequence diagram ──

def fig_sequence_diagram(traces, out_dir):
    """UML-style sequence diagram of message deliveries in the bug-finding trace.
    Shows Request/Reply arrows between node lifelines."""
    for policy, records in traces.items():
        bug_run = find_bug_run(records)
        if bug_run is None:
            continue
        steps = bug_run.get("steps", [])
        if not steps:
            continue

        # Extract only global (message delivery) steps
        msgs = [s for s in steps if s["kind"] == "global"]
        if len(msgs) < 2:
            continue

        nodes = sorted(set(s.get("from", "") for s in msgs) |
                      set(s.get("to", "") for s in msgs))
        if not nodes:
            continue

        node_x = {n: i * 2 for i, n in enumerate(nodes)}
        n_msgs = len(msgs)

        fig_width = max(5, len(nodes) * 2.5)
        fig_height = max(4, n_msgs * 0.4 + 2)
        fig, ax = plt.subplots(figsize=(fig_width, fig_height))

        # Draw lifelines
        for n, x in node_x.items():
            ax.plot([x, x], [0, n_msgs + 1], color='#ccc', linewidth=1.5,
                    linestyle='--', zorder=0)
            ax.text(x, -0.5, f"Node {n}", ha='center', va='top', fontsize=10,
                   fontweight='bold')

        # Draw arrows
        for i, s in enumerate(msgs):
            y = i + 1
            fr = s.get("from", "")
            to = s.get("to", "")
            mt = s.get("msg_type", "?")
            idx = s["index"]
            alts = s["alternatives"]
            is_nonfifo = idx != 0

            x_from = node_x.get(fr, 0)
            x_to = node_x.get(to, 0)

            color = COLORS["non_fifo"] if is_nonfifo else '#666'
            lw = 1.8 if is_nonfifo else 1.0
            style = "->" if mt == "Request" else "-|>"

            ax.annotate("", xy=(x_to, y), xytext=(x_from, y),
                        arrowprops=dict(arrowstyle=style, color=color, lw=lw))

            # Label on arrow
            mid_x = (x_from + x_to) / 2
            offset_y = 0.15
            if is_nonfifo:
                label = f"{fr}\u2192{to} {mt} [{idx}/{alts}]"
            else:
                label = f"{fr}\u2192{to} {mt}"
            ax.text(mid_x, y + offset_y, label, ha='center', va='bottom',
                   fontsize=7, color=color,
                   fontweight='bold' if is_nonfifo else 'normal')

        ax.set_xlim(-1, max(node_x.values()) + 1)
        ax.set_ylim(n_msgs + 1.5, -1)
        ax.set_axis_off()

        run_num = bug_run.get("run_num", "?")
        ax.set_title(f"Message Sequence \u2014 {algo_label(policy).replace(chr(10), ' ')} "
                     f"(run {run_num})",
                     fontsize=11, fontweight='bold', pad=10)

        legend_elements = [
            Line2D([0], [0], color='#666', linewidth=1, label='FIFO delivery'),
            Line2D([0], [0], color=COLORS["non_fifo"], linewidth=1.8, label='Non-FIFO delivery'),
        ]
        ax.legend(handles=legend_elements, loc='lower right', fontsize=8,
                 framealpha=0.9)

        fig.tight_layout()
        fig.savefig(os.path.join(out_dir, f"sequence_{policy}.png"), dpi=200,
                   bbox_inches='tight')
        plt.close(fig)
        print(f"  sequence_{policy}.png")


# ── Figure 4: Non-FIFO decision comparison ──

def fig_nonfifo_comparison(summaries, out_dir):
    """Compare non-FIFO decision counts across algorithms for the bug-finding run.
    Stacked bar: global non-FIFO vs local non-FIFO."""
    data = {}
    for policy, records in summaries.items():
        bug = find_bug_run(records)
        if bug is None:
            continue
        data[policy] = {
            "total_nonfifo": bug.get("non_fifo", 0),
            "global_count": bug.get("global_decision_count", 0),
            "local_nonfifo": 0,
            "global_nonfifo": 0,
        }

    # We need the trace data for global vs local non-FIFO breakdown
    return data  # Will be filled from traces below


def fig_nonfifo_from_traces(traces, summaries, out_dir):
    """Non-FIFO breakdown per algorithm from trace data."""
    policies = []
    g_nonfifo = []
    l_nonfifo = []

    for policy in ["random", "pct-d2", "chess-gl", "pct-d3", "chess-global"]:
        if policy in traces:
            bug = find_bug_run(traces[policy])
            if bug and bug.get("steps"):
                steps = bug["steps"]
                gn = sum(1 for s in steps if s["kind"] == "global" and s["index"] != 0)
                ln = sum(1 for s in steps if s["kind"] == "local" and s["index"] != 0)
                policies.append(policy)
                g_nonfifo.append(gn)
                l_nonfifo.append(ln)
        elif policy in summaries:
            bug = find_bug_run(summaries[policy])
            if bug:
                policies.append(policy)
                g_nonfifo.append(bug.get("non_fifo", 0))
                l_nonfifo.append(0)

    if not policies:
        return

    labels = [algo_label(p) for p in policies]
    x = np.arange(len(policies))
    width = 0.5

    fig, ax = plt.subplots(figsize=(8, 4.5))
    bars_g = ax.bar(x, g_nonfifo, width, label="Global non-FIFO",
                    color=COLORS["global_fifo"], edgecolor='#333', linewidth=0.5)
    bars_l = ax.bar(x, l_nonfifo, width, bottom=g_nonfifo, label="Local non-FIFO",
                    color=COLORS["local_fifo"], edgecolor='#333', linewidth=0.5)

    for i, (g, l) in enumerate(zip(g_nonfifo, l_nonfifo)):
        total = g + l
        if total > 0:
            ax.text(i, total + 0.3, str(total), ha='center', va='bottom',
                   fontsize=10, fontweight='bold')

    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=9)
    ax.set_ylabel("Non-FIFO Decisions", fontsize=10)
    ax.set_title("Non-FIFO Decisions in Bug-Finding Run", fontsize=12, fontweight='bold', pad=12)
    ax.legend(fontsize=9)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    if g_nonfifo or l_nonfifo:
        ax.set_ylim(0, max(g + l for g, l in zip(g_nonfifo, l_nonfifo)) * 1.2 + 1)

    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "nonfifo_comparison.png"), dpi=200, bbox_inches='tight')
    plt.close(fig)
    print("  nonfifo_comparison.png")


# ── Figure 5: CHESS exploration tree (improved) ──

def fig_chess_tree(data_dir, out_dir):
    """CHESS DFS tree. Capped width, readable nodes."""
    for f in find_files(data_dir, "-tree.json"):
        tree = load_json(f)
        if not tree:
            continue

        name = f.stem.replace("-tree", "")

        children = defaultdict(list)
        for i, node in enumerate(tree):
            if node["Parent"] >= 0:
                children[node["Parent"]].append(i)

        # BFS layout
        x_pos = {}
        y_pos = {}
        next_x = [0]

        def layout(idx, depth):
            y_pos[idx] = depth
            kids = children[idx]
            if not kids:
                x_pos[idx] = next_x[0]
                next_x[0] += 1
            else:
                for kid in kids:
                    layout(kid, depth + 1)
                x_pos[idx] = np.mean([x_pos[k] for k in kids])

        roots = [i for i, n in enumerate(tree) if n["Parent"] < 0]
        for root in roots:
            layout(root, 0)

        # Cap width for readability
        max_width = 18
        n_nodes = len(tree)
        if next_x[0] > max_width:
            scale = max_width / next_x[0]
            x_pos = {k: v * scale for k, v in x_pos.items()}

        max_depth = max(y_pos.values()) if y_pos else 1
        fig_w = min(14, max(6, max_width * 0.7))
        fig_h = min(8, max(4, max_depth * 0.6 + 2))
        fig, ax = plt.subplots(figsize=(fig_w, fig_h))

        # Draw edges
        for i, node in enumerate(tree):
            if node["Parent"] >= 0 and node["Parent"] in x_pos:
                ax.plot([x_pos[node["Parent"]], x_pos[i]],
                        [y_pos[node["Parent"]], y_pos[i]],
                        color=COLORS["edge"], linewidth=0.6, zorder=1)

        # Draw nodes
        xs = [x_pos[i] for i in range(n_nodes)]
        ys = [y_pos[i] for i in range(n_nodes)]
        colors = [COLORS["failed"] if not n["Passed"] else COLORS["passed"] for n in tree]
        node_size = max(8, min(40, 800 / n_nodes))
        ax.scatter(xs, ys, c=colors, s=node_size, zorder=2,
                  edgecolors='#333', linewidth=0.3)

        # Count stats
        n_passed = sum(1 for n in tree if n["Passed"])
        n_failed = n_nodes - n_passed

        ax.set_xlabel("Exploration Frontier", fontsize=10)
        ax.set_ylabel("DFS Depth", fontsize=10)
        ax.set_title(f"CHESS Tree \u2014 {algo_label(name).replace(chr(10), ' ')} "
                     f"({n_nodes} runs, {n_failed} bugs)",
                     fontsize=11, fontweight='bold', pad=10)
        ax.invert_yaxis()
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)

        passed_patch = mpatches.Patch(color=COLORS["passed"], label=f'Passed ({n_passed})')
        failed_patch = mpatches.Patch(color=COLORS["failed"], label=f'Bug found ({n_failed})')
        ax.legend(handles=[passed_patch, failed_patch], loc='lower right', fontsize=8)

        fig.tight_layout()
        fig.savefig(os.path.join(out_dir, f"tree_{name}.png"), dpi=200, bbox_inches='tight')
        plt.close(fig)
        print(f"  tree_{name}.png")


# ── Figure 6: Per-run non-FIFO count over runs ──

def fig_nonfifo_over_runs(summaries, out_dir):
    """Line chart showing non-FIFO decision count per run for each algorithm.
    More informative than the old flat 'branch_points' chart."""
    algo_order = ["chess-gl", "chess-global", "pct-d2", "pct-d3", "random"]
    present = [p for p in algo_order if p in summaries]
    if not present:
        return

    fig, ax = plt.subplots(figsize=(10, 4))
    colors_list = [COLORS["node_A"], COLORS["node_B"], COLORS["node_C"], "#9C27B0", "#795548"]

    for i, policy in enumerate(present):
        records = summaries[policy]
        runs = [r["run_num"] for r in records]
        nf = [r.get("non_fifo", 0) for r in records]
        c = colors_list[i % len(colors_list)]
        ax.plot(runs, nf, label=algo_label(policy).replace('\n', ' '),
                linewidth=1.2, alpha=0.8, color=c)

        # Mark the bug-finding run
        bug = find_bug_run(records)
        if bug:
            ax.axvline(x=bug["run_num"], color=c, linestyle=':', alpha=0.5, linewidth=0.8)
            ax.scatter([bug["run_num"]], [bug.get("non_fifo", 0)],
                      color=c, s=60, zorder=5, edgecolors='#333', linewidth=0.5)

    ax.set_xlabel("Run", fontsize=10)
    ax.set_ylabel("Non-FIFO Decisions", fontsize=10)
    ax.set_title("Non-FIFO Decisions per Run", fontsize=12, fontweight='bold', pad=10)
    ax.legend(fontsize=8, loc='upper right')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "nonfifo_over_runs.png"), dpi=200, bbox_inches='tight')
    plt.close(fig)
    print("  nonfifo_over_runs.png")


# ── Figure 7: Theoretical vs explored search space ──

def fig_search_space_vs_explored(summaries, traces, out_dir):
    """Approximate theoretical interleavings versus runs actually explored."""
    algo_order = ["random", "pct-d2", "chess-gl", "pct-d3", "chess-global"]
    policies = []
    theoretical = []
    explored = []

    for policy in algo_order:
        metrics = bug_trace_metrics(policy, summaries, traces)
        if metrics is None:
            continue
        policies.append(policy)
        theoretical.append(search_space_upper_bound(metrics["local_sizes"], metrics["global_sizes"]))
        explored.append(metrics["explored_runs"])

    if not policies:
        return

    labels = [algo_label(p) for p in policies]
    x = np.arange(len(policies))
    width = 0.35

    fig, ax = plt.subplots(figsize=(9, 4.8))
    bars_theoretical = ax.bar(
        x - width / 2, theoretical, width,
        label="Theoretical interleavings",
        color="#D9E8F7", edgecolor="#333", linewidth=0.5
    )
    bars_explored = ax.bar(
        x + width / 2, explored, width,
        label="Runs explored",
        color=COLORS["found"], edgecolor="#333", linewidth=0.5
    )

    for bar, val in zip(bars_theoretical, theoretical):
        ax.text(bar.get_x() + bar.get_width() / 2, val * 1.12, human_large_int(val),
                ha='center', va='bottom', fontsize=8, rotation=90, color="#555")
    for bar, val in zip(bars_explored, explored):
        ax.text(bar.get_x() + bar.get_width() / 2, val * 1.12, str(val),
                ha='center', va='bottom', fontsize=9, fontweight='bold')

    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=9)
    ax.set_yscale("log")
    ax.set_ylabel("Count (log scale)", fontsize=10)
    ax.set_title("Search Space: Theoretical vs Explored", fontsize=12, fontweight='bold', pad=12)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.legend(fontsize=9)

    # Clarify that this is an upper bound, not an exact trace count.
    ax.text(0.01, 0.01,
            "Upper bound from per-step local runq sizes and global queue sizes in the bug-finding trace.",
            transform=ax.transAxes, fontsize=8, color="#666", va='bottom')

    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "search_space_vs_explored.png"), dpi=200, bbox_inches='tight')
    plt.close(fig)
    print("  search_space_vs_explored.png")


# ── Figure 8: Local vs global decisions in bug trace ──

def fig_bug_trace_decision_mix(summaries, traces, out_dir):
    """Break down the bug-finding interleaving into local and global decisions."""
    algo_order = ["random", "pct-d2", "chess-gl", "pct-d3", "chess-global"]
    policies = []
    local_counts = []
    global_counts = []

    for policy in algo_order:
        metrics = bug_trace_metrics(policy, summaries, traces)
        if metrics is None:
            continue
        policies.append(policy)
        local_counts.append(len(metrics["local_steps"]))
        global_counts.append(len(metrics["global_steps"]))

    if not policies:
        return

    labels = [algo_label(p) for p in policies]
    x = np.arange(len(policies))

    fig, ax = plt.subplots(figsize=(8.5, 4.5))
    bars_local = ax.bar(
        x, local_counts, width=0.55,
        label="Local decisions",
        color=COLORS["local_fifo"], edgecolor="#333", linewidth=0.5
    )
    bars_global = ax.bar(
        x, global_counts, width=0.55, bottom=local_counts,
        label="Global decisions",
        color=COLORS["global_fifo"], edgecolor="#333", linewidth=0.5
    )

    for i, (local_n, global_n) in enumerate(zip(local_counts, global_counts)):
        total = local_n + global_n
        ax.text(i, total + 0.7, str(total), ha='center', va='bottom',
                fontsize=10, fontweight='bold')
        ax.text(i, local_n / 2, str(local_n), ha='center', va='center',
                fontsize=8, color='#333')
        ax.text(i, local_n + global_n / 2, str(global_n), ha='center', va='center',
                fontsize=8, color='white' if global_n >= 3 else '#333',
                fontweight='bold' if global_n >= 3 else 'normal')

    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=9)
    ax.set_ylabel("Decisions", fontsize=10)
    ax.set_title("Bug-Finding Interleaving: Local vs Global Decisions",
                 fontsize=12, fontweight='bold', pad=12)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.legend(fontsize=9)
    ax.set_ylim(0, max(l + g for l, g in zip(local_counts, global_counts)) * 1.18)

    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "bug_trace_decision_mix.png"), dpi=200, bbox_inches='tight')
    plt.close(fig)
    print("  bug_trace_decision_mix.png")


# ── Figure 9: Detailed trace with RPC labels (fixed) ──

def fig_detailed_trace(traces, out_dir):
    """Per-node timeline with proper label placement. No overlap with title."""
    for policy, records in traces.items():
        bug_run = find_bug_run(records)
        if bug_run is None:
            continue
        steps = bug_run.get("steps", [])
        if len(steps) < 2:
            continue

        nodes = sorted(set(s.get("node", "") for s in steps
                          if s["kind"] == "local" and s.get("node")))
        if not nodes:
            continue

        y_map = {}
        for i, n in enumerate(nodes):
            y_map[n] = i
        global_y = len(nodes)

        fig_width = max(10, min(20, len(steps) * 0.35))
        fig_height = max(3.5, (len(nodes) + 1) * 1.0 + 1.5)
        fig, ax = plt.subplots(figsize=(fig_width, fig_height))

        # Alternate label positions to avoid overlap
        label_above = True

        for i, s in enumerate(steps):
            alts = s["alternatives"]
            idx = s["index"]
            is_nonfifo = idx != 0

            if s["kind"] == "global":
                y = global_y
                if is_nonfifo:
                    color = COLORS["non_fifo"]
                else:
                    color = COLORS["global_fifo"]
                size = max(25, alts * 16)
            else:
                node = s.get("node", "")
                y = y_map.get(node, 0)
                if is_nonfifo:
                    color = COLORS["non_fifo"]
                else:
                    color = COLORS["local_fifo"]
                size = max(12, alts * 10)

            ax.scatter(i, y, c=color, s=size, alpha=0.8, edgecolors='#333',
                      linewidth=0.3, zorder=3)

            # Only label non-FIFO or multi-alt global decisions
            if s["kind"] == "global" and (is_nonfifo or alts > 1):
                mt = s.get("msg_type", "")[:3]
                fr = s.get("from", "?")
                to = s.get("to", "?")
                label = f"{fr}\u2192{to} {mt}"

                offset = 12 if label_above else -12
                va = 'bottom' if label_above else 'top'
                ax.annotate(label, (i, y), textcoords="offset points",
                           xytext=(0, offset), ha='center', fontsize=5.5,
                           fontweight='bold' if is_nonfifo else 'normal',
                           color=color, va=va)
                label_above = not label_above
            elif s["kind"] == "local" and is_nonfifo and alts > 1:
                bgid = s.get("chosen_bgid", "?")
                ax.annotate(f"B{bgid}", (i, y), textcoords="offset points",
                           xytext=(0, -10), ha='center', fontsize=5.5,
                           color=COLORS["non_fifo"], fontweight='bold')

        yticks = list(range(len(nodes))) + [global_y]
        yticklabels = [f"Node {n}" for n in nodes] + ["Global"]
        ax.set_yticks(yticks)
        ax.set_yticklabels(yticklabels, fontsize=9)
        ax.set_xlabel("Decision Step", fontsize=10)

        run_num = bug_run.get("run_num", "?")
        ax.set_title(f"Detailed Trace \u2014 {algo_label(policy).replace(chr(10), ' ')} "
                     f"(run {run_num}, {len(steps)} decisions)",
                     fontsize=11, fontweight='bold', pad=12)
        ax.set_ylim(-0.5, global_y + 0.8)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)

        legend_elements = [
            Line2D([0], [0], marker='o', color='w', markerfacecolor=COLORS["global_fifo"],
                   markersize=7, label='Global (FIFO)'),
            Line2D([0], [0], marker='o', color='w', markerfacecolor=COLORS["local_fifo"],
                   markersize=7, label='Local (FIFO)'),
            Line2D([0], [0], marker='o', color='w', markerfacecolor=COLORS["non_fifo"],
                   markersize=7, label='Non-FIFO'),
        ]
        ax.legend(handles=legend_elements, loc='upper right', fontsize=7,
                 framealpha=0.9)

        fig.tight_layout()
        fig.savefig(os.path.join(out_dir, f"detailed_{policy}.png"), dpi=200,
                   bbox_inches='tight')
        plt.close(fig)
        print(f"  detailed_{policy}.png")


# ── Figure 10: Narrative trace (text) ──

def fig_narrative_trace(traces, out_dir):
    """Human-readable narrative of each algorithm's bug-finding run."""
    for policy, records in traces.items():
        bug_run = find_bug_run(records)
        if bug_run is None:
            continue

        steps = bug_run.get("steps", [])
        outpath = os.path.join(out_dir, f"narrative_{policy}.txt")

        with open(outpath, "w") as out:
            out.write(f"{'=' * 70}\n")
            out.write(f"Bug-Finding Trace: {policy}, Run {bug_run['run_num']}\n")
            out.write(f"Total decisions: {len(steps)}\n")
            out.write(f"{'=' * 70}\n\n")

            global_step = 0
            for i, s in enumerate(steps):
                alts = s["alternatives"]
                idx = s["index"]
                nonfifo = " ** NON-FIFO **" if idx != 0 else ""

                if s["kind"] == "global":
                    global_step += 1
                    fr = s.get("from", "?")
                    to = s.get("to", "?")
                    mt = s.get("msg_type", "?")
                    out.write(f"Step {i:3d}: [GLOBAL #{global_step}]  "
                              f"Deliver {fr}\u2192{to}({mt})  "
                              f"[chose {idx} of {alts}]{nonfifo}\n")
                else:
                    node = s.get("node", "?")
                    bgid = s.get("chosen_bgid", 0)
                    runq = s.get("runq_bgids", [])
                    runq_str = ", ".join(f"B{b}" for b in runq) if runq else "?"
                    out.write(f"Step {i:3d}: [LOCAL  {node}]     "
                              f"Run B{bgid}  "
                              f"from [{runq_str}]  "
                              f"[chose {idx} of {alts}]{nonfifo}\n")

            out.write(f"\n{'=' * 70}\n")
            out.write(f"RESULT: BUG FOUND on run {bug_run['run_num']}\n")
            out.write(f"  Global decisions: {global_step}\n")
            out.write(f"  Local decisions:  {len(steps) - global_step}\n")
            non_fifo = sum(1 for s in steps if s["index"] != 0)
            out.write(f"  Non-FIFO choices: {non_fifo}\n")

            if non_fifo > 0:
                out.write(f"\nKey non-FIFO decisions:\n")
                for i, s in enumerate(steps):
                    if s["index"] == 0:
                        continue
                    if s["kind"] == "global":
                        fr = s.get("from", "?")
                        to = s.get("to", "?")
                        mt = s.get("msg_type", "?")
                        out.write(f"  Step {i}: GLOBAL  Deliver {fr}\u2192{to}({mt})  "
                                  f"[index {s['index']} of {s['alternatives']}]\n")
                    else:
                        node = s.get("node", "?")
                        bgid = s.get("chosen_bgid", 0)
                        runq = s.get("runq_bgids", [])
                        runq_str = ", ".join(f"B{b}" for b in runq)
                        out.write(f"  Step {i}: LOCAL   node={node}  "
                                  f"B{bgid} from [{runq_str}]  "
                                  f"[index {s['index']} of {s['alternatives']}]\n")

            out.write(f"{'=' * 70}\n")

        print(f"  narrative_{policy}.txt")


# ── Figure 11: Algorithm comparison summary table ──

def fig_summary_table(summaries, traces, out_dir):
    """Table comparing algorithms on key metrics."""
    algo_order = ["random", "pct-d2", "chess-gl", "pct-d3", "chess-global"]
    present = [p for p in algo_order if p in summaries]
    if not present:
        return

    columns = ["Algorithm", "Runs", "Bug\nFound?", "Bug\nRun #", "Avg\nNon-FIFO",
               "Avg Total\nDecisions"]
    rows = []
    for policy in present:
        records = summaries[policy]
        bug = find_bug_run(records)
        avg_nf = np.mean([r.get("non_fifo", 0) for r in records])
        avg_td = np.mean([r.get("total_decisions", 0) for r in records])
        rows.append([
            algo_label(policy).replace('\n', ' '),
            str(len(records)),
            "Yes" if bug else "No",
            str(bug["run_num"]) if bug else "-",
            f"{avg_nf:.1f}",
            f"{avg_td:.1f}",
        ])

    fig, ax = plt.subplots(figsize=(9, 1.2 + len(rows) * 0.5))
    ax.set_axis_off()

    table = ax.table(cellText=rows, colLabels=columns, loc='center',
                     cellLoc='center', colColours=['#E8E8E8'] * len(columns))
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1, 1.6)

    # Color the "Bug Found?" column
    for i, row in enumerate(rows):
        cell = table[i + 1, 2]  # +1 for header
        if row[2] == "Yes":
            cell.set_facecolor('#E8F5E9')
        else:
            cell.set_facecolor('#FFEBEE')

    ax.set_title("Algorithm Comparison Summary", fontsize=12, fontweight='bold', pad=20)

    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "summary_table.png"), dpi=200, bbox_inches='tight')
    plt.close(fig)
    print("  summary_table.png")


# ── Figure 12: Cumulative unique traces over runs ──

def fig_cumulative_unique_traces(summaries, out_dir):
    """Show how quickly each algorithm accumulates distinct delivery traces."""
    algo_order = ["chess-gl", "chess-global", "pct-d2", "pct-d3", "random"]
    present = [p for p in algo_order if p in summaries]
    if not present:
        return

    fig, ax = plt.subplots(figsize=(10, 4.8))
    colors_list = [COLORS["node_A"], COLORS["node_B"], COLORS["node_C"], "#9C27B0", "#795548"]

    max_runs = 0
    max_unique = 0
    for i, policy in enumerate(present):
        records = sorted(summaries[policy], key=lambda r: r["run_num"])
        seen = set()
        runs = []
        cumulative_unique = []
        for record in records:
            fingerprint = record.get("trace_fingerprint")
            if fingerprint:
                seen.add(fingerprint)
            runs.append(record["run_num"])
            cumulative_unique.append(len(seen))

        if not runs:
            continue
        max_runs = max(max_runs, runs[-1])
        max_unique = max(max_unique, cumulative_unique[-1])

        c = colors_list[i % len(colors_list)]
        ax.plot(
            runs,
            cumulative_unique,
            label=algo_label(policy).replace('\n', ' '),
            linewidth=1.8,
            color=c,
        )

        bug = find_bug_run(records)
        if bug:
            bug_idx = next((j for j, r in enumerate(records) if r["run_num"] == bug["run_num"]), None)
            if bug_idx is not None:
                ax.scatter(
                    [runs[bug_idx]],
                    [cumulative_unique[bug_idx]],
                    color=c,
                    s=55,
                    zorder=5,
                    edgecolors='#333',
                    linewidth=0.5,
                )

    if max_runs == 0:
        plt.close(fig)
        return

    ax.plot([1, max_runs], [1, max_runs], linestyle='--', linewidth=1,
            color="#BDBDBD", label="Ideal no-repetition")
    ax.set_xlim(1, max_runs)
    ax.set_ylim(0, max(max_unique, 1) * 1.08)
    ax.set_xlabel("Run", fontsize=10)
    ax.set_ylabel("Cumulative unique delivery traces", fontsize=10)
    ax.set_title("Exploration Diversity Over Runs", fontsize=12, fontweight='bold', pad=10)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.legend(fontsize=8, loc='lower right')

    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "cumulative_unique_traces.png"), dpi=200, bbox_inches='tight')
    plt.close(fig)
    print("  cumulative_unique_traces.png")


# ── Figure 13: Queue pressure in the bug-finding run ──

def fig_queue_pressure_profile(summaries, out_dir):
    """Compare how much global delivery choice remains over the failing run."""
    algo_order = ["random", "pct-d2", "chess-gl", "pct-d3", "chess-global"]
    series = []

    for policy in algo_order:
        records = summaries.get(policy, [])
        bug = find_bug_run(records)
        if bug is None:
            continue
        queue_sizes = list(bug.get("queue_sizes") or [])
        if not queue_sizes:
            continue
        series.append((policy, queue_sizes))

    if not series:
        return

    fig, ax = plt.subplots(figsize=(9, 4.8))
    colors_list = [COLORS["node_A"], COLORS["node_B"], COLORS["node_C"], "#9C27B0", "#795548"]

    for i, (policy, queue_sizes) in enumerate(series):
        x = np.arange(1, len(queue_sizes) + 1)
        y = np.array(queue_sizes)
        c = colors_list[i % len(colors_list)]
        ax.plot(x, y, linewidth=1.8, color=c, label=algo_label(policy).replace('\n', ' '))
        ax.scatter(x, y, s=20, color=c, edgecolors='#333', linewidth=0.3, zorder=4)

    ax.set_xlabel("Global decision index in failing run", fontsize=10)
    ax.set_ylabel("Runnable messages in global queue", fontsize=10)
    ax.set_title("Queue Pressure Profile of the Failing Interleaving",
                 fontsize=12, fontweight='bold', pad=10)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.legend(fontsize=8, loc='upper right')

    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "queue_pressure_profile.png"), dpi=200, bbox_inches='tight')
    plt.close(fig)
    print("  queue_pressure_profile.png")


# ── Figure 14: Per-node local decision burden ──

def fig_local_decision_burden(summaries, out_dir):
    """Heatmap of where local scheduling choices concentrate across nodes."""
    algo_order = ["random", "pct-d2", "chess-gl", "pct-d3", "chess-global"]
    rows = []
    policies = []
    all_nodes = sorted({
        node
        for records in summaries.values()
        for record in records
        for node in (record.get("local_decision_by_node") or {}).keys()
    })
    if not all_nodes:
        return

    for policy in algo_order:
        records = summaries.get(policy, [])
        bug = find_bug_run(records)
        if bug is None:
            continue
        counts = bug.get("local_decision_by_node") or {}
        rows.append([counts.get(node, 0) for node in all_nodes])
        policies.append(policy)

    if not rows:
        return

    matrix = np.array(rows, dtype=float)
    fig_w = max(6.5, len(all_nodes) * 1.2 + 2.5)
    fig_h = max(3.5, len(policies) * 0.7 + 2.0)
    fig, ax = plt.subplots(figsize=(fig_w, fig_h))
    im = ax.imshow(matrix, cmap="Blues", aspect="auto")

    ax.set_xticks(np.arange(len(all_nodes)))
    ax.set_xticklabels([f"Node {node}" for node in all_nodes], fontsize=9)
    ax.set_yticks(np.arange(len(policies)))
    ax.set_yticklabels([algo_label(policy).replace('\n', ' ') for policy in policies], fontsize=9)
    ax.set_title("Local Scheduling Burden by Node in the Failing Run",
                 fontsize=12, fontweight='bold', pad=10)

    for i in range(matrix.shape[0]):
        for j in range(matrix.shape[1]):
            value = int(matrix[i, j])
            text_color = "white" if matrix[i, j] > matrix.max() * 0.55 else "#222"
            ax.text(j, i, str(value), ha='center', va='center', fontsize=9, color=text_color)

    cbar = fig.colorbar(im, ax=ax, shrink=0.9)
    cbar.set_label("Local decisions", fontsize=9)

    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "local_decision_burden.png"), dpi=200, bbox_inches='tight')
    plt.close(fig)
    print("  local_decision_burden.png")


# ── Figure 15: Non-FIFO positions inside the failing trace ──

def fig_nonfifo_position_profile(traces, out_dir):
    """Show where the divergence from FIFO happens within each failing trace."""
    algo_order = ["random", "pct-d2", "chess-gl", "pct-d3", "chess-global"]
    rows = []

    for policy in algo_order:
        records = traces.get(policy, [])
        bug = find_bug_run(records)
        if bug is None:
            continue
        steps = bug.get("steps", [])
        if not steps:
            continue

        total_steps = len(steps)
        local_positions = [
            (idx + 1) / total_steps
            for idx, step in enumerate(steps)
            if step["kind"] == "local" and step["index"] != 0
        ]
        global_positions = [
            (idx + 1) / total_steps
            for idx, step in enumerate(steps)
            if step["kind"] == "global" and step["index"] != 0
        ]
        rows.append((policy, local_positions, global_positions, total_steps))

    if not rows:
        return

    fig_h = max(3.8, len(rows) * 0.75 + 1.6)
    fig, ax = plt.subplots(figsize=(10, fig_h))

    local_labeled = False
    global_labeled = False
    for y, (policy, local_positions, global_positions, total_steps) in enumerate(rows):
        ax.hlines(y, 0, 1, color="#E6E6E6", linewidth=1, zorder=0)
        if local_positions:
            ax.scatter(
                local_positions,
                [y] * len(local_positions),
                marker='o',
                s=46,
                color=COLORS["local_fifo"],
                edgecolors="#333",
                linewidth=0.4,
                label="Local non-FIFO" if not local_labeled else None,
                zorder=3,
            )
            local_labeled = True
        if global_positions:
            ax.scatter(
                global_positions,
                [y] * len(global_positions),
                marker='D',
                s=42,
                color=COLORS["global_fifo"],
                edgecolors="#333",
                linewidth=0.4,
                label="Global non-FIFO" if not global_labeled else None,
                zorder=4,
            )
            global_labeled = True
        ax.text(1.01, y, f"{total_steps} steps", va='center', ha='left', fontsize=8, color="#666")

    ax.set_yticks(np.arange(len(rows)))
    ax.set_yticklabels([algo_label(policy).replace('\n', ' ') for policy, _, _, _ in rows], fontsize=9)
    ax.set_xlim(0, 1.08)
    ax.set_xticks(np.linspace(0, 1, 6))
    ax.set_xticklabels([f"{int(x * 100)}%" for x in np.linspace(0, 1, 6)], fontsize=9)
    ax.set_xlabel("Position within failing trace", fontsize=10)
    ax.set_title("Where Non-FIFO Decisions Occur in the Failing Trace",
                 fontsize=12, fontweight='bold', pad=10)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    if local_labeled or global_labeled:
        ax.legend(fontsize=8, loc='upper right')

    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "nonfifo_position_profile.png"), dpi=200, bbox_inches='tight')
    plt.close(fig)
    print("  nonfifo_position_profile.png")


# ── Main ──

def main():
    parser = argparse.ArgumentParser(description="Synctest exploration charts")
    parser.add_argument("--data", default="charts/data", help="Input data directory")
    parser.add_argument("--out", default="charts/figures", help="Output figures directory")
    args = parser.parse_args()

    os.makedirs(args.out, exist_ok=True)
    print(f"Reading from {args.data}, writing to {args.out}\n")

    # Load all data upfront
    summaries = load_all_summaries(args.data)
    traces = load_all_traces(args.data)

    print("Figure 1: Runs to first bug")
    fig_runs_to_bug(summaries, args.out)

    print("\nFigure 2: Bug-finding swimlane")
    fig_swimlane(traces, args.out)

    print("\nFigure 3: Message sequence diagram")
    fig_sequence_diagram(traces, args.out)

    print("\nFigure 4: Non-FIFO decision comparison")
    fig_nonfifo_from_traces(traces, summaries, args.out)

    print("\nFigure 5: CHESS tree")
    fig_chess_tree(args.data, args.out)

    print("\nFigure 6: Non-FIFO over runs")
    fig_nonfifo_over_runs(summaries, args.out)

    print("\nFigure 7: Search space vs explored")
    fig_search_space_vs_explored(summaries, traces, args.out)

    print("\nFigure 8: Bug trace decision mix")
    fig_bug_trace_decision_mix(summaries, traces, args.out)

    print("\nFigure 9: Detailed trace")
    fig_detailed_trace(traces, args.out)

    print("\nFigure 10: Narrative trace (text)")
    fig_narrative_trace(traces, args.out)

    print("\nFigure 11: Summary table")
    fig_summary_table(summaries, traces, args.out)

    print("\nFigure 12: Cumulative unique traces")
    fig_cumulative_unique_traces(summaries, args.out)

    print("\nFigure 13: Queue pressure profile")
    fig_queue_pressure_profile(summaries, args.out)

    print("\nFigure 14: Local decision burden")
    fig_local_decision_burden(summaries, args.out)

    print("\nFigure 15: Non-FIFO position profile")
    fig_nonfifo_position_profile(traces, args.out)

    print(f"\nDone. Figures in {args.out}/")


if __name__ == "__main__":
    main()
