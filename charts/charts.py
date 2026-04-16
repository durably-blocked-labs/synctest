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

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import numpy as np


# ── Style ──

COLORS = {
    "global_fifo": "#4A90D9",
    "local_fifo": "#F5A623",
    "non_fifo": "#D0021B",
    "found": "#4A90D9",
    "partial_found": "#F5A623",
    "not_found": "#BDBDBD",
    "passed": "#7ED321",
    "failed": "#D0021B",
    "node_A": "#4A90D9",
    "node_B": "#F5A623",
    "node_C": "#7ED321",
    "edge": "#CCCCCC",
    "mean": "#222222",
    "range_fill": "#D9E8F7",
}

ALGO_LABELS = {
    "targeted": "Targeted\n(trace)",
    "chess-gl": "CHESS\n(G+L, k=4)",
    "chess-global": "CHESS\n(G-only, k=4)",
    "pct-d2": "PCT\n(d=2)",
    "pct-d3": "PCT\n(d=3)",
    "random": "Random",
}

ALGO_ORDER = ["targeted", "random", "pct-d2", "chess-gl", "pct-d3", "chess-global"]
LINE_COLORS = [COLORS["node_A"], COLORS["node_B"], COLORS["node_C"], "#9C27B0", "#795548"]


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


def infer_attempt_from_path(path):
    for parent in [path.parent, *path.parents]:
        name = parent.name
        if name.startswith("attempt-"):
            suffix = name[len("attempt-"):]
            try:
                return int(suffix)
            except ValueError:
                return 1
    return 1


def load_all_summaries(data_dir):
    """Load all summary JSONL files, grouped by policy and attempt."""
    summaries = defaultdict(dict)
    for f in sorted(Path(data_dir).rglob("*.jsonl")):
        if "-trace" in f.name:
            continue
        records = load_jsonl(f)
        if not records:
            continue
        attempt = infer_attempt_from_path(f)
        policy = records[0].get("policy", f.stem)
        for record in records:
            record.setdefault("attempt", attempt)
        summaries[policy][attempt] = sorted(records, key=lambda r: r.get("run_num", 0))
    return {policy: dict(attempts) for policy, attempts in summaries.items()}


def load_all_traces(data_dir):
    """Load all detailed trace JSONL files, grouped by policy and attempt."""
    traces = defaultdict(dict)
    for f in sorted(Path(data_dir).rglob("*-trace.jsonl")):
        records = load_jsonl(f)
        if not records:
            continue
        attempt = infer_attempt_from_path(f)
        policy = records[0].get("policy", f.stem.replace("-trace", ""))
        for record in records:
            record.setdefault("attempt", attempt)
        traces[policy][attempt] = sorted(records, key=lambda r: r.get("run_num", 0))
    return {policy: dict(attempts) for policy, attempts in traces.items()}


def load_tree_files(data_dir):
    """Load tree JSON file paths, grouped by policy and attempt."""
    trees = defaultdict(dict)
    for f in sorted(Path(data_dir).rglob("*-tree.json")):
        attempt = infer_attempt_from_path(f)
        policy = f.stem.replace("-tree", "")
        trees[policy][attempt] = f
    return {policy: dict(attempts) for policy, attempts in trees.items()}


_TREE_JSON_CACHE = {}


def policies_in_order(*datasets):
    policies = set()
    for dataset in datasets:
        policies.update(dataset.keys())
    ordered = [policy for policy in ALGO_ORDER if policy in policies]
    extras = sorted(policy for policy in policies if policy not in ALGO_ORDER)
    return ordered + extras


def has_multi_attempt_data(*datasets):
    attempts = set()
    for dataset in datasets:
        for per_attempt in dataset.values():
            attempts.update(per_attempt.keys())
    return len(attempts) > 1 or any(attempt != 1 for attempt in attempts)


def find_bug_run(records):
    """Find the first run with a real user-asserted failure (not just deadlock)."""
    has_user_failed_field = any("user_failed" in r for r in records)
    for record in records:
        if has_user_failed_field:
            if record.get("user_failed", False):
                return record
        elif not record.get("passed", True):
            return record
    return None


def is_control_step(step):
    return step.get("kind") == "global" and step.get("msg_type") == "Control"


def measured_steps(steps):
    return [step for step in steps if not is_control_step(step)]


def measured_non_fifo(steps):
    return sum(1 for step in measured_steps(steps) if step.get("index", 0) != 0)


def meaningful_local_decision(step):
    runq = step.get("runq_bgids", [])
    if runq:
        return sum(1 for bgid in runq if bgid != 0) > 1
    return step.get("alternatives", 0) > 1


def measured_global_decisions(steps):
    return sum(1 for step in measured_steps(steps) if step.get("kind") == "global")


def measured_local_decisions(steps):
    return sum(
        1
        for step in measured_steps(steps)
        if step.get("kind") == "local" and meaningful_local_decision(step)
    )


def measured_branch_points(steps):
    return sum(1 for step in measured_steps(steps) if step.get("alternatives", 0) > 1)


def measured_queue_sizes(steps):
    return [
        step.get("alternatives", 0)
        for step in measured_steps(steps)
        if step.get("kind") == "global"
    ]


def measured_trace_fingerprint(steps):
    parts = []
    for step in measured_steps(steps):
        if step.get("kind") != "global":
            continue
        parts.append(f"{step.get('from', '')}->{step.get('to', '')}({step.get('msg_type', '')})")
    return " ".join(parts)


def measured_local_by_node(steps):
    counts = defaultdict(int)
    for step in measured_steps(steps):
        if step.get("kind") == "local" and meaningful_local_decision(step):
            counts[step.get("node", "")] += 1
    return dict(counts)


def apply_chart_adjustments(summaries, traces):
    """Attach chart-only metrics with Control traffic excluded.

    Raw summary and detailed trace records stay unchanged. Charts should use the
    chart_* fields when present and fall back to raw fields for older data that
    lacks detailed traces.
    """
    for policy, attempts in summaries.items():
        for attempt, records in attempts.items():
            trace_records = traces.get(policy, {}).get(attempt, [])
            by_run = {record.get("run_num"): record for record in trace_records}
            for record in records:
                trace = by_run.get(record.get("run_num"))
                if not trace:
                    continue
                steps = trace.get("steps") or []
                if not steps:
                    continue
                local_total = measured_local_decisions(steps)
                global_total = measured_global_decisions(steps)
                record["chart_non_fifo"] = measured_non_fifo(steps)
                record["chart_global_decision_count"] = global_total
                record["chart_local_decision_total"] = local_total
                record["chart_total_decisions"] = global_total + local_total
                record["chart_branch_points"] = measured_branch_points(steps)
                record["chart_queue_sizes"] = measured_queue_sizes(steps)
                record["chart_trace_fingerprint"] = measured_trace_fingerprint(steps)
                record["chart_local_decision_by_node"] = measured_local_by_node(steps)


def metric_value(record, key, default=0):
    return record.get(f"chart_{key}", record.get(key, default))


def effective_runs_to_bug(records):
    bug = find_bug_run(records)
    if bug:
        return bug["run_num"]
    return len(records)


def found_rate_text(found, total):
    return f"found {found}/{total}"


def attempt_series(policy_attempts):
    return sorted(policy_attempts.items())


def representative_attempt(policy, summaries, traces):
    candidates = []
    for attempt, records in attempt_series(traces.get(policy, {})):
        if find_bug_run(records) is None:
            continue
        summary_records = summaries.get(policy, {}).get(attempt, [])
        effective_runs = effective_runs_to_bug(summary_records) if summary_records else effective_runs_to_bug(records)
        candidates.append((attempt, effective_runs))
    if not candidates:
        return None
    target = float(np.median([effective_runs for _, effective_runs in candidates]))
    return min(candidates, key=lambda item: (abs(item[1] - target), item[0]))[0]


def representative_views(summaries, traces, trees):
    rep_summaries = {}
    rep_traces = {}
    rep_trees = {}
    rep_attempts = {}
    for policy in policies_in_order(summaries, traces, trees):
        attempt = representative_attempt(policy, summaries, traces)
        if attempt is None:
            continue
        if attempt in summaries.get(policy, {}):
            rep_summaries[policy] = summaries[policy][attempt]
        if attempt in traces.get(policy, {}):
            rep_traces[policy] = traces[policy][attempt]
        if attempt in trees.get(policy, {}):
            rep_trees[policy] = trees[policy][attempt]
        rep_attempts[policy] = attempt
    return rep_summaries, rep_traces, rep_trees, rep_attempts


def load_tree(path):
    key = str(path)
    if key not in _TREE_JSON_CACHE:
        _TREE_JSON_CACHE[key] = load_json(path)
    return _TREE_JSON_CACHE[key]


def title_attempt_suffix(policy, rep_attempts, multi_attempt):
    if not multi_attempt:
        return ""
    attempt = rep_attempts.get(policy)
    if attempt is None:
        return ""
    return f", attempt {attempt}"


def bug_trace_metrics(policy, summaries, traces):
    """Return paired summary/trace metrics for a representative bug-finding run."""
    summary_bug = find_bug_run(summaries.get(policy, []))
    trace_bug = find_bug_run(traces.get(policy, []))
    if trace_bug is None:
        return None

    steps = trace_bug.get("steps", [])
    measured = measured_steps(steps)
    local_steps = [step for step in steps if step["kind"] == "local"]
    global_steps = [step for step in measured if step["kind"] == "global"]
    local_sizes = [len(step.get("runq_bgids", [])) or step.get("alternatives", 0) for step in local_steps]
    if summary_bug and metric_value(summary_bug, "queue_sizes", []):
        global_sizes = list(metric_value(summary_bug, "queue_sizes", []))
    else:
        global_sizes = [step.get("alternatives", 0) for step in global_steps]

    return {
        "summary_bug": summary_bug,
        "trace_bug": trace_bug,
        "steps": measured,
        "local_steps": local_steps,
        "global_steps": global_steps,
        "local_sizes": local_sizes,
        "global_sizes": global_sizes,
        "explored_runs": effective_runs_to_bug(summaries.get(policy, [])),
    }


def found_bug_attempts(policy, summaries, traces):
    attempts = sorted(set(summaries.get(policy, {}).keys()) | set(traces.get(policy, {}).keys()))
    found = []
    for attempt in attempts:
        summary_records = summaries.get(policy, {}).get(attempt, [])
        trace_records = traces.get(policy, {}).get(attempt, [])
        if find_bug_run(summary_records) is not None or find_bug_run(trace_records) is not None:
            found.append(attempt)
    return found


def has_truncated_bug_trace(policy, attempt, run_num, trees):
    tree_path = trees.get(policy, {}).get(attempt)
    if tree_path is None or run_num is None:
        return False

    tree = load_tree(tree_path)
    if not isinstance(tree, list) or run_num < 1 or run_num > len(tree):
        return False

    node = tree[run_num - 1]
    branch_step = node.get("BranchStep")
    parent = node.get("Parent")
    trace_len = node.get("TraceLen")
    if parent in (None, -1) or branch_step is None or trace_len is None:
        return False
    return trace_len < branch_step + 1


def successful_bug_trace_counts(policy, summaries, traces, trees):
    counts = []
    attempts = sorted(set(summaries.get(policy, {}).keys()) | set(traces.get(policy, {}).keys()))
    for attempt in attempts:
        summary_records = summaries.get(policy, {}).get(attempt, [])
        trace_records = traces.get(policy, {}).get(attempt, [])
        summary_bug = find_bug_run(summary_records)
        trace_bug = find_bug_run(trace_records)
        bug_record = trace_bug or summary_bug

        if bug_record is not None and has_truncated_bug_trace(policy, attempt, bug_record.get("run_num"), trees):
            continue

        if trace_bug is not None and trace_bug.get("steps"):
            steps = trace_bug.get("steps", [])
            counts.append(
                {
                    "attempt": attempt,
                    "local": measured_local_decisions(steps),
                    "global": measured_global_decisions(steps),
                }
            )
            continue

        if summary_bug is None:
            continue

        counts.append(
            {
                "attempt": attempt,
                "local": metric_value(summary_bug, "local_decision_total", 0),
                "global": metric_value(summary_bug, "global_decision_count", 0),
            }
        )
    return counts


def aggregate_series(values_by_attempt):
    max_len = max((len(values) for values in values_by_attempt), default=0)
    if max_len == 0:
        return None

    runs = []
    means = []
    mins = []
    maxs = []
    for idx in range(max_len):
        bucket = [values[idx] for values in values_by_attempt if idx < len(values)]
        if not bucket:
            continue
        runs.append(idx + 1)
        means.append(float(np.mean(bucket)))
        mins.append(float(np.min(bucket)))
        maxs.append(float(np.max(bucket)))
    return {
        "runs": np.array(runs),
        "mean": np.array(means),
        "min": np.array(mins),
        "max": np.array(maxs),
    }


def search_space_upper_bound(local_sizes, global_sizes):
    total = 1
    for size in local_sizes + global_sizes:
        total *= max(1, size)
    return total


def human_large_int(n):
    if n < 1000:
        return str(n)

    digits = str(n)
    exp = len(digits) - 1
    whole = int(digits[0])
    tenth = int(digits[1]) if len(digits) > 1 else 0
    hundredth = int(digits[2]) if len(digits) > 2 else 0

    if hundredth >= 5:
        tenth += 1
        if tenth == 10:
            whole += 1
            tenth = 0
            if whole == 10:
                whole = 1
                exp += 1

    return f"{whole}.{tenth}e{exp}"


# ── Figure 1: Runs to first bug ──

def fig_runs_to_bug(summaries, out_dir):
    """Bar chart of average successful runs-to-bug across attempts."""
    policies = [policy for policy in policies_in_order(summaries) if summaries.get(policy)]
    if not policies:
        return

    labels = []
    bar_values = []
    bar_colors = []
    found_rates = []
    max_run_value = 0

    for policy in policies:
        attempts = attempt_series(summaries[policy])
        successful_values = [
            bug["run_num"]
            for _, records in attempts
            for bug in [find_bug_run(records)]
            if bug is not None
        ]
        found = len(successful_values)
        total = len(attempts)

        labels.append(algo_label(policy))
        found_rates.append(found_rate_text(found, total))
        if successful_values:
            avg_value = float(np.mean(successful_values))
            bar_values.append(avg_value)
            if found == total:
                bar_colors.append(COLORS["found"])
            else:
                bar_colors.append(COLORS["partial_found"])
            max_run_value = max(max_run_value, avg_value)
        else:
            bar_values.append(0.0)
            bar_colors.append(COLORS["not_found"])

    fig, ax = plt.subplots(figsize=(9, 4.8))
    x = np.arange(len(policies))
    bars = ax.bar(x, bar_values, color=bar_colors, edgecolor="#333", linewidth=0.5, width=0.6)

    for idx, (bar, value, rate) in enumerate(zip(bars, bar_values, found_rates)):
        if value > 0:
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + max(0.5, 0.02 * max_run_value),
                f"{value:.1f}",
                ha="center",
                va="bottom",
                fontsize=10,
                fontweight="bold",
            )
            rate_y = bar.get_height() + max(3, 0.08 * max_run_value)
        else:
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                max(0.6, 0.04 * max(max_run_value, 1)),
                "not\nfound",
                ha="center",
                va="bottom",
                fontsize=8,
                fontweight="bold",
                color="#888",
            )
            rate_y = max(2.2, 0.12 * max(max_run_value, 1))
        ax.text(x[idx], rate_y, rate, ha="center", va="bottom", fontsize=8, color="#555")

    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=9)
    ax.set_ylabel("Average Runs to Bug", fontsize=11)
    ax.set_title("Average Successful Runs to First Bug Across Attempts", fontsize=13, fontweight="bold", pad=15)
    ax.set_ylim(0, max_run_value * 1.25 + 4 if max_run_value else 4)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    legend_handles = [
        mpatches.Patch(color=COLORS["found"], label="All attempts found bug"),
        mpatches.Patch(color=COLORS["partial_found"], label="Some attempts missed"),
        mpatches.Patch(color=COLORS["not_found"], label="No successful attempts"),
    ]
    ax.legend(handles=legend_handles, loc="upper right", fontsize=9)

    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "runs_to_bug.png"), dpi=200, bbox_inches="tight")
    plt.close(fig)
    print("  runs_to_bug.png")


# ── Figure 2: Bug-finding trace swimlane ──

def fig_swimlane(traces, out_dir, rep_attempts, multi_attempt):
    """Swimlane diagram of the representative bug-finding trace."""
    for policy, records in traces.items():
        bug_run = find_bug_run(records)
        if bug_run is None:
            continue
        steps = bug_run.get("steps", [])
        if len(steps) < 2:
            continue

        nodes = sorted(set(step.get("node", "") for step in steps if step["kind"] == "local" and step.get("node")))
        if not nodes:
            continue

        node_y = {node: i for i, node in enumerate(nodes)}
        global_y = len(nodes)
        fig_height = max(3.5, 1.2 * (len(nodes) + 1))
        fig, ax = plt.subplots(figsize=(max(10, len(steps) * 0.25), fig_height))

        for _, y in node_y.items():
            ax.axhline(y=y, color="#eee", linewidth=0.8, zorder=0)
        ax.axhline(y=global_y, color="#eee", linewidth=0.8, zorder=0)

        last_global_label_x = -999
        global_label_above = True
        for i, step in enumerate(steps):
            alts = step["alternatives"]
            idx = step["index"]
            is_nonfifo = idx != 0

            if step["kind"] == "global":
                y = global_y
                color = COLORS["non_fifo"] if is_nonfifo else COLORS["global_fifo"]
                size = max(30, alts * 18)
                ax.scatter(i, y, c=color, s=size, alpha=0.85, edgecolors="#333", linewidth=0.4, zorder=3)

                fr = step.get("from", "")
                to = step.get("to", "")
                if fr in node_y and to in node_y:
                    ax.annotate(
                        "",
                        xy=(i, node_y[to]),
                        xytext=(i, node_y[fr]),
                        arrowprops=dict(arrowstyle="->", color=color, alpha=0.4, lw=0.8),
                        zorder=1,
                    )

                if is_nonfifo:
                    label = f"{step.get('from', '?')}\u2192{step.get('to', '?')} {step.get('msg_type', '')[:3]}"
                    if i - last_global_label_x < 3:
                        global_label_above = not global_label_above
                    else:
                        global_label_above = True
                    offset_y = 10 if global_label_above else -12
                    va = "bottom" if global_label_above else "top"
                    ax.annotate(
                        label,
                        (i, y),
                        textcoords="offset points",
                        xytext=(0, offset_y),
                        ha="center",
                        fontsize=5.5,
                        color=COLORS["non_fifo"],
                        fontweight="bold",
                        va=va,
                    )
                    last_global_label_x = i
            else:
                node = step.get("node", "")
                y = node_y.get(node, 0)
                color = COLORS["non_fifo"] if is_nonfifo else COLORS["local_fifo"]
                size = max(15, alts * 12)
                ax.scatter(i, y, c=color, s=size, alpha=0.85, edgecolors="#333", linewidth=0.3, zorder=3)

                if is_nonfifo and alts > 1:
                    ax.annotate(
                        f"B{step.get('chosen_bgid', '?')}",
                        (i, y),
                        textcoords="offset points",
                        xytext=(0, -10),
                        ha="center",
                        fontsize=5.5,
                        color=COLORS["non_fifo"],
                        fontweight="bold",
                    )

        yticks = list(range(len(nodes))) + [global_y]
        yticklabels = [f"Node {node}" for node in nodes] + ["Global\nDelivery"]
        ax.set_yticks(yticks)
        ax.set_yticklabels(yticklabels, fontsize=9)
        ax.set_xlabel("Decision Step", fontsize=10)
        run_num = bug_run.get("run_num", "?")
        ax.set_title(
            f"Bug-Finding Trace Swimlane \u2014 {algo_label(policy).replace(chr(10), ' ')} "
            f"(run {run_num}{title_attempt_suffix(policy, rep_attempts, multi_attempt)}, {len(steps)} steps)",
            fontsize=11,
            fontweight="bold",
            pad=12,
        )
        ax.set_ylim(-0.5, global_y + 0.8)
        ax.set_xlim(-1, len(steps))
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

        legend_elements = [
            Line2D([0], [0], marker="o", color="w", markerfacecolor=COLORS["global_fifo"],
                   markersize=7, label="Global (FIFO)"),
            Line2D([0], [0], marker="o", color="w", markerfacecolor=COLORS["local_fifo"],
                   markersize=7, label="Local (FIFO)"),
            Line2D([0], [0], marker="o", color="w", markerfacecolor=COLORS["non_fifo"],
                   markersize=7, label="Non-FIFO"),
        ]
        ax.legend(handles=legend_elements, loc="upper right", fontsize=8, framealpha=0.9)

        fig.tight_layout()
        fig.savefig(os.path.join(out_dir, f"swimlane_{policy}.png"), dpi=200, bbox_inches="tight")
        plt.close(fig)
        print(f"  swimlane_{policy}.png")


# ── Figure 3: Message sequence diagram ──

def fig_sequence_diagram(traces, out_dir, rep_attempts, multi_attempt):
    """UML-style sequence diagram of message deliveries in the representative bug trace."""
    for policy, records in traces.items():
        bug_run = find_bug_run(records)
        if bug_run is None:
            continue
        steps = bug_run.get("steps", [])
        msgs = [step for step in steps if step["kind"] == "global"]
        if len(msgs) < 2:
            continue

        nodes = sorted(set(step.get("from", "") for step in msgs) | set(step.get("to", "") for step in msgs))
        if not nodes:
            continue

        node_x = {node: i * 2 for i, node in enumerate(nodes)}
        fig_width = max(5, len(nodes) * 2.5)
        fig_height = max(4, len(msgs) * 0.4 + 2)
        fig, ax = plt.subplots(figsize=(fig_width, fig_height))

        for node, x in node_x.items():
            ax.plot([x, x], [0, len(msgs) + 1], color="#ccc", linewidth=1.5, linestyle="--", zorder=0)
            ax.text(x, -0.5, f"Node {node}", ha="center", va="top", fontsize=10, fontweight="bold")

        for i, step in enumerate(msgs):
            y = i + 1
            fr = step.get("from", "")
            to = step.get("to", "")
            msg_type = step.get("msg_type", "?")
            is_nonfifo = step["index"] != 0
            color = COLORS["non_fifo"] if is_nonfifo else "#666"
            lw = 1.8 if is_nonfifo else 1.0
            style = "->" if msg_type == "Request" else "-|>"
            ax.annotate("", xy=(node_x.get(to, 0), y), xytext=(node_x.get(fr, 0), y),
                        arrowprops=dict(arrowstyle=style, color=color, lw=lw))
            label = f"{fr}\u2192{to} {msg_type}"
            if is_nonfifo:
                label = f"{label} [{step['index']}/{step['alternatives']}]"
            ax.text((node_x.get(fr, 0) + node_x.get(to, 0)) / 2, y + 0.15, label,
                    ha="center", va="bottom", fontsize=7, color=color,
                    fontweight="bold" if is_nonfifo else "normal")

        ax.set_xlim(-1, max(node_x.values()) + 1)
        ax.set_ylim(len(msgs) + 1.5, -1)
        ax.set_axis_off()
        ax.set_title(
            f"Message Sequence \u2014 {algo_label(policy).replace(chr(10), ' ')} "
            f"(run {bug_run.get('run_num', '?')}{title_attempt_suffix(policy, rep_attempts, multi_attempt)})",
            fontsize=11,
            fontweight="bold",
            pad=10,
        )

        legend_elements = [
            Line2D([0], [0], color="#666", linewidth=1, label="FIFO delivery"),
            Line2D([0], [0], color=COLORS["non_fifo"], linewidth=1.8, label="Non-FIFO delivery"),
        ]
        ax.legend(handles=legend_elements, loc="lower right", fontsize=8, framealpha=0.9)

        fig.tight_layout()
        fig.savefig(os.path.join(out_dir, f"sequence_{policy}.png"), dpi=200, bbox_inches="tight")
        plt.close(fig)
        print(f"  sequence_{policy}.png")


# ── Figure 4: Non-FIFO decision comparison ──

def fig_nonfifo_from_traces(traces, summaries, out_dir):
    """Non-FIFO breakdown per algorithm from representative trace data."""
    policies = []
    g_nonfifo = []
    l_nonfifo = []

    for policy in policies_in_order(traces, summaries):
        if policy in traces:
            bug = find_bug_run(traces[policy])
            if bug and bug.get("steps"):
                steps = bug["steps"]
                policies.append(policy)
                measured = measured_steps(steps)
                g_nonfifo.append(sum(1 for step in measured if step["kind"] == "global" and step["index"] != 0))
                l_nonfifo.append(sum(1 for step in measured if step["kind"] == "local" and step["index"] != 0))
        elif policy in summaries:
            bug = find_bug_run(summaries[policy])
            if bug:
                policies.append(policy)
                g_nonfifo.append(metric_value(bug, "non_fifo", 0))
                l_nonfifo.append(0)

    if not policies:
        return

    x = np.arange(len(policies))
    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.bar(x, g_nonfifo, 0.5, label="Global non-FIFO", color=COLORS["global_fifo"], edgecolor="#333", linewidth=0.5)
    ax.bar(x, l_nonfifo, 0.5, bottom=g_nonfifo, label="Local non-FIFO",
           color=COLORS["local_fifo"], edgecolor="#333", linewidth=0.5)

    for i, (global_count, local_count) in enumerate(zip(g_nonfifo, l_nonfifo)):
        total = global_count + local_count
        if total > 0:
            ax.text(i, total + 0.3, str(total), ha="center", va="bottom", fontsize=10, fontweight="bold")

    ax.set_xticks(x)
    ax.set_xticklabels([algo_label(policy) for policy in policies], fontsize=9)
    ax.set_ylabel("Non-FIFO Decisions", fontsize=10)
    ax.set_title("Non-FIFO Decisions in Bug-Finding Run", fontsize=12, fontweight="bold", pad=12)
    ax.legend(fontsize=9)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.set_ylim(0, max((global_count + local_count for global_count, local_count in zip(g_nonfifo, l_nonfifo)), default=1) * 1.2 + 1)

    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "nonfifo_comparison.png"), dpi=200, bbox_inches="tight")
    plt.close(fig)
    print("  nonfifo_comparison.png")


# ── Figure 5: CHESS exploration tree ──

def fig_chess_tree(tree_files, out_dir, rep_attempts, multi_attempt):
    """Representative CHESS DFS tree."""
    for policy, tree_path in tree_files.items():
        tree = load_json(tree_path)
        if not tree:
            continue

        children = defaultdict(list)
        for i, node in enumerate(tree):
            if node["Parent"] >= 0:
                children[node["Parent"]].append(i)

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
                x_pos[idx] = np.mean([x_pos[kid] for kid in kids])

        roots = [i for i, node in enumerate(tree) if node["Parent"] < 0]
        for root in roots:
            layout(root, 0)

        max_width = 18
        n_nodes = len(tree)
        if next_x[0] > max_width:
            scale = max_width / next_x[0]
            x_pos = {idx: pos * scale for idx, pos in x_pos.items()}

        fig_w = min(14, max(6, max_width * 0.7))
        fig_h = min(8, max(4, max(y_pos.values(), default=1) * 0.6 + 2))
        fig, ax = plt.subplots(figsize=(fig_w, fig_h))

        for i, node in enumerate(tree):
            if node["Parent"] >= 0 and node["Parent"] in x_pos:
                ax.plot([x_pos[node["Parent"]], x_pos[i]],
                        [y_pos[node["Parent"]], y_pos[i]],
                        color=COLORS["edge"], linewidth=0.6, zorder=1)

        xs = [x_pos[i] for i in range(n_nodes)]
        ys = [y_pos[i] for i in range(n_nodes)]
        node_colors = [COLORS["failed"] if not node["Passed"] else COLORS["passed"] for node in tree]
        node_size = max(8, min(40, 800 / max(n_nodes, 1)))
        ax.scatter(xs, ys, c=node_colors, s=node_size, zorder=2, edgecolors="#333", linewidth=0.3)

        n_passed = sum(1 for node in tree if node["Passed"])
        n_failed = n_nodes - n_passed
        ax.set_xlabel("Exploration Frontier", fontsize=10)
        ax.set_ylabel("DFS Depth", fontsize=10)
        ax.set_title(
            f"CHESS Tree \u2014 {algo_label(policy).replace(chr(10), ' ')} "
            f"({n_nodes} runs, {n_failed} bugs{title_attempt_suffix(policy, rep_attempts, multi_attempt)})",
            fontsize=11,
            fontweight="bold",
            pad=10,
        )
        ax.invert_yaxis()
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.legend(
            handles=[
                mpatches.Patch(color=COLORS["passed"], label=f"Passed ({n_passed})"),
                mpatches.Patch(color=COLORS["failed"], label=f"Bug found ({n_failed})"),
            ],
            loc="upper right",
            fontsize=8,
        )

        fig.tight_layout()
        fig.savefig(os.path.join(out_dir, f"tree_{policy}.png"), dpi=200, bbox_inches="tight")
        plt.close(fig)
        print(f"  tree_{policy}.png")


# ── Figure 6: Per-run non-FIFO count over runs ──

def fig_nonfifo_over_runs(summaries, out_dir):
    """Aggregate non-FIFO decision count per run across attempts."""
    policies = [policy for policy in policies_in_order(summaries) if summaries.get(policy)]
    if not policies:
        return

    fig, ax = plt.subplots(figsize=(10, 4.4))
    for i, policy in enumerate(policies):
        values_by_attempt = []
        for _, records in attempt_series(summaries[policy]):
            ordered = sorted(records, key=lambda record: record.get("run_num", 0))
            values_by_attempt.append([metric_value(record, "non_fifo", 0) for record in ordered])
        aggregated = aggregate_series(values_by_attempt)
        if aggregated is None:
            continue

        color = LINE_COLORS[i % len(LINE_COLORS)]
        ax.fill_between(aggregated["runs"], aggregated["min"], aggregated["max"],
                        color=color, alpha=0.14, linewidth=0)
        ax.plot(aggregated["runs"], aggregated["mean"], label=algo_label(policy).replace("\n", " "),
                linewidth=1.8, color=color)

    ax.set_xlabel("Run", fontsize=10)
    ax.set_ylabel("Non-FIFO Decisions", fontsize=10)
    ax.set_title("Non-FIFO Decisions per Run Across Attempts", fontsize=12, fontweight="bold", pad=10)
    ax.legend(fontsize=8, loc="upper right")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "nonfifo_over_runs.png"), dpi=200, bbox_inches="tight")
    plt.close(fig)
    print("  nonfifo_over_runs.png")


# ── Figure 7: Theoretical vs explored search space ──

def fig_search_space_vs_explored(summaries, traces, out_dir):
    """Approximate theoretical interleavings versus runs actually explored."""
    policies = []
    theoretical = []
    explored = []

    for policy in policies_in_order(summaries, traces):
        metrics = bug_trace_metrics(policy, summaries, traces)
        if metrics is None:
            continue
        policies.append(policy)
        theoretical.append(search_space_upper_bound(metrics["local_sizes"], metrics["global_sizes"]))
        explored.append(metrics["explored_runs"])

    if not policies:
        return

    x = np.arange(len(policies))
    width = 0.35
    theoretical_plot = np.array([float(value) for value in theoretical], dtype=float)
    explored_plot = np.array([float(value) for value in explored], dtype=float)
    fig, ax = plt.subplots(figsize=(9, 4.8))
    bars_theoretical = ax.bar(x - width / 2, theoretical_plot, width,
                              label="Theoretical interleavings", color="#D9E8F7",
                              edgecolor="#333", linewidth=0.5)
    bars_explored = ax.bar(x + width / 2, explored_plot, width,
                           label="Runs explored", color=COLORS["found"],
                           edgecolor="#333", linewidth=0.5)

    for bar, val in zip(bars_theoretical, theoretical):
        ax.text(bar.get_x() + bar.get_width() / 2, val * 1.12, human_large_int(val),
                ha="center", va="bottom", fontsize=8, rotation=90, color="#555")
    for bar, val in zip(bars_explored, explored):
        ax.text(bar.get_x() + bar.get_width() / 2, val * 1.12, str(val),
                ha="center", va="bottom", fontsize=9, fontweight="bold")

    ax.set_xticks(x)
    ax.set_xticklabels([algo_label(policy) for policy in policies], fontsize=9)
    ax.set_yscale("log")
    ax.set_ylabel("Count (log scale)", fontsize=10)
    ax.set_title("Search Space: Theoretical vs Explored", fontsize=12, fontweight="bold", pad=12)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.legend(fontsize=9)
    ax.text(0.01, 0.01,
            "Upper bound from per-step local runq sizes and global queue sizes in the bug-finding trace.",
            transform=ax.transAxes, fontsize=8, color="#666", va="bottom")

    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "search_space_vs_explored.png"), dpi=200, bbox_inches="tight")
    plt.close(fig)
    print("  search_space_vs_explored.png")


# ── Figure 8: Local vs global decisions in bug trace ──

def fig_bug_trace_decision_mix(summaries, traces, trees, out_dir):
    """Aggregate local/global decision counts across successful attempts."""
    rows = []
    for policy in policies_in_order(summaries, traces):
        found_attempts = found_bug_attempts(policy, summaries, traces)
        attempts = successful_bug_trace_counts(policy, summaries, traces, trees)
        if not found_attempts:
            continue
        if attempts:
            local_mean = float(np.mean([item["local"] for item in attempts]))
            global_mean = float(np.mean([item["global"] for item in attempts]))
        else:
            local_mean = 0.0
            global_mean = 0.0
        total_attempts = len(summaries.get(policy, {})) or len(traces.get(policy, {}))
        rows.append(
            {
                "policy": policy,
                "local": local_mean,
                "global": global_mean,
                "found": len(found_attempts),
                "total": total_attempts,
                "usable": len(attempts),
            }
        )

    if not rows:
        return

    x = np.arange(len(rows))
    local_counts = [row["local"] for row in rows]
    global_counts = [row["global"] for row in rows]
    fig, ax = plt.subplots(figsize=(8.8, 4.7))
    ax.bar(x, local_counts, width=0.55, label="Local decisions",
           color=COLORS["local_fifo"], edgecolor="#333", linewidth=0.5)
    ax.bar(x, global_counts, width=0.55, bottom=local_counts, label="Global decisions",
           color=COLORS["global_fifo"], edgecolor="#333", linewidth=0.5)

    max_total = 0
    for i, row in enumerate(rows):
        total = row["local"] + row["global"]
        max_total = max(max_total, total)
        if total == 0 and row["found"] > 0:
            marker = "D" if row["usable"] > 0 else "x"
            ax.scatter([i], [0], marker=marker, s=42, color=COLORS["found"],
                       linewidths=1.2, zorder=5, clip_on=False)
            value_y = 0.42
            rate_y = 0.12
        else:
            value_y = total + 0.8
            rate_y = total + 0.2
        ax.text(i, value_y, f"{total:.1f}", ha="center", va="bottom", fontsize=10, fontweight="bold")
        ax.text(i, rate_y, found_rate_text(row["found"], row["total"]),
                ha="center", va="bottom", fontsize=7.5, color="#666")
        if row["usable"] < row["found"]:
            ax.text(i, rate_y - 0.36 if total == 0 else rate_y - 0.28,
                    f"trace {row['usable']}/{row['found']}",
                    ha="center", va="bottom", fontsize=7.0, color="#999")

    ax.set_xticks(x)
    ax.set_xticklabels([algo_label(row["policy"]) for row in rows], fontsize=9)
    ax.set_ylabel("Mean Decisions in Failing Run", fontsize=10)
    ax.set_title("Bug-Finding Interleaving: Local vs Global Decisions Across Attempts",
                 fontsize=12, fontweight="bold", pad=12)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.legend(fontsize=9)
    ax.set_ylim(0, max_total * 1.22 if max_total else 1)

    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "bug_trace_decision_mix.png"), dpi=200, bbox_inches="tight")
    plt.close(fig)
    print("  bug_trace_decision_mix.png")


# ── Figure 9: Detailed trace with RPC labels ──

def fig_detailed_trace(traces, out_dir, rep_attempts, multi_attempt):
    """Per-node timeline for the representative bug-finding trace."""
    for policy, records in traces.items():
        bug_run = find_bug_run(records)
        if bug_run is None:
            continue
        steps = bug_run.get("steps", [])
        if len(steps) < 2:
            continue

        nodes = sorted(set(step.get("node", "") for step in steps if step["kind"] == "local" and step.get("node")))
        if not nodes:
            continue

        y_map = {node: i for i, node in enumerate(nodes)}
        global_y = len(nodes)
        fig_width = max(10, min(20, len(steps) * 0.35))
        fig_height = max(3.5, (len(nodes) + 1) * 1.0 + 1.5)
        fig, ax = plt.subplots(figsize=(fig_width, fig_height))

        label_above = True
        for i, step in enumerate(steps):
            alts = step["alternatives"]
            is_nonfifo = step["index"] != 0
            if step["kind"] == "global":
                y = global_y
                color = COLORS["non_fifo"] if is_nonfifo else COLORS["global_fifo"]
                size = max(25, alts * 16)
            else:
                y = y_map.get(step.get("node", ""), 0)
                color = COLORS["non_fifo"] if is_nonfifo else COLORS["local_fifo"]
                size = max(12, alts * 10)

            ax.scatter(i, y, c=color, s=size, alpha=0.8, edgecolors="#333", linewidth=0.3, zorder=3)
            if step["kind"] == "global" and (is_nonfifo or alts > 1):
                label = f"{step.get('from', '?')}\u2192{step.get('to', '?')} {step.get('msg_type', '')[:3]}"
                offset = 12 if label_above else -12
                va = "bottom" if label_above else "top"
                ax.annotate(label, (i, y), textcoords="offset points", xytext=(0, offset),
                            ha="center", fontsize=5.5, fontweight="bold" if is_nonfifo else "normal",
                            color=color, va=va)
                label_above = not label_above
            elif step["kind"] == "local" and is_nonfifo and alts > 1:
                ax.annotate(f"B{step.get('chosen_bgid', '?')}", (i, y), textcoords="offset points",
                            xytext=(0, -10), ha="center", fontsize=5.5,
                            color=COLORS["non_fifo"], fontweight="bold")

        ax.set_yticks(list(range(len(nodes))) + [global_y])
        ax.set_yticklabels([f"Node {node}" for node in nodes] + ["Global"], fontsize=9)
        ax.set_xlabel("Decision Step", fontsize=10)
        ax.set_title(
            f"Detailed Trace \u2014 {algo_label(policy).replace(chr(10), ' ')} "
            f"(run {bug_run.get('run_num', '?')}{title_attempt_suffix(policy, rep_attempts, multi_attempt)}, "
            f"{len(steps)} decisions)",
            fontsize=11,
            fontweight="bold",
            pad=12,
        )
        ax.set_ylim(-0.5, global_y + 0.8)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.legend(
            handles=[
                Line2D([0], [0], marker="o", color="w", markerfacecolor=COLORS["global_fifo"],
                       markersize=7, label="Global (FIFO)"),
                Line2D([0], [0], marker="o", color="w", markerfacecolor=COLORS["local_fifo"],
                       markersize=7, label="Local (FIFO)"),
                Line2D([0], [0], marker="o", color="w", markerfacecolor=COLORS["non_fifo"],
                       markersize=7, label="Non-FIFO"),
            ],
            loc="upper right",
            fontsize=7,
            framealpha=0.9,
        )

        fig.tight_layout()
        fig.savefig(os.path.join(out_dir, f"detailed_{policy}.png"), dpi=200, bbox_inches="tight")
        plt.close(fig)
        print(f"  detailed_{policy}.png")


# ── Figure 10: Narrative trace ──

def fig_narrative_trace(traces, out_dir, rep_attempts, multi_attempt):
    """Human-readable narrative of each representative bug-finding run."""
    for policy, records in traces.items():
        bug_run = find_bug_run(records)
        if bug_run is None:
            continue

        steps = bug_run.get("steps", [])
        outpath = os.path.join(out_dir, f"narrative_{policy}.txt")
        with open(outpath, "w") as out:
            out.write(f"{'=' * 70}\n")
            out.write(
                f"Bug-Finding Trace: {policy}, Run {bug_run['run_num']}"
                f"{title_attempt_suffix(policy, rep_attempts, multi_attempt).replace(', ', ' (').rstrip()}"
            )
            if multi_attempt and policy in rep_attempts:
                out.write(")")
            out.write("\n")
            out.write(f"Total decisions: {len(steps)}\n")
            out.write(f"{'=' * 70}\n\n")

            global_step = 0
            for i, step in enumerate(steps):
                nonfifo = " ** NON-FIFO **" if step["index"] != 0 else ""
                if step["kind"] == "global":
                    global_step += 1
                    out.write(
                        f"Step {i:3d}: [GLOBAL #{global_step}]  "
                        f"Deliver {step.get('from', '?')}\u2192{step.get('to', '?')}({step.get('msg_type', '?')})  "
                        f"[chose {step['index']} of {step['alternatives']}]{nonfifo}\n"
                    )
                else:
                    runq = step.get("runq_bgids", [])
                    runq_str = ", ".join(f"B{bgid}" for bgid in runq) if runq else "?"
                    out.write(
                        f"Step {i:3d}: [LOCAL  {step.get('node', '?')}]     "
                        f"Run B{step.get('chosen_bgid', 0)}  "
                        f"from [{runq_str}]  "
                        f"[chose {step['index']} of {step['alternatives']}]{nonfifo}\n"
                    )

            out.write(f"\n{'=' * 70}\n")
            out.write(f"RESULT: BUG FOUND on run {bug_run['run_num']}\n")
            out.write(f"  Global decisions: {global_step}\n")
            out.write(f"  Local decisions:  {len(steps) - global_step}\n")
            non_fifo = sum(1 for step in steps if step["index"] != 0)
            out.write(f"  Non-FIFO choices: {non_fifo}\n")
            if non_fifo > 0:
                out.write("\nKey non-FIFO decisions:\n")
                for i, step in enumerate(steps):
                    if step["index"] == 0:
                        continue
                    if step["kind"] == "global":
                        out.write(
                            f"  Step {i}: GLOBAL  Deliver {step.get('from', '?')}\u2192{step.get('to', '?')}"
                            f"({step.get('msg_type', '?')})  "
                            f"[index {step['index']} of {step['alternatives']}]\n"
                        )
                    else:
                        runq = ", ".join(f"B{bgid}" for bgid in step.get("runq_bgids", []))
                        out.write(
                            f"  Step {i}: LOCAL   node={step.get('node', '?')}  "
                            f"B{step.get('chosen_bgid', 0)} from [{runq}]  "
                            f"[index {step['index']} of {step['alternatives']}]\n"
                        )
            out.write(f"{'=' * 70}\n")

        print(f"  narrative_{policy}.txt")


# ── Figure 11: Algorithm comparison summary table ──

def fig_summary_table(summaries, out_dir):
    """Table comparing algorithms on attempt-aware metrics."""
    policies = [policy for policy in policies_in_order(summaries) if summaries.get(policy)]
    if not policies:
        return

    columns = ["Algorithm", "Attempts", "Found\nRate", "Avg Effective\nRuns to Bug",
               "Avg\nNon-FIFO", "Avg Total\nDecisions"]
    rows = []
    for policy in policies:
        attempts = attempt_series(summaries[policy])
        effective_runs = [effective_runs_to_bug(records) for _, records in attempts]
        found = sum(1 for _, records in attempts if find_bug_run(records))
        avg_nf = np.mean([metric_value(record, "non_fifo", 0) for _, records in attempts for record in records])
        avg_td = np.mean([metric_value(record, "total_decisions", 0) for _, records in attempts for record in records])
        rows.append([
            algo_label(policy).replace("\n", " "),
            str(len(attempts)),
            found_rate_text(found, len(attempts)),
            f"{float(np.mean(effective_runs)):.1f}",
            f"{float(avg_nf):.1f}",
            f"{float(avg_td):.1f}",
        ])

    fig, ax = plt.subplots(figsize=(10.5, 1.2 + len(rows) * 0.5))
    ax.set_axis_off()
    table = ax.table(cellText=rows, colLabels=columns, loc="center",
                     cellLoc="center", colColours=["#E8E8E8"] * len(columns))
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1, 1.6)

    for i, row in enumerate(rows):
        found, total = row[2].replace("found ", "").split("/")
        cell = table[i + 1, 2]
        if int(found) == int(total):
            cell.set_facecolor("#E8F5E9")
        elif int(found) == 0:
            cell.set_facecolor("#FFEBEE")
        else:
            cell.set_facecolor("#FFF8E1")

    ax.set_title("Algorithm Comparison Summary Across Attempts", fontsize=12, fontweight="bold", pad=20)

    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "summary_table.png"), dpi=200, bbox_inches="tight")
    plt.close(fig)
    print("  summary_table.png")


# ── Figure 12: Cumulative unique traces over runs ──

def fig_cumulative_unique_traces(summaries, out_dir):
    """Show how quickly each algorithm accumulates distinct delivery traces across attempts."""
    policies = [policy for policy in policies_in_order(summaries) if summaries.get(policy)]
    if not policies:
        return

    fig, ax = plt.subplots(figsize=(10, 4.8))
    max_runs = 0
    max_unique = 0
    for i, policy in enumerate(policies):
        values_by_attempt = []
        for _, records in attempt_series(summaries[policy]):
            seen = set()
            cumulative = []
            for record in sorted(records, key=lambda r: r.get("run_num", 0)):
                fingerprint = metric_value(record, "trace_fingerprint", "")
                if fingerprint:
                    seen.add(fingerprint)
                cumulative.append(len(seen))
            values_by_attempt.append(cumulative)
        aggregated = aggregate_series(values_by_attempt)
        if aggregated is None:
            continue
        max_runs = max(max_runs, int(aggregated["runs"][-1]))
        max_unique = max(max_unique, float(np.max(aggregated["max"])))
        color = LINE_COLORS[i % len(LINE_COLORS)]
        ax.fill_between(aggregated["runs"], aggregated["min"], aggregated["max"],
                        color=color, alpha=0.14, linewidth=0)
        ax.plot(aggregated["runs"], aggregated["mean"],
                label=algo_label(policy).replace("\n", " "), linewidth=1.8, color=color)

    if max_runs == 0:
        plt.close(fig)
        return

    ax.plot([1, max_runs], [1, max_runs], linestyle="--", linewidth=1,
            color="#BDBDBD", label="Ideal no-repetition")
    ax.set_xlim(1, max_runs)
    ax.set_ylim(0, max(max_unique, 1) * 1.08)
    ax.set_xlabel("Run", fontsize=10)
    ax.set_ylabel("Cumulative unique delivery traces", fontsize=10)
    ax.set_title("Exploration Diversity Over Runs Across Attempts", fontsize=12, fontweight="bold", pad=10)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.legend(fontsize=8, loc="lower right")

    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "cumulative_unique_traces.png"), dpi=200, bbox_inches="tight")
    plt.close(fig)
    print("  cumulative_unique_traces.png")


# ── Figure 13: Queue pressure in the bug-finding run ──

def fig_queue_pressure_profile(summaries, out_dir):
    """Compare queue pressure in the representative failing run."""
    series = []
    for policy in policies_in_order(summaries):
        bug = find_bug_run(summaries.get(policy, []))
        if bug is None:
            continue
        queue_sizes = list(metric_value(bug, "queue_sizes", []) or [])
        if queue_sizes:
            series.append((policy, queue_sizes))

    if not series:
        return

    fig, ax = plt.subplots(figsize=(9, 4.8))
    for i, (policy, queue_sizes) in enumerate(series):
        x = np.arange(1, len(queue_sizes) + 1)
        y = np.array(queue_sizes)
        color = LINE_COLORS[i % len(LINE_COLORS)]
        ax.plot(x, y, linewidth=1.8, color=color, label=algo_label(policy).replace("\n", " "))
        ax.scatter(x, y, s=20, color=color, edgecolors="#333", linewidth=0.3, zorder=4)

    ax.set_xlabel("Global decision index in failing run", fontsize=10)
    ax.set_ylabel("Runnable messages in global queue", fontsize=10)
    ax.set_title("Queue Pressure Profile of the Failing Interleaving", fontsize=12, fontweight="bold", pad=10)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.legend(fontsize=8, loc="upper right")

    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "queue_pressure_profile.png"), dpi=200, bbox_inches="tight")
    plt.close(fig)
    print("  queue_pressure_profile.png")


# ── Figure 14: Per-node local decision burden ──

def fig_local_decision_burden(summaries, out_dir):
    """Heatmap of where local scheduling choices concentrate in the representative failing run."""
    all_nodes = sorted({
        node
        for records in summaries.values()
        for record in records
        for node in (record.get("local_decision_by_node") or {}).keys()
    })
    if not all_nodes:
        return

    rows = []
    policies = []
    for policy in policies_in_order(summaries):
        bug = find_bug_run(summaries.get(policy, []))
        if bug is None:
            continue
        counts = metric_value(bug, "local_decision_by_node", {}) or {}
        rows.append([counts.get(node, 0) for node in all_nodes])
        policies.append(policy)

    if not rows:
        return

    matrix = np.array(rows, dtype=float)
    fig, ax = plt.subplots(figsize=(max(6.5, len(all_nodes) * 1.2 + 2.5), max(3.5, len(policies) * 0.7 + 2.0)))
    im = ax.imshow(matrix, cmap="Blues", aspect="auto")
    ax.set_xticks(np.arange(len(all_nodes)))
    ax.set_xticklabels([f"Node {node}" for node in all_nodes], fontsize=9)
    ax.set_yticks(np.arange(len(policies)))
    ax.set_yticklabels([algo_label(policy).replace("\n", " ") for policy in policies], fontsize=9)
    ax.set_title("Local Scheduling Burden by Node in the Failing Run", fontsize=12, fontweight="bold", pad=10)

    for i in range(matrix.shape[0]):
        for j in range(matrix.shape[1]):
            value = int(matrix[i, j])
            text_color = "white" if matrix[i, j] > matrix.max() * 0.55 else "#222"
            ax.text(j, i, str(value), ha="center", va="center", fontsize=9, color=text_color)

    cbar = fig.colorbar(im, ax=ax, shrink=0.9)
    cbar.set_label("Local decisions", fontsize=9)

    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "local_decision_burden.png"), dpi=200, bbox_inches="tight")
    plt.close(fig)
    print("  local_decision_burden.png")


# ── Figure 15: Non-FIFO positions inside the failing trace ──

def fig_nonfifo_position_profile(traces, out_dir):
    """Show where the divergence from FIFO happens within each representative failing trace."""
    rows = []
    for policy in policies_in_order(traces):
        bug = find_bug_run(traces.get(policy, []))
        if bug is None:
            continue
        steps = bug.get("steps", [])
        if not steps:
            continue
        total_steps = len(steps)
        rows.append(
            (
                policy,
                [
                    (idx + 1) / total_steps
                    for idx, step in enumerate(steps)
                    if not is_control_step(step) and step["kind"] == "local" and step["index"] != 0
                ],
                [
                    (idx + 1) / total_steps
                    for idx, step in enumerate(steps)
                    if not is_control_step(step) and step["kind"] == "global" and step["index"] != 0
                ],
                total_steps,
            )
        )

    if not rows:
        return

    fig, ax = plt.subplots(figsize=(10, max(3.8, len(rows) * 0.75 + 1.6)))
    local_labeled = False
    global_labeled = False
    for y, (policy, local_positions, global_positions, total_steps) in enumerate(rows):
        ax.hlines(y, 0, 1, color="#E6E6E6", linewidth=1, zorder=0)
        if local_positions:
            ax.scatter(local_positions, [y] * len(local_positions), marker="o", s=46,
                       color=COLORS["local_fifo"], edgecolors="#333", linewidth=0.4,
                       label="Local non-FIFO" if not local_labeled else None, zorder=3)
            local_labeled = True
        if global_positions:
            ax.scatter(global_positions, [y] * len(global_positions), marker="D", s=42,
                       color=COLORS["global_fifo"], edgecolors="#333", linewidth=0.4,
                       label="Global non-FIFO" if not global_labeled else None, zorder=4)
            global_labeled = True
        ax.text(1.01, y, f"{total_steps} steps", va="center", ha="left", fontsize=8, color="#666")

    ax.set_yticks(np.arange(len(rows)))
    ax.set_yticklabels([algo_label(policy).replace("\n", " ") for policy, _, _, _ in rows], fontsize=9)
    ax.set_xlim(0, 1.08)
    ax.set_xticks(np.linspace(0, 1, 6))
    ax.set_xticklabels([f"{int(x * 100)}%" for x in np.linspace(0, 1, 6)], fontsize=9)
    ax.set_xlabel("Position within failing trace", fontsize=10)
    ax.set_title("Where Non-FIFO Decisions Occur in the Failing Trace", fontsize=12, fontweight="bold", pad=10)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    if local_labeled or global_labeled:
        ax.legend(fontsize=8, loc="upper right")

    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "nonfifo_position_profile.png"), dpi=200, bbox_inches="tight")
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

    summaries = load_all_summaries(args.data)
    traces = load_all_traces(args.data)
    apply_chart_adjustments(summaries, traces)
    trees = load_tree_files(args.data)
    multi_attempt = has_multi_attempt_data(summaries, traces, trees)
    rep_summaries, rep_traces, rep_trees, rep_attempts = representative_views(summaries, traces, trees)

    print("Figure 1: Runs to first bug")
    fig_runs_to_bug(summaries, args.out)

    print("\nFigure 2: Bug-finding swimlane")
    fig_swimlane(rep_traces, args.out, rep_attempts, multi_attempt)

    print("\nFigure 3: Message sequence diagram")
    fig_sequence_diagram(rep_traces, args.out, rep_attempts, multi_attempt)

    print("\nFigure 4: Non-FIFO decision comparison")
    fig_nonfifo_from_traces(rep_traces, rep_summaries, args.out)

    print("\nFigure 5: CHESS tree")
    fig_chess_tree(rep_trees, args.out, rep_attempts, multi_attempt)

    print("\nFigure 6: Non-FIFO over runs")
    fig_nonfifo_over_runs(summaries, args.out)

    print("\nFigure 7: Search space vs explored")
    fig_search_space_vs_explored(rep_summaries, rep_traces, args.out)

    print("\nFigure 8: Bug trace decision mix")
    fig_bug_trace_decision_mix(summaries, traces, trees, args.out)

    print("\nFigure 9: Detailed trace")
    fig_detailed_trace(rep_traces, args.out, rep_attempts, multi_attempt)

    print("\nFigure 10: Narrative trace (text)")
    fig_narrative_trace(rep_traces, args.out, rep_attempts, multi_attempt)

    print("\nFigure 11: Summary table")
    fig_summary_table(summaries, args.out)

    print("\nFigure 12: Cumulative unique traces")
    fig_cumulative_unique_traces(summaries, args.out)

    print("\nFigure 13: Queue pressure profile")
    fig_queue_pressure_profile(rep_summaries, args.out)

    print("\nFigure 14: Local decision burden")
    fig_local_decision_burden(rep_summaries, args.out)

    print("\nFigure 15: Non-FIFO position profile")
    fig_nonfifo_position_profile(rep_traces, args.out)

    print(f"\nDone. Figures in {args.out}/")


if __name__ == "__main__":
    main()
