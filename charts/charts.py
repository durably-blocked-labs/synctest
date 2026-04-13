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
import numpy as np

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


# ── Figure 1: Runs to first bug (bar chart) ──

def fig_runs_to_bug(data_dir, out_dir):
    """Bar chart: how many runs each algorithm needed to find the bug."""
    results = {}
    for f in find_files(data_dir, ".jsonl"):
        if "-trace" in f.name:
            continue
        records = load_jsonl(f)
        if not records:
            continue
        policy = records[0].get("policy", f.stem)
        for r in records:
            if not r["passed"]:
                results[policy] = r["run_num"]
                break
        else:
            results[policy] = len(records)

    if not results:
        return

    names = list(results.keys())
    runs = [results[n] for n in names]
    colors = ['#2196F3' if r < 500 else '#ccc' for r in runs]

    fig, ax = plt.subplots(figsize=(8, 4))
    bars = ax.bar(names, runs, color=colors, edgecolor='#333', linewidth=0.5)
    for bar, val in zip(bars, runs):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                str(val), ha='center', va='bottom', fontsize=10, fontweight='bold')
    ax.set_ylabel("Runs to first bug")
    ax.set_title("Bug Detection: Runs Required per Algorithm")
    ax.set_ylim(0, max(runs) * 1.15)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "runs_to_bug.png"), dpi=150)
    plt.close(fig)
    print(f"  runs_to_bug.png")


# ── Figure 2: Decision composition per run (stacked area) ──

def fig_decision_composition(data_dir, out_dir):
    """Per-algorithm: stacked area of global vs local decisions per run."""
    for f in find_files(data_dir, ".jsonl"):
        if "-trace" in f.name:
            continue
        records = load_jsonl(f)
        if not records:
            continue
        policy = records[0].get("policy", f.stem)

        run_nums = [r["run_num"] for r in records]
        globals_ = [r.get("global_decision_count", 0) for r in records]
        locals_ = [r.get("local_decision_total", 0) for r in records]
        totals = [r.get("total_decisions", g + l) for r, g, l in zip(records, globals_, locals_)]

        if len(run_nums) < 2:
            continue

        fig, ax = plt.subplots(figsize=(10, 4))
        ax.fill_between(run_nums, 0, globals_, alpha=0.7, label="Global", color='#2196F3')
        ax.fill_between(run_nums, globals_, totals, alpha=0.7, label="Local", color='#FF9800')
        ax.set_xlabel("Run")
        ax.set_ylabel("Decisions")
        ax.set_title(f"Decision Composition — {policy}")
        ax.legend(loc='upper right')
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        fig.tight_layout()
        fig.savefig(os.path.join(out_dir, f"decisions_{policy}.png"), dpi=150)
        plt.close(fig)
        print(f"  decisions_{policy}.png")


# ── Figure 3: Bug-finding run trace timeline ──

def fig_bug_trace(data_dir, out_dir):
    """Timeline of the bug-finding run for each algorithm: each decision
    as a colored dot (blue=global, orange=local), size = alternatives."""
    for f in find_files(data_dir, "-trace.jsonl"):
        records = load_jsonl(f)
        if not records:
            continue
        # Find the failing run.
        bug_run = None
        for r in records:
            if not r["passed"]:
                bug_run = r
                break
        if bug_run is None:
            continue

        policy = bug_run.get("policy", f.stem.replace("-trace", ""))
        steps = bug_run["steps"]
        if not steps:
            continue

        xs = list(range(len(steps)))
        kinds = [s["kind"] for s in steps]
        alts = [s["alternatives"] for s in steps]
        colors = ['#2196F3' if k == 'global' else '#FF9800' for k in kinds]
        sizes = [max(10, a * 15) for a in alts]

        fig, ax = plt.subplots(figsize=(12, 3))
        ax.scatter(xs, [1 if k == 'global' else 0 for k in kinds],
                   c=colors, s=sizes, alpha=0.7, edgecolors='#333', linewidth=0.3)
        ax.set_yticks([0, 1])
        ax.set_yticklabels(["Local", "Global"])
        ax.set_xlabel("Decision step")
        ax.set_title(f"Bug-Finding Trace — {policy} (run {bug_run['run_num']})")
        ax.set_ylim(-0.5, 1.5)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)

        global_patch = mpatches.Patch(color='#2196F3', label='Global delivery')
        local_patch = mpatches.Patch(color='#FF9800', label='Local scheduling')
        ax.legend(handles=[global_patch, local_patch], loc='upper right', fontsize=8)

        fig.tight_layout()
        fig.savefig(os.path.join(out_dir, f"trace_{policy}.png"), dpi=150)
        plt.close(fig)
        print(f"  trace_{policy}.png")


# ── Figure 4: CHESS exploration tree ──

def fig_chess_tree(data_dir, out_dir):
    """Visualize CHESS DFS exploration tree. Each node = one run.
    Color = passed (green) or failed (red). X = branch step, Y = depth."""
    for f in find_files(data_dir, "-tree.json"):
        tree = load_json(f)
        if not tree:
            continue

        name = f.stem.replace("-tree", "")

        # Build adjacency for layout.
        children = defaultdict(list)
        for i, node in enumerate(tree):
            if node["Parent"] >= 0:
                children[node["Parent"]].append(i)

        # BFS for x positions.
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

        # Find root(s).
        roots = [i for i, n in enumerate(tree) if n["Parent"] < 0]
        for root in roots:
            layout(root, 0)

        fig, ax = plt.subplots(figsize=(max(8, len(tree) * 0.15), 6))

        # Draw edges.
        for i, node in enumerate(tree):
            if node["Parent"] >= 0 and node["Parent"] in x_pos:
                ax.plot([x_pos[node["Parent"]], x_pos[i]],
                        [y_pos[node["Parent"]], y_pos[i]],
                        color='#ccc', linewidth=0.5, zorder=1)

        # Draw nodes.
        xs = [x_pos[i] for i in range(len(tree))]
        ys = [y_pos[i] for i in range(len(tree))]
        colors = ['#f44336' if not n["Passed"] else '#4CAF50' for n in tree]
        ax.scatter(xs, ys, c=colors, s=30, zorder=2, edgecolors='#333', linewidth=0.3)

        ax.set_xlabel("Exploration frontier")
        ax.set_ylabel("DFS depth")
        ax.set_title(f"CHESS Exploration Tree — {name} ({len(tree)} runs)")
        ax.invert_yaxis()
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)

        passed_patch = mpatches.Patch(color='#4CAF50', label='Passed')
        failed_patch = mpatches.Patch(color='#f44336', label='Bug found')
        ax.legend(handles=[passed_patch, failed_patch], loc='lower right', fontsize=8)

        fig.tight_layout()
        fig.savefig(os.path.join(out_dir, f"tree_{name}.png"), dpi=150)
        plt.close(fig)
        print(f"  tree_{name}.png")


# ── Figure 5: Queue size / branch points over runs ──

def fig_branch_points(data_dir, out_dir):
    """Line chart: branch points (decisions with >1 alternative) per run."""
    series = {}
    for f in find_files(data_dir, ".jsonl"):
        if "-trace" in f.name:
            continue
        records = load_jsonl(f)
        if not records:
            continue
        policy = records[0].get("policy", f.stem)
        series[policy] = {
            "runs": [r["run_num"] for r in records],
            "branches": [r.get("branch_points", 0) for r in records],
        }

    if not series:
        return

    fig, ax = plt.subplots(figsize=(10, 4))
    for policy, data in series.items():
        if len(data["runs"]) < 2:
            continue
        ax.plot(data["runs"], data["branches"], label=policy, linewidth=1.5, alpha=0.8)
    ax.set_xlabel("Run")
    ax.set_ylabel("Branch points (alts > 1)")
    ax.set_title("Branch Points per Run")
    ax.legend()
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "branch_points.png"), dpi=150)
    plt.close(fig)
    print(f"  branch_points.png")


# ── Figure 6: Message type distribution in bug-finding run ──

def fig_message_types(data_dir, out_dir):
    """Pie chart: distribution of message types in the bug-finding trace."""
    for f in find_files(data_dir, "-trace.jsonl"):
        records = load_jsonl(f)
        bug_run = None
        for r in records:
            if not r["passed"]:
                bug_run = r
                break
        if bug_run is None:
            continue

        policy = bug_run.get("policy", f.stem.replace("-trace", ""))
        types = defaultdict(int)
        for s in bug_run["steps"]:
            if s["kind"] == "global" and s.get("msg_type"):
                types[s["msg_type"]] += 1

        if not types:
            continue

        labels = list(types.keys())
        values = [types[l] for l in labels]

        fig, ax = plt.subplots(figsize=(6, 6))
        ax.pie(values, labels=labels, autopct='%1.0f%%',
               colors=['#2196F3', '#FF9800', '#4CAF50', '#9C27B0', '#F44336'])
        ax.set_title(f"Message Types in Bug Trace — {policy}")
        fig.tight_layout()
        fig.savefig(os.path.join(out_dir, f"msgtypes_{policy}.png"), dpi=150)
        plt.close(fig)
        print(f"  msgtypes_{policy}.png")


# ── Figure 7: Narrative trace of bug-finding run ──

def fig_narrative_trace(data_dir, out_dir):
    """Write a human-readable narrative of each algorithm's bug-finding run."""
    for f in find_files(data_dir, "-trace.jsonl"):
        records = load_jsonl(f)
        bug_run = None
        for r in records:
            if not r["passed"]:
                bug_run = r
                break
        if bug_run is None:
            continue

        policy = bug_run.get("policy", f.stem.replace("-trace", ""))
        steps = bug_run["steps"]
        outpath = os.path.join(out_dir, f"narrative_{policy}.txt")

        with open(outpath, "w") as out:
            out.write(f"{'='*70}\n")
            out.write(f"Bug-Finding Trace: {policy}, Run {bug_run['run_num']}\n")
            out.write(f"Total decisions: {len(steps)}\n")
            out.write(f"{'='*70}\n\n")

            global_step = 0
            local_step = 0
            for i, s in enumerate(steps):
                alts = s["alternatives"]
                idx = s["index"]
                nonfifo = " ** NON-FIFO **" if idx != 0 else ""

                if s["kind"] == "global":
                    global_step += 1
                    # Show what was chosen and what alternatives existed
                    chosen = s.get("chosen_id", "?")
                    fr, to, mt = s.get("from", "?"), s.get("to", "?"), s.get("msg_type", "?")
                    out.write(f"Step {i:3d}: [GLOBAL #{global_step}]  "
                              f"Deliver {fr}→{to}({mt})  "
                              f"[chose {idx} of {alts}]{nonfifo}\n")
                else:
                    local_step += 1
                    node = s.get("node", "?")
                    bgid = s.get("chosen_bgid", 0)
                    runq = s.get("runq_bgids", [])
                    runq_str = ", ".join(f"B{b}" for b in runq) if runq else "?"
                    out.write(f"Step {i:3d}: [LOCAL  {node}]     "
                              f"Run B{bgid}  "
                              f"from [{runq_str}]  "
                              f"[chose {idx} of {alts}]{nonfifo}\n")

            out.write(f"\n{'='*70}\n")
            out.write(f"RESULT: BUG FOUND on run {bug_run['run_num']}\n")
            out.write(f"  Global decisions: {global_step}\n")
            out.write(f"  Local decisions:  {local_step}\n")
            non_fifo = sum(1 for s in steps if s["index"] != 0)
            out.write(f"  Non-FIFO choices: {non_fifo}\n")

            # Highlight the non-FIFO decisions (the ones that matter).
            if non_fifo > 0:
                out.write(f"\nKey non-FIFO decisions:\n")
                for i, s in enumerate(steps):
                    if s["index"] == 0:
                        continue
                    if s["kind"] == "global":
                        fr, to, mt = s.get("from", "?"), s.get("to", "?"), s.get("msg_type", "?")
                        out.write(f"  Step {i}: GLOBAL  Deliver {fr}→{to}({mt})  "
                                  f"[index {s['index']} of {s['alternatives']}]\n")
                    else:
                        node = s.get("node", "?")
                        bgid = s.get("chosen_bgid", 0)
                        runq = s.get("runq_bgids", [])
                        runq_str = ", ".join(f"B{b}" for b in runq)
                        out.write(f"  Step {i}: LOCAL   node={node}  "
                                  f"B{bgid} from [{runq_str}]  "
                                  f"[index {s['index']} of {s['alternatives']}]\n")

            out.write(f"{'='*70}\n")

        print(f"  narrative_{policy}.txt")


# ── Figure 8: Detailed trace timeline with RPC labels ──

def fig_detailed_trace(data_dir, out_dir):
    """Timeline showing each decision with actual RPC names / goroutine IDs."""
    for f in find_files(data_dir, "-trace.jsonl"):
        records = load_jsonl(f)
        bug_run = None
        for r in records:
            if not r["passed"]:
                bug_run = r
                break
        if bug_run is None:
            continue

        policy = bug_run.get("policy", f.stem.replace("-trace", ""))
        steps = bug_run["steps"]
        if len(steps) < 2:
            continue

        fig, ax = plt.subplots(figsize=(max(14, len(steps) * 0.3), 5))

        # Assign y position by node (global = top, then nodes alphabetically).
        nodes = sorted(set(s.get("node", "") for s in steps if s["kind"] == "local"))
        y_map = {"": len(nodes)}  # global at top
        for i, n in enumerate(nodes):
            y_map[n] = i

        xs, ys, colors, sizes, labels = [], [], [], [], []
        for i, s in enumerate(steps):
            node = s.get("node", "") if s["kind"] == "local" else ""
            xs.append(i)
            ys.append(y_map.get(node, 0))
            alts = s["alternatives"]
            sizes.append(max(20, alts * 20))

            if s["index"] != 0:
                colors.append('#f44336')  # red for non-FIFO
            elif s["kind"] == "global":
                colors.append('#2196F3')  # blue
            else:
                colors.append('#FF9800')  # orange

            # Label: RPC name for global, BGID for local (only for non-trivial)
            if s["kind"] == "global":
                mt = s.get("msg_type", "")
                fr = s.get("from", "")
                to = s.get("to", "")
                labels.append(f"{fr}→{to}\n({mt})" if alts > 1 else "")
            else:
                if alts > 1:
                    labels.append(f"B{s.get('chosen_bgid', '?')}")
                else:
                    labels.append("")

        ax.scatter(xs, ys, c=colors, s=sizes, alpha=0.7, edgecolors='#333', linewidth=0.3, zorder=2)

        # Add labels for important decisions.
        for i, label in enumerate(labels):
            if label and steps[i]["alternatives"] > 1:
                ax.annotate(label, (xs[i], ys[i]), textcoords="offset points",
                           xytext=(0, 12), ha='center', fontsize=6, rotation=45)

        yticks = list(range(len(nodes))) + [len(nodes)]
        yticklabels = [f"node {n}" for n in nodes] + ["Global"]
        ax.set_yticks(yticks)
        ax.set_yticklabels(yticklabels)
        ax.set_xlabel("Decision step")
        ax.set_title(f"Detailed Trace — {policy} (run {bug_run['run_num']}, "
                     f"{len(steps)} decisions)")
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)

        # Legend.
        from matplotlib.lines import Line2D
        legend_elements = [
            Line2D([0], [0], marker='o', color='w', markerfacecolor='#2196F3',
                   markersize=8, label='Global (FIFO)'),
            Line2D([0], [0], marker='o', color='w', markerfacecolor='#FF9800',
                   markersize=8, label='Local (FIFO)'),
            Line2D([0], [0], marker='o', color='w', markerfacecolor='#f44336',
                   markersize=8, label='Non-FIFO choice'),
        ]
        ax.legend(handles=legend_elements, loc='upper right', fontsize=7)

        fig.tight_layout()
        fig.savefig(os.path.join(out_dir, f"detailed_{policy}.png"), dpi=150)
        plt.close(fig)
        print(f"  detailed_{policy}.png")


# ── Main ──

def main():
    parser = argparse.ArgumentParser(description="Synctest exploration charts")
    parser.add_argument("--data", default="charts/data", help="Input data directory")
    parser.add_argument("--out", default="charts/figures", help="Output figures directory")
    args = parser.parse_args()

    os.makedirs(args.out, exist_ok=True)
    print(f"Reading from {args.data}, writing to {args.out}")

    print("\nFigure 1: Runs to first bug")
    fig_runs_to_bug(args.data, args.out)

    print("\nFigure 2: Decision composition per run")
    fig_decision_composition(args.data, args.out)

    print("\nFigure 3: Bug-finding trace timeline")
    fig_bug_trace(args.data, args.out)

    print("\nFigure 4: CHESS exploration tree")
    fig_chess_tree(args.data, args.out)

    print("\nFigure 5: Branch points per run")
    fig_branch_points(args.data, args.out)

    print("\nFigure 6: Message type distribution")
    fig_message_types(args.data, args.out)

    print("\nFigure 7: Narrative trace (text)")
    fig_narrative_trace(args.data, args.out)

    print("\nFigure 8: Detailed trace with RPC labels")
    fig_detailed_trace(args.data, args.out)

    print(f"\nDone. Figures in {args.out}/")


if __name__ == "__main__":
    main()
