#!/usr/bin/env python3
"""Cross-cutting charts combining GoBench and distributed evaluation data.

Usage:
    python3 charts/cross_cutting.py

Produces:
  1. unified_bug_matrix.png   — heatmap of all bugs × strategies
  2. strategy_comparison.md/png — strategy summary table
  3. runtime_overhead.png      — wall-clock time per run
"""

import json
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import numpy as np

ROOT = Path(__file__).resolve().parent.parent
FIG_DIR = Path(__file__).parent / "figures"

# ── Data sources ──

GOBENCH_DATA = ROOT / "bugs" / "gobench" / "charts" / "data"
RAGATE_DATA = ROOT / "bugs" / "ra-gate" / "charts" / "data"
RASTALE_DATA = ROOT / "bugs" / "ra-stale-reply" / "benchmarking" / "data"

# Bug definitions: (bug_id, data_dir, strategies_map, group)
# strategies_map: {display_name: file_stem}

GOBENCH_BUGS = [
    "grpc1353", "k8s6632", "etcd7902", "moby33781", "etcd7443",
    "etcd6873", "grpc1460", "istio16224", "k8s10182", "k8s26980",
    "moby28462", "k8s1321", "etcd7492", "serving2137",
]

GOBENCH_STRATEGIES = {
    "CHESS": "chess",
    "PCT-d2": "pct-d2",
    "PCT-d3": "pct-d3",
    "Random": "random",
}

DISTRIBUTED_STRATEGIES = {
    "CHESS G+L": "chess-gl",
    "CHESS G-only": "chess-global",
    "PCT-d2": "pct-d2",
    "PCT-d3": "pct-d3",
    "Random": "random",
}

DISTRIBUTED_BUGS = [
    ("ra-gate", RAGATE_DATA),
]

BUG_LABELS = {
    "grpc1353": "grpc#1353", "k8s6632": "k8s#6632", "etcd7902": "etcd#7902",
    "moby33781": "moby#33781", "etcd7443": "etcd#7443", "etcd6873": "etcd#6873",
    "grpc1460": "grpc#1460", "istio16224": "istio#16224", "k8s10182": "k8s#10182",
    "k8s26980": "k8s#26980", "moby28462": "moby#28462", "k8s1321": "k8s#1321",
    "etcd7492": "etcd#7492", "serving2137": "serving#2137",
    "ra-gate": "ra-gate", "ra-stale-reply": "ra-stale-reply",
}


def load_jsonl(path):
    if not path.exists() or path.stat().st_size == 0:
        return []
    records = []
    with open(path) as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))
    return records


def first_bug_run(records):
    for r in records:
        if not r.get("passed", True):
            return r.get("run_num", 0)
    return None


def load_gobench_summary(bug, strategy_stem):
    return load_jsonl(GOBENCH_DATA / f"{bug}-{strategy_stem}.jsonl")


def load_distributed_summary(data_dir, strategy_stem):
    return load_jsonl(data_dir / f"{strategy_stem}.jsonl")


# ── Chart 12: Unified Bug Matrix (Heatmap) ──

def fig_unified_bug_matrix():
    FIG_DIR.mkdir(parents=True, exist_ok=True)

    # Collect all strategies (union of gobench and distributed)
    all_strategies = list(GOBENCH_STRATEGIES.keys())
    for s in DISTRIBUTED_STRATEGIES:
        if s not in all_strategies:
            all_strategies.append(s)

    # Build matrix: rows = bugs, cols = strategies
    row_labels = []
    matrix = []
    group_boundaries = []  # index where distributed bugs start

    # GoBench bugs
    for bug in GOBENCH_BUGS:
        row_labels.append(BUG_LABELS.get(bug, bug))
        row = []
        for strat_name in all_strategies:
            stem = GOBENCH_STRATEGIES.get(strat_name)
            if stem is None:
                row.append(None)
                continue
            records = load_gobench_summary(bug, stem)
            fb = first_bug_run(records)
            row.append(fb)
        matrix.append(row)

    group_boundaries.append(len(row_labels))

    # Distributed bugs
    for bug_id, data_dir in DISTRIBUTED_BUGS:
        row_labels.append(BUG_LABELS.get(bug_id, bug_id))
        row = []
        for strat_name in all_strategies:
            stem = DISTRIBUTED_STRATEGIES.get(strat_name)
            if stem is None:
                # GoBench-only strategy (CHESS without G+L/G-only) — map to chess-gl
                stem = GOBENCH_STRATEGIES.get(strat_name)
                if stem == "chess":
                    row.append(None)  # no plain CHESS for distributed
                    continue
                elif stem is None:
                    row.append(None)
                    continue
            records = load_distributed_summary(data_dir, stem)
            fb = first_bug_run(records)
            row.append(fb)
        matrix.append(row)

    n_rows = len(row_labels)
    n_cols = len(all_strategies)

    # Build numeric matrix for coloring
    max_val = 1
    for row in matrix:
        for v in row:
            if v is not None:
                max_val = max(max_val, v)

    fig, ax = plt.subplots(figsize=(max(8, n_cols * 1.2), max(6, n_rows * 0.45)))

    # Custom colormap: green (few runs) to yellow (many runs)
    cmap = plt.cm.YlGn_r

    for i, row in enumerate(matrix):
        for j, val in enumerate(row):
            if val is None:
                # No data
                ax.add_patch(plt.Rectangle((j, i), 1, 1, fill=True,
                                           facecolor="white", edgecolor="#ddd",
                                           linewidth=0.5))
            elif val == 0:
                # Should not happen, but handle
                ax.add_patch(plt.Rectangle((j, i), 1, 1, fill=True,
                                           facecolor="#bdbdbd", edgecolor="#ddd",
                                           linewidth=0.5))
                ax.text(j + 0.5, i + 0.5, "X", ha="center", va="center",
                        fontsize=8, color="#666")
            else:
                # Found — color by run number
                norm_val = np.log1p(val) / np.log1p(max_val)
                color = cmap(norm_val)
                ax.add_patch(plt.Rectangle((j, i), 1, 1, fill=True,
                                           facecolor=color, edgecolor="#ddd",
                                           linewidth=0.5))
                ax.text(j + 0.5, i + 0.5, str(val), ha="center", va="center",
                        fontsize=8, fontweight="bold")

    # Mark "not found" — bugs with data but no bug found
    for i, row in enumerate(matrix):
        for j, val in enumerate(row):
            bug_idx = i
            strat = all_strategies[j]
            # Check if data exists but bug not found
            if val is None:
                # Check if this is a "no data" vs "not found" case
                if bug_idx < len(GOBENCH_BUGS):
                    stem = GOBENCH_STRATEGIES.get(strat)
                    if stem:
                        records = load_gobench_summary(GOBENCH_BUGS[bug_idx], stem)
                        if records and first_bug_run(records) is None:
                            ax.add_patch(plt.Rectangle((j, i), 1, 1, fill=True,
                                                       facecolor="#bdbdbd",
                                                       edgecolor="#ddd",
                                                       linewidth=0.5))
                            ax.text(j + 0.5, i + 0.5, "X", ha="center",
                                    va="center", fontsize=8, color="#666")

    # Group divider
    for boundary in group_boundaries:
        ax.axhline(y=boundary, color="#333", linewidth=1.5)

    ax.set_xlim(0, n_cols)
    ax.set_ylim(0, n_rows)
    ax.invert_yaxis()
    ax.set_xticks([j + 0.5 for j in range(n_cols)])
    ax.set_xticklabels(all_strategies, fontsize=9, rotation=30, ha="right")
    ax.set_yticks([i + 0.5 for i in range(n_rows)])
    ax.set_yticklabels(row_labels, fontsize=9)
    ax.set_title("Unified Bug Matrix: Runs to First Bug", fontsize=13,
                 fontweight="bold", pad=15)

    # Group labels on the right margin
    gb_mid = len(GOBENCH_BUGS) / 2
    dist_mid = len(GOBENCH_BUGS) + len(DISTRIBUTED_BUGS) / 2
    ax.text(n_cols + 0.3, gb_mid, "GoBench\nCh+Lock",
            ha="left", va="center", fontsize=9, color="#666", fontstyle="italic")
    ax.text(n_cols + 0.3, dist_mid, "Distributed\n(RA)",
            ha="left", va="center", fontsize=9, color="#666", fontstyle="italic")

    # Legend
    ax.text(0, n_rows + 0.8,
            "Cell = run # where bug first found (fewer = better). "
            "X = not found. White = no data.",
            fontsize=8, color="#666", transform=ax.transData)

    fig.tight_layout()
    fig.savefig(FIG_DIR / "unified_bug_matrix.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    print("  -> unified_bug_matrix.png")


# ── Chart 13: Strategy Comparison Summary Table ──

def fig_strategy_comparison_table():
    FIG_DIR.mkdir(parents=True, exist_ok=True)

    # Collect per-strategy stats across all bugs
    strategies = {
        "CHESS (k=3)": {"stems": {"gobench": "chess"}, "guarantee": "Complete to bound k"},
        "CHESS G+L (k=2)": {"stems": {"distributed": "chess-gl"}, "guarantee": "Complete to bound k"},
        "CHESS G-only (k=2)": {"stems": {"distributed": "chess-global"}, "guarantee": "Complete to bound k"},
        "PCT (d=2)": {"stems": {"gobench": "pct-d2", "distributed": "pct-d2"}, "guarantee": "Probabilistic"},
        "PCT (d=3)": {"stems": {"gobench": "pct-d3", "distributed": "pct-d3"}, "guarantee": "Probabilistic"},
        "Random": {"stems": {"gobench": "random", "distributed": "random"}, "guarantee": "None"},
    }

    results = {}
    for strat_name, info in strategies.items():
        gb_found = 0
        gb_total = 0
        dist_found = 0
        dist_total = 0
        all_runs = []
        all_times_ms = []

        gb_stem = info["stems"].get("gobench")
        if gb_stem:
            for bug in GOBENCH_BUGS:
                records = load_gobench_summary(bug, gb_stem)
                if not records:
                    continue
                gb_total += 1
                fb = first_bug_run(records)
                if fb is not None:
                    gb_found += 1
                    all_runs.append(fb)
                for r in records:
                    if "elapsed_ns" in r:
                        all_times_ms.append(r["elapsed_ns"] / 1e6)

        dist_stem = info["stems"].get("distributed")
        if dist_stem:
            for bug_id, data_dir in DISTRIBUTED_BUGS:
                records = load_distributed_summary(data_dir, dist_stem)
                if not records:
                    continue
                dist_total += 1
                fb = first_bug_run(records)
                if fb is not None:
                    dist_found += 1
                    all_runs.append(fb)
                for r in records:
                    if "elapsed_ns" in r:
                        all_times_ms.append(r["elapsed_ns"] / 1e6)

        avg_runs = f"{np.mean(all_runs):.1f}" if all_runs else "--"
        worst = str(max(all_runs)) if all_runs else "--"
        avg_time = f"{np.mean(all_times_ms):.2f}" if all_times_ms else "--"
        total_time = f"{np.sum(all_times_ms):.0f}" if all_times_ms else "--"

        results[strat_name] = {
            "guarantee": info["guarantee"],
            "gb": f"{gb_found}/{gb_total}" if gb_total else "--",
            "dist": f"{dist_found}/{dist_total}" if dist_total else "--",
            "avg": avg_runs,
            "worst": worst,
            "avg_time": avg_time,
            "total_time": total_time,
        }

    # Load seed sweep for multi-seed stats on etcd#7443
    seed_sweep_path = Path(__file__).parent / "data" / "seed-sweep.json"
    seed_stats = {}  # {strategy: {best, med, worst, avg_ms_per_run}}
    if seed_sweep_path.exists():
        sweep = json.loads(seed_sweep_path.read_text())
        # Get avg ms/run from etcd7443 data for time estimates
        avg_ms_per_run = {}
        for strat_key in ["random", "pct-d2", "pct-d3"]:
            p = GOBENCH_DATA / f"etcd7443-{strat_key}.jsonl"
            if p.exists() and p.stat().st_size > 0:
                times = []
                for line in p.read_text().splitlines():
                    if line.strip():
                        r = json.loads(line)
                        if "elapsed_ns" in r:
                            times.append(r["elapsed_ns"] / 1e6)
                if times:
                    avg_ms_per_run[strat_key] = float(np.mean(times))

        for strat_key in ["random", "pct-d2", "pct-d3"]:
            vals = sorted([r["first_bug"] for r in sweep
                           if r["strategy"] == strat_key and r["first_bug"] > 0])
            if vals:
                ms_per = avg_ms_per_run.get(strat_key, 0.025)
                seed_stats[strat_key] = {
                    "best": vals[0],
                    "med": int(np.median(vals)),
                    "worst": vals[-1],
                    "p95": int(np.percentile(vals, 95)),
                    "ms_per": ms_per,
                }

    # Map strategy names to seed sweep keys
    seed_map = {"PCT (d=2)": "pct-d2", "PCT (d=3)": "pct-d3", "Random": "random"}

    # Markdown output
    md_lines = [
        "| Strategy | Guarantees | GoBench | Distributed | Avg Runs | Worst | ms/run | Total ms |",
        "|----------|-----------|---------|-------------|----------|-------|--------|----------|",
    ]
    for strat_name, r in results.items():
        md_lines.append(
            f"| {strat_name} | {r['guarantee']} | {r['gb']} "
            f"| {r['dist']} | {r['avg']} | {r['worst']} "
            f"| {r['avg_time']} | {r['total_time']} |"
        )
    if seed_stats:
        md_lines.append("")
        md_lines.append("**etcd#7443 seed sweep (100 seeds) — the hard bug:**")
        md_lines.append("")
        md_lines.append("| Strategy | Seed | Runs to Bug | Est. Time |")
        md_lines.append("|----------|------|-------------|-----------|")
        md_lines.append(f"| CHESS (k=3) | deterministic | 74 | {74*0.025:.1f}ms |")
        for strat_key in ["random", "pct-d2", "pct-d3"]:
            ss = seed_stats[strat_key]
            label = {"random": "Random", "pct-d2": "PCT(d=2)", "pct-d3": "PCT(d=3)"}[strat_key]
            for tag, val in [("best", ss["best"]), ("median", ss["med"]), ("worst", ss["worst"])]:
                est_ms = val * ss["ms_per"]
                md_lines.append(f"| {label} | {tag} | {val} | {est_ms:.1f}ms |")

    md_text = "\n".join(md_lines)
    with open(FIG_DIR / "strategy_comparison.md", "w") as f:
        f.write(md_text + "\n")
    print("  -> strategy_comparison.md")

    # Matplotlib table figure — two tables stacked
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 5.5),
                                    gridspec_kw={"height_ratios": [6, 7]})

    # Table 1: Main strategy comparison
    ax1.axis("off")

    # Table 1: main strategy comparison
    col_labels = ["Strategy", "Guarantees", "GoBench", "Distributed", "Avg Runs", "Worst", "ms/run", "Total ms"]
    cell_text = []
    cell_colors = []
    n_cols = len(col_labels)
    for strat_name, r in results.items():
        cell_text.append([strat_name, r["guarantee"], r["gb"],
                          r["dist"], r["avg"], r["worst"],
                          r["avg_time"], r["total_time"]])
        if "Complete" in r["guarantee"]:
            cell_colors.append(["#e3f2fd"] * n_cols)
        elif "Probabilistic" in r["guarantee"]:
            cell_colors.append(["#fff3e0"] * n_cols)
        else:
            cell_colors.append(["#fafafa"] * n_cols)

    t1 = ax1.table(cellText=cell_text, colLabels=col_labels,
                    cellColours=cell_colors, loc="center",
                    colColours=["#e0e0e0"] * n_cols)
    t1.auto_set_font_size(False)
    t1.set_fontsize(9)
    t1.scale(1.0, 1.4)
    ax1.set_title("Strategy Comparison Summary", fontsize=13,
                   fontweight="bold", pad=15)

    # Table 2: seed sweep for etcd#7443 — best & worst with time
    ax2.axis("off")
    if seed_stats:
        col2 = ["Strategy", "Seed", "Runs to Bug"]
        rows2 = []
        colors2 = []
        rows2.append(["CHESS (k=3)", "deterministic", "74"])
        colors2.append(["#e3f2fd"] * 3)

        for strat_key in ["random", "pct-d2", "pct-d3"]:
            ss = seed_stats[strat_key]
            label = {"random": "Random", "pct-d2": "PCT(d=2)", "pct-d3": "PCT(d=3)"}[strat_key]
            for tag, val in [("best", ss["best"]), ("worst", ss["worst"])]:
                color = "#e8f5e9" if tag == "best" else "#ffebee"
                rows2.append([label, tag, str(val)])
                colors2.append([color] * 3)

        t2 = ax2.table(cellText=rows2, colLabels=col2,
                        cellColours=colors2, loc="center",
                        colColours=["#e0e0e0"] * 3)
        t2.auto_set_font_size(False)
        t2.set_fontsize(9)
        t2.scale(0.7, 1.3)
        ax2.set_title("etcd#7443: 100 seeds", fontsize=10, pad=8)

    fig.tight_layout()
    fig.savefig(FIG_DIR / "strategy_comparison.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    print("  -> strategy_comparison.png")


# ── Chart 14: Runtime Overhead ──

def fig_runtime_overhead():
    FIG_DIR.mkdir(parents=True, exist_ok=True)

    # Collect elapsed_ns per bug per strategy
    bug_data = {}  # {bug_label: {strategy: [elapsed_ms, ...]}}

    # GoBench bugs
    for bug in GOBENCH_BUGS:
        label = BUG_LABELS.get(bug, bug)
        bug_data[label] = {}
        for strat_name, stem in GOBENCH_STRATEGIES.items():
            records = load_gobench_summary(bug, stem)
            if records:
                times = [r["elapsed_ns"] / 1e6 for r in records if "elapsed_ns" in r]
                if times:
                    bug_data[label][strat_name] = times

    # Distributed bugs
    for bug_id, data_dir in DISTRIBUTED_BUGS:
        label = BUG_LABELS.get(bug_id, bug_id)
        bug_data[label] = {}
        for strat_name, stem in DISTRIBUTED_STRATEGIES.items():
            records = load_distributed_summary(data_dir, stem)
            if records:
                times = [r["elapsed_ns"] / 1e6 for r in records if "elapsed_ns" in r]
                if times:
                    bug_data[label][strat_name] = times

    # Filter to bugs that have at least one strategy with data
    bugs_with_data = [b for b in bug_data if bug_data[b]]
    if not bugs_with_data:
        print("  [skip] runtime_overhead.png (no elapsed_ns data)")
        return

    all_strats = list(GOBENCH_STRATEGIES.keys())
    for s in DISTRIBUTED_STRATEGIES:
        if s not in all_strats:
            all_strats.append(s)

    strat_colors = {
        "CHESS": "#2196F3",
        "CHESS G+L": "#1565C0",
        "CHESS G-only": "#64B5F6",
        "PCT-d2": "#FF9800",
        "PCT-d3": "#4CAF50",
        "Random": "#9C27B0",
    }

    # Separate GoBench and distributed for panel layout
    gb_bugs = [b for b in bugs_with_data if b not in ("ra-gate", "ra-stale-reply")]
    dist_bugs = [b for b in bugs_with_data if b in ("ra-gate", "ra-stale-reply")]

    n_panels = (1 if gb_bugs else 0) + (1 if dist_bugs else 0)
    if n_panels == 0:
        print("  [skip] runtime_overhead.png (no data)")
        return

    fig, axes = plt.subplots(1, n_panels, figsize=(max(10, len(bugs_with_data) * 0.8), 5),
                             squeeze=False)
    ax_idx = 0

    for panel_bugs, panel_title in [(gb_bugs, "GoBench"), (dist_bugs, "Distributed")]:
        if not panel_bugs:
            continue
        ax = axes[0][ax_idx]
        ax_idx += 1

        x = np.arange(len(panel_bugs))
        width = 0.8 / len(all_strats)

        for i, strat in enumerate(all_strats):
            means = []
            errs_lo = []
            errs_hi = []
            for bug in panel_bugs:
                times = bug_data.get(bug, {}).get(strat, [])
                if times:
                    m = np.mean(times)
                    means.append(m)
                    errs_lo.append(m - np.min(times))
                    errs_hi.append(np.max(times) - m)
                else:
                    means.append(0)
                    errs_lo.append(0)
                    errs_hi.append(0)

            if all(m == 0 for m in means):
                continue

            bars = ax.bar(x + i * width, means, width,
                          label=strat, color=strat_colors.get(strat, "#999"),
                          edgecolor="white", linewidth=0.3)
            ax.errorbar(x + i * width + width / 2, means,
                        yerr=[errs_lo, errs_hi],
                        fmt="none", ecolor="#333", elinewidth=0.5, capsize=2)

        ax.set_xticks(x + width * len(all_strats) / 2)
        ax.set_xticklabels(panel_bugs, fontsize=8, rotation=45, ha="right")
        ax.set_ylabel("Wall-clock time per run (ms)")
        ax.set_title(panel_title, fontsize=11)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    # Single legend for all panels
    handles, labels = axes[0][0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper right", fontsize=8, ncol=2)

    fig.suptitle("Runtime per Exploration Run", fontsize=13, fontweight="bold")
    fig.tight_layout(rect=[0, 0, 1, 0.93])
    fig.savefig(FIG_DIR / "runtime_overhead.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    print("  -> runtime_overhead.png")


# ── Chart 15: Paired CHESS trees — G-only vs G+L (council-designed) ──

def fig_paired_trees():
    """Side-by-side CHESS trees for ra-gate: G-only (455 nodes, no bug) vs G+L (27 nodes, bug found)."""
    gl_path = RAGATE_DATA / "chess-gl-tree.json"
    go_path = RAGATE_DATA / "chess-global-tree.json"
    if not gl_path.exists() or not go_path.exists():
        print("  [skip] paired_trees.png (missing tree data)")
        return

    gl_tree = json.loads(gl_path.read_text())
    go_tree = json.loads(go_path.read_text())

    FIG_DIR.mkdir(parents=True, exist_ok=True)
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    def draw_tree(ax, tree, title):
        children = {i: [] for i in range(len(tree))}
        for i, node in enumerate(tree):
            if node["Parent"] >= 0:
                children[node["Parent"]].append(i)

        # Find bug path
        bug_idx = None
        for i, node in enumerate(tree):
            if not node["Passed"]:
                bug_idx = i
                break
        bug_path = set()
        bug_edges = set()
        if bug_idx is not None:
            cur = bug_idx
            path_list = [cur]
            while tree[cur]["Parent"] >= 0:
                parent = tree[cur]["Parent"]
                bug_edges.add((parent, cur))
                cur = parent
                path_list.append(cur)
            bug_path = set(path_list)

        # Layout
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

        import sys
        old_limit = sys.getrecursionlimit()
        sys.setrecursionlimit(max(old_limit, len(tree) + 100))
        layout(0, 0)
        sys.setrecursionlimit(old_limit)

        # Depth info
        depth_counts = {}
        for node in tree:
            d = node["NonFIFO"]
            depth_counts[d] = depth_counts.get(d, 0) + 1

        # Depth bands
        band_colors = ["#f5f5f5", "#eaeaea"]
        for i, d in enumerate(sorted(depth_counts.keys())):
            ax.axhspan(-d - 0.4, -d + 0.4, color=band_colors[i % 2], zorder=0)

        # Edges
        for i, node in enumerate(tree):
            if node["Parent"] >= 0:
                px, py = x_pos[node["Parent"]], y_pos[node["Parent"]]
                cx, cy = x_pos[i], y_pos[i]
                if (node["Parent"], i) in bug_edges:
                    continue
                ax.plot([px, cx], [py, cy], color="#ccc", linewidth=0.3, zorder=1)

        # Bug path edges
        for (p, c) in bug_edges:
            ax.plot([x_pos[p], x_pos[c]], [y_pos[p], y_pos[c]],
                    color="#e53935", linewidth=2.5, zorder=3)

        # Nodes
        for i, node in enumerate(tree):
            if not node["Passed"]:
                color, size, marker = "#e53935", 60, "X"
            elif i in bug_path:
                color, size, marker = "#e53935", 25, "o"
            else:
                color, size, marker = "#4CAF50", 6, "o"
            ax.scatter(x_pos[i], y_pos[i], c=color, s=size, marker=marker,
                       zorder=4 if i in bug_path else 2, edgecolors="none")

        n_passed = sum(1 for n in tree if n["Passed"])
        n_failed = len(tree) - n_passed
        ax.set_title(title, fontsize=11, fontweight="bold")
        ax.set_ylabel("Context depth k")
        ax.set_yticks([-d for d in sorted(depth_counts.keys())])
        ax.set_yticklabels([f"k={d}" for d in sorted(depth_counts.keys())])
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.set_xlabel(f"{len(tree)} runs, {n_failed} bug{'s' if n_failed != 1 else ''} found")

    draw_tree(ax1, go_tree, "Global-only (k=2): bug NOT found")
    draw_tree(ax2, gl_tree, "Global+Local (k=2): bug found at run 27")

    fig.suptitle("ra-gate: Why local scheduling matters",
                 fontsize=13, fontweight="bold", y=1.02)
    fig.tight_layout()
    fig.savefig(FIG_DIR / "paired_trees_ragate.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    print("  -> paired_trees_ragate.png")


if __name__ == "__main__":
    print("Generating cross-cutting charts...")
    fig_unified_bug_matrix()
    fig_strategy_comparison_table()
    fig_runtime_overhead()
    print("Done.")
