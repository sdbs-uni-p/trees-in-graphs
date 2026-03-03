#!/usr/bin/env python3
"""
Benchmark report generator for Kuzu and Neo4j results.

Usage:
    python kuzu_report.py --raw-results <path-to-json-dir> --output <output-dir> [--gdms <name>]

Produces in <output-dir>/:
    results.csv          – median client-side runtime per graph / query / annotation
    speedup_heatmap.png  – dewey & prepost speedup over plain baseline
"""

import argparse
import json
import re
import statistics
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from matplotlib.patches import Rectangle

# ── Constants ──────────────────────────────────────────────────────────────────

TRAVERSAL_QUERIES = [
    "all_children", "all_descendants", "all_leaves",
    "count_descendants", "count_leaves", "all_ancestors",
]
POINT_QUERIES = [
    "check_if_ancestor_true", "check_if_ancestor_false",
    "check_same_subtree_true", "check_same_subtree_false",
]

Y_AXIS_LABELS = {
    "artificial_forest_40":    "Tiny Forest (40 Nodes)",
    "artificial_forest_1000":  "Tiny Forest (1,000 Nodes)",
    "truebase_10":             "Base Tree (10 Nodes)",
    "truebase_100":            "Base Tree (100 Nodes)",
    "truebase_1000":           "Base Tree (1,000 Nodes)",
    "truebase_10000":          "Base Tree (10,000 Nodes)",
    "truebase_100000":         "Base Tree (100,000 Nodes)",
    "ultrawide_10":            "Wide Tree (10 Nodes)",
    "ultrawide_100":           "Wide Tree (100 Nodes)",
    "ultrawide_1000":          "Wide Tree (1,000 Nodes)",
    "ultrawide_10000":         "Wide Tree (10,000 Nodes)",
    "ultrawide_100000":        "Wide Tree (100,000 Nodes)",
    "ultratall_10":            "Deep Tree (10 Nodes)",
    "ultratall_100":           "Deep Tree (100 Nodes)",
    "ultratall_1000":          "Deep Tree (1,000 Nodes)",
    "ultratall_10000":         "Deep Tree (10,000 Nodes)",
    "ultratall_100000":        "Deep Tree (100,000 Nodes)",
    "s_all_comment":           "SNB1 (Comment) (Full Graph)",
    "s_all_place":             "SNB2 (Place) (Full Graph)",
    "s_all_tagclass":          "SNB3 (Tagclass) (Full Graph)",
    "s1":                     "SNB1 (Comment) (Comment Nodes Only)",
    "s2":                     "SNB2 (Place) (Place Nodes Only)",
    "s3":                     "SNB3 (Tagclass) (Tagclass Nodes Only)",
}

QUERY_LABELS = {
    "all_children":             "all_children",
    "all_descendants":          "all_descendants",
    "all_leaves":               "all_leaves",
    "count_descendants":        "count_descendants",
    "count_leaves":             "count_leaves",
    "all_ancestors":            "all_ancestors",
    "check_if_ancestor_true":   "ancestor? (T)",
    "check_if_ancestor_false":  "ancestor? (F)",
    "check_same_subtree_true":  "same_subtree? (T)",
    "check_same_subtree_false": "same_subtree? (F)",
}

_Y_AXIS_ORDER = {name: i for i, name in enumerate(Y_AXIS_LABELS)}


# ── Analysis ───────────────────────────────────────────────────────────────────
def build_table(raw_results_dir: Path) -> pd.DataFrame:
    """Read all JSON result files and return a flat DataFrame of median runtimes."""
    data = []
    for j_file in raw_results_dir.iterdir():
        if not j_file.name.endswith(".json"):
            continue
        with j_file.open("r", encoding="utf-8") as f:
            json_data = json.load(f)
        for query_name, query_results in json_data.items():
            for annotation_method, result in query_results.items():
                if annotation_method == "run_info":
                    continue
                data.append({
                    "Graph Name":                j_file.stem,
                    "Query Name":                query_name,
                    "Annotation Method":         annotation_method,
                    "Median Client-Side Runtime": round(
                        statistics.median(result["time"]), 3
                    ),
                })
    return pd.DataFrame(data)


# ── Slowdown analysis ──────────────────────────────────────────────────────────
def build_slowdown_table(df: pd.DataFrame) -> pd.DataFrame:
    """Compute per-(graph, query, annotation) slowdown relative to baseline, sorted worst-first."""
    baseline = (
        df[df["Annotation Method"] == "baseline"]
        [["Graph Name", "Query Name", "Median Client-Side Runtime"]]
        .rename(columns={"Median Client-Side Runtime": "Baseline Runtime (ms)"})
    )
    annotated = df[df["Annotation Method"] != "baseline"].copy()
    merged = annotated.merge(baseline, on=["Graph Name", "Query Name"], how="inner")
    merged = merged[merged["Baseline Runtime (ms)"] != 0]
    merged["Slowdown"] = merged["Median Client-Side Runtime"] / merged["Baseline Runtime (ms)"]
    merged = merged.rename(columns={"Median Client-Side Runtime": "Annotated Runtime (ms)"})
    merged = merged.sort_values("Slowdown", ascending=False).reset_index(drop=True)
    merged.insert(0, "Rank", merged.index + 1)
    return merged[["Rank", "Slowdown", "Graph Name", "Query Name",
                    "Annotation Method", "Annotated Runtime (ms)", "Baseline Runtime (ms)"]]


# ── Visualization ──────────────────────────────────────────────────────────────
def _graph_sort_key(name):
    return _Y_AXIS_ORDER.get(name, len(_Y_AXIS_ORDER))


def _family(name):
    if name.startswith("s_all_") or re.match(r"^sf\d+$", name):
        return "snb"
    m = re.match(r"^(artificial_forest|truebase|ultrawide|ultratall)", name)
    return m.group(1) if m else name


def _pivot(df, method):
    sub = df[df["Annotation Method"] == method].copy()
    return sub.pivot_table(
        index="Graph Name", columns="Query Name",
        values="Median Client-Side Runtime",
    )


def plot_speedup_heatmap(df: pd.DataFrame, output_dir: Path, gdms: str = "Kuzu"):
    all_queries = TRAVERSAL_QUERIES + POINT_QUERIES
    base_pivot = _pivot(df, "baseline")
    graph_names = sorted(base_pivot.index.tolist(), key=_graph_sort_key)

    family_breaks = [
        i for i in range(1, len(graph_names))
        if _family(graph_names[i]) != _family(graph_names[i - 1])
    ]

    fig, axes = plt.subplots(
        1, 2,
        figsize=(18, max(6, len(graph_names) * 0.55 + 2)),
        sharey=True,
    )

    for ax, method in zip(axes, ["dewey", "prepost"]):
        m_pivot = _pivot(df, method)
        speedup = base_pivot[all_queries] / m_pivot[all_queries]
        mat = speedup.reindex(graph_names)

        log_mat = np.log2(mat.values)
        norm = mcolors.TwoSlopeNorm(vcenter=0.0, vmin=np.log2(0.3), vmax=np.log2(15))
        im = ax.imshow(log_mat, aspect="auto", cmap="RdYlGn", norm=norm)

        for r in range(mat.shape[0]):
            for c in range(mat.shape[1]):
                val = mat.values[r, c]
                if np.isnan(val):
                    continue
                color = "black" if 0.45 < val < 8 else "white"
                ax.text(c, r, f"{val:.1f}x", ha="center", va="center",
                        fontsize=6.5, color=color)
                if val <= 1.0:
                    ax.add_patch(Rectangle(
                        (c - 0.5, r - 0.5), 1, 1,
                        fill=False, hatch="////",
                        edgecolor="black", linewidth=0.0,
                    ))

        ax.set_xticks(range(len(all_queries)))
        ax.set_xticklabels(
            [QUERY_LABELS[q] for q in all_queries],
            rotation=40, ha="right", fontsize=8,
        )
        ax.set_yticks(range(len(graph_names)))
        ax.set_yticklabels(
            [Y_AXIS_LABELS.get(g, g) for g in graph_names], fontsize=8,
        )
        ax.set_title(f"{method.title()} speedup over baseline",
                     fontsize=15, fontweight="bold")

        for brk in family_breaks:
            ax.axhline(brk - 0.5, color="black", linewidth=3)

        cb_ticks = [0.3, 0.5, 1.0, 2.0, 4.0, 8.0, 15.0]
        cb = fig.colorbar(im, ax=ax, shrink=0.6, label="speedup (×)", pad=0.02)
        cb.set_ticks([np.log2(t) for t in cb_ticks])
        cb.set_ticklabels([f"{t}×" for t in cb_ticks])

    fig.suptitle(f"{gdms} – Annotation speedup over baseline",
                 fontsize=22, y=1.05, fontweight="bold")
    fig.text(0.5, 1.01, "green = faster,  red = slower,  yellow = no change",
             ha="center", va="bottom", fontsize=13, style="italic")
    plt.tight_layout()

    out = output_dir / "speedup_heatmap.png"
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Saved {out}")


# ── Entry point ────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Generate a Kuzu benchmark CSV and speedup heatmap from raw JSON results."
    )
    parser.add_argument(
        "--raw-results", required=True, metavar="DIR",
        help="Directory containing the raw JSON result files.",
    )
    parser.add_argument(
        "--output", required=True, metavar="DIR",
        help="Output directory to create (will be created if it does not exist).",
    )
    parser.add_argument(
        "--gdms", default="Kuzu", metavar="NAME",
        help="Database system name used in the heatmap title (default: Kuzu).",
    )
    args = parser.parse_args()

    raw_results_dir = Path(args.raw_results)
    output_dir = Path(args.output)

    if not raw_results_dir.is_dir():
        parser.error(f"--raw-results path does not exist or is not a directory: {raw_results_dir}")

    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Reading JSON results from: {raw_results_dir}")
    df = build_table(raw_results_dir)

    csv_path = output_dir / "results.csv"
    df.to_csv(csv_path, index=False)
    print(f"Saved {csv_path}")

    slowdown_path = output_dir / "slowdown.csv"
    build_slowdown_table(df).to_csv(slowdown_path, index=False)
    print(f"Saved {slowdown_path}")

    plot_speedup_heatmap(df, output_dir, gdms=args.gdms)
    print(f"Done. Output written to {output_dir}/")


if __name__ == "__main__":
    main()
