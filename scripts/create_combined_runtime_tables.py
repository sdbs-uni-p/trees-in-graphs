#!/usr/bin/env python3
"""Create combined AGE/Kuzu/Neo4j runtime tables as a four-page PDF."""

import argparse
import csv
import html
import math
import shutil
import statistics
import subprocess
import tempfile
from collections import defaultdict
from pathlib import Path

from create_runtime_tables import speedup_color


METHODS = ("baseline", "dewey", "prepost")
SYSTEMS = ("Apache AGE", "Kuzu", "Neo4j")
QUERIES = (
    "01_all_descendants",
    "02_all_children",
    "05_all_leaves",
    "11_check_if_ancestor",
)
QUERY_LABELS = {
    "01_all_descendants": "Query 01 - All Descendants",
    "02_all_children": "Query 02 - All Children",
    "05_all_leaves": "Query 05 - All Leaves",
    "11_check_if_ancestor": "Query 11 - Check if Ancestor",
}
SCENARIOS = {
    "01_all_descendants": (
        ("root", "Root of largest tree", ("q01_q02", "q01")),
        ("leaf", "Leaf parent", ("q04", "q03_q04")),
    ),
    "02_all_children": (
        ("high", "Highest-degree node", ("q05",)),
        ("low", "Lowest-degree node", ("q06",)),
    ),
    "05_all_leaves": (
        ("root", "Root of largest tree", ("q01_q02", "q01")),
        ("leaf", "Leaf parent", ("q04", "q03_q04")),
    ),
    "11_check_if_ancestor": (
        ("far", "Root-farthest-leaf pair", ("q07",)),
        ("deep", "Deepest parent-leaf pair", ("q08",)),
    ),
}


def split_graph(graph):
    for method in METHODS:
        suffix = "_" + method
        if graph.endswith(suffix):
            return graph[: -len(suffix)], method
    raise ValueError(f"Unknown representation: {graph}")


def graph_label(graph):
    prefixes = {
        "artificial_forests_": "F",
        "artificial_trees_truebase_": "NT",
        "artificial_trees_ultratall_": "DT",
        "artificial_trees_ultrawide_": "WT",
    }
    for prefix, abbreviation in prefixes.items():
        if graph.startswith(prefix):
            return abbreviation + graph[len(prefix) :]
    return {
        "snb_sf1_comment": "SNB/C",
        "snb_sf1_place": "SNB/P",
        "snb_sf1_tagclass": "SNB/T",
    }[graph]


def graph_sort_key(graph):
    label = graph_label(graph)
    if label.startswith("SNB/"):
        return 4, {"SNB/C": 0, "SNB/P": 1, "SNB/T": 2}[label]
    for prefix, order in (("F", 0), ("NT", 1), ("DT", 2), ("WT", 3)):
        if label.startswith(prefix):
            return order, int(label[len(prefix) :])
    raise ValueError(label)


def load_medians(path):
    values = defaultdict(list)
    with path.open(newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            graph, method = split_graph(row["graph"].strip())
            runtime = row["runtime_ms"].strip()
            values[(graph, row["query"], row["scenario"], method)].append(
                None if not runtime else float(runtime)
            )
    medians = {}
    for key, runs in values.items():
        if len(runs) != 5:
            raise ValueError(f"Expected five runs for {key}, found {len(runs)}")
        if any(value is None for value in runs):
            if not all(value is None for value in runs):
                raise ValueError(f"Partial timeout for {key}")
            medians[key] = None
        else:
            medians[key] = statistics.median(runs)
    return medians


def select_scenario(medians, graph, query, method, candidates):
    for scenario in candidates:
        key = (graph, query, scenario, method)
        if key in medians:
            return scenario
    raise KeyError(f"No scenario {candidates} for {graph}, {query}, {method}")


def format_runtime(value):
    return ">6 h" if value is None else f"{value:,.3f}"


def format_speedup(baseline, method):
    if baseline is None and method is None:
        return "–"
    if method is None:
        return "–"
    if baseline is None:
        return f">{6 * 60 * 60 * 1000 / method:,.3f}x"
    return f"{baseline / method:,.3f}x"


def make_rows(system_medians, query):
    graphs = set.intersection(
        *({key[0] for key in medians if key[1] == query} for medians in system_medians.values())
    )
    rows = []
    for graph in sorted(graphs, key=graph_sort_key):
        for _, parameter_label, candidates in SCENARIOS[query]:
            row = [graph_label(graph), parameter_label]
            for system in SYSTEMS:
                medians = system_medians[system]
                scenario = select_scenario(medians, graph, query, "baseline", candidates)
                times = {
                    method: medians[(graph, query, scenario, method)]
                    for method in METHODS
                }
                row.extend(
                    (
                        format_runtime(times["baseline"]),
                        format_runtime(times["dewey"]),
                        format_runtime(times["prepost"]),
                        format_speedup(times["baseline"], times["dewey"]),
                        format_speedup(times["baseline"], times["prepost"]),
                    )
                )
            rows.append(row)
    return rows


def numeric_speedup(text):
    if text == "–":
        return None
    return float(text.lstrip(">").rstrip("x").replace(",", ""))


def svg_page(title, rows):
    width, height = 1191, 842
    left, top = 30, 52
    header_height, subheader_height, row_height = 21, 19, 15
    widths = [50, 126] + [62, 62, 62, 68, 68] * 3
    table_width = sum(widths)
    xs = [left]
    for value in widths:
        xs.append(xs[-1] + value)
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="420mm" height="297mm" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        '<g font-family="Arial, Helvetica, sans-serif" fill="#111">',
        f'<text x="{width / 2}" y="30" text-anchor="middle" font-size="16" font-weight="bold">{html.escape(title)}</text>',
    ]
    total_header = header_height + subheader_height
    parts.append(
        f'<rect x="{left}" y="{top}" width="{table_width}" height="{total_header}" '
        'fill="#eeeeee" stroke="#111"/>'
    )
    for index, heading in enumerate(("Graph", "Parameters")):
        center = (xs[index] + xs[index + 1]) / 2
        parts.append(
            f'<text x="{center}" y="{top + 25}" text-anchor="middle" font-size="8.2">{heading}</text>'
        )
    subheadings = (
        "Baseline (ms)",
        "Dewey (ms)",
        "Prepost (ms)",
        "Speedup Dewey",
        "Speedup Prepost",
    )
    for system_index, system in enumerate(SYSTEMS):
        start_column = 2 + system_index * 5
        group_left, group_right = xs[start_column], xs[start_column + 5]
        parts.append(
            f'<text x="{(group_left + group_right) / 2}" y="{top + 14}" '
            f'text-anchor="middle" font-size="9" font-weight="bold">{system}</text>'
        )
        parts.append(
            f'<line x1="{group_left}" y1="{top + header_height}" x2="{group_right}" '
            f'y2="{top + header_height}" stroke="#333" stroke-width="0.6"/>'
        )
        for offset, heading in enumerate(subheadings):
            column = start_column + offset
            parts.append(
                f'<text x="{(xs[column] + xs[column + 1]) / 2}" y="{top + 34}" '
                f'text-anchor="middle" font-size="7.7">{heading}</text>'
            )
    speedup_columns = {5, 6, 10, 11, 15, 16}
    previous_graph = None
    for row_index, row in enumerate(rows):
        y = top + total_header + row_index * row_height
        for column, value in enumerate(row):
            fill, foreground = "white", "#111"
            if column in speedup_columns:
                ratio = numeric_speedup(value)
                if ratio is not None:
                    fill = speedup_color(ratio)
                    red, green, blue = (int(fill[pos : pos + 2], 16) for pos in (1, 3, 5))
                    if 0.2126 * red + 0.7152 * green + 0.0722 * blue < 105:
                        foreground = "white"
            parts.append(
                f'<rect x="{xs[column]}" y="{y}" width="{widths[column]}" height="{row_height}" '
                f'fill="{fill}" stroke="#444" stroke-width="0.4"/>'
            )
            right = column >= 2
            x = xs[column + 1] - 3 if right else xs[column] + 3
            anchor = "end" if right else "start"
            weight = "bold" if value.startswith(">") else "normal"
            if right:
                # Preserve the original table renderer's fixed-width numeric
                # positioning instead of relying on proportional digits.
                advance = 4.15
                parts.append(
                    f'<g font-family="Adwaita Sans, Arial, sans-serif" '
                    f'font-weight="{weight}" fill="{foreground}">'
                )
                for character_index, character in enumerate(value):
                    character_x = x - (len(value) - character_index - 0.5) * advance
                    parts.append(
                        f'<text x="{character_x}" y="{y + 10.8}" '
                        f'text-anchor="middle" font-size="8.2">'
                        f'{html.escape(character)}</text>'
                    )
                parts.append("</g>")
            else:
                parts.append(
                    f'<text x="{x}" y="{y + 10.8}" text-anchor="{anchor}" '
                    f'font-size="8.2" font-weight="{weight}" '
                    f'fill="{foreground}">{html.escape(value)}</text>'
                )
        if previous_graph is not None and row[0] != previous_graph:
            parts.append(
                f'<line x1="{left}" y1="{y}" x2="{left + table_width}" y2="{y}" '
                'stroke="#111" stroke-width="2"/>'
            )
        previous_graph = row[0]
    bottom = top + total_header + len(rows) * row_height
    group_boundaries = (0, 1, 2, 7, 12, 17)
    for column, x in enumerate(xs):
        stroke_width = "1.2" if column in group_boundaries else "0.55"
        line_top = top if column in group_boundaries else top + header_height
        parts.append(
            f'<line x1="{x}" y1="{line_top}" x2="{x}" y2="{bottom}" '
            f'stroke="#222" stroke-width="{stroke_width}"/>'
        )
    has_runtime_timeout = any(">6 h" in value for row in rows for value in row)
    has_lower_bound = any(
        row[column].startswith(">")
        for row in rows
        for column in speedup_columns
    )
    has_double_timeout = any(
        row[column] == "–"
        for row in rows
        for column in speedup_columns
    )
    notes = ["Runtimes: median of five runs in milliseconds (ms)."]
    timeout_parts = []
    if has_runtime_timeout:
        timeout_parts.append('">6 h" denotes a runtime timeout')
    if has_lower_bound:
        timeout_parts.append('">" denotes a timeout-derived speedup lower bound')
    if has_double_timeout:
        timeout_parts.append('"–" means both methods timed out')
    if timeout_parts:
        notes.append("Timeouts: " + "; ".join(timeout_parts) + ".")
    notes.append(
        "Graphs: F = forest; NT = truebase; DT = ultratall; WT = ultrawide; "
        "SNB/C, SNB/P, SNB/T = SNB SF1 trees."
    )
    for index, note in enumerate(notes):
        parts.append(
            f'<text x="{left}" y="{bottom + 17 + index * 12}" '
            f'font-size="7.5">{html.escape(note)}</text>'
        )
    legend_y = bottom + 75
    label_width, bar_width, bar_height = 95, 360, 12
    bar_x = left + label_width
    parts.append(
        f'<text x="{left}" y="{legend_y + 1}" font-size="8" '
        'font-weight="bold">Speedup color:</text>'
    )
    slowdown_width, normal_width = 90, 160
    samples = 180
    for sample in range(samples):
        offset = bar_width * sample / samples
        if offset < slowdown_width:
            transformed = 1 - offset / slowdown_width
            ratio = 1 - 0.05 * (
                math.exp(transformed * math.log1p(1 / 0.05)) - 1
            )
        elif offset < slowdown_width + normal_width:
            transformed = (offset - slowdown_width) / normal_width
            ratio = math.exp(transformed * math.log(10))
        else:
            transformed = (
                (offset - slowdown_width - normal_width)
                / (bar_width - slowdown_width - normal_width)
            )
            ratio = 10 * math.exp(transformed * math.log(10))
        parts.append(
            f'<rect x="{bar_x + offset}" y="{legend_y - 9}" '
            f'width="{bar_width / samples + 0.5}" height="{bar_height}" '
            f'fill="{speedup_color(ratio)}"/>'
        )
    parts.append(
        f'<rect x="{bar_x}" y="{legend_y - 9}" width="{bar_width}" '
        f'height="{bar_height}" fill="none" stroke="#333" stroke-width="0.6"/>'
    )
    for position, label in (
        (0, "0x"),
        (slowdown_width, "1x"),
        (slowdown_width + normal_width, "10x"),
        (bar_width, "100x+"),
    ):
        anchor = "start" if position == 0 else "end" if position == bar_width else "middle"
        parts.append(
            f'<text x="{bar_x + position}" y="{legend_y + 16}" '
            f'text-anchor="{anchor}" font-size="7.5">{label}</text>'
        )
    parts.append(
        f'<text x="{bar_x + bar_width + 18}" y="{legend_y + 1}" font-size="7.7">'
        'red = slowdown; yellow = 1x; green = speedup; darker green = larger speedup</text>'
    )
    parts.extend(("</g>", "</svg>"))
    return "\n".join(parts)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--age", required=True, type=Path)
    parser.add_argument("--kuzu", required=True, type=Path)
    parser.add_argument("--neo4j", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    medians = {
        "Apache AGE": load_medians(args.age),
        "Kuzu": load_medians(args.kuzu),
        "Neo4j": load_medians(args.neo4j),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="combined-runtime-tables-") as temp_name:
        temp = Path(temp_name)
        page_pdfs = []
        for number, query in enumerate(QUERIES, start=1):
            rows = make_rows(medians, query)
            if len(rows) != 40:
                raise ValueError(f"Expected 40 rows for {query}, found {len(rows)}")
            svg = temp / f"page-{number}.svg"
            pdf = temp / f"page-{number}.pdf"
            svg.write_text(svg_page(QUERY_LABELS[query], rows), encoding="utf-8")
            subprocess.run(["rsvg-convert", "-f", "pdf", "-o", pdf, svg], check=True)
            page_pdfs.append(pdf)
        combined = temp / "runtime_tables.pdf"
        subprocess.run(["pdfunite", *page_pdfs, combined], check=True)
        shutil.copyfile(combined, args.output)
    print(args.output)


if __name__ == "__main__":
    main()
