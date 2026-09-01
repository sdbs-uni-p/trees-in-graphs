#!/usr/bin/env python3
"""Create one selected Tree/LDBC comparison table for AGE, Kuzu, and Neo4j."""

import argparse
import math
import shutil
import subprocess
import tempfile
from pathlib import Path

from create_combined_runtime_tables import (
    SCENARIOS,
    SYSTEMS,
    load_ldbc_medians,
    load_medians,
    make_ldbc_rows,
    make_rows,
    svg_page,
)


TREE_GRAPHS = {
    "F1000",
    "WT100",
    "WT1000",
    "WT10000",
    "WT100000",
    "SNB/C",
    "SNB/P",
    "SNB/T",
}

QUERY_LABELS = {
    "All Descendants": "Q_desc",
    "Check if Ancestor": "Q_a&d",
}

PARAMETER_LABELS = {
    "Root of largest tree": "Root",
    "Leaf parent": "Leaf parent",
    "Root-farthest-leaf pair": "Root–leaf",
    "Deepest parent-leaf pair": "Deep parent–leaf",
}

LDBC_QUERY_LABELS = {
    "Interactive Complex 12": "IC12",
    "Interactive Short 2": "IS2",
    "Interactive Short 6": "IS6",
}


def compact_graph(graph):
    if graph.startswith("WT") and graph[2:].isdigit() and graph.endswith("000"):
        return graph[:-3] + "K"
    return graph


def selected_tree_rows(system_medians):
    selections = (
        ("01_all_descendants", "All Descendants", SCENARIOS["01_all_descendants"]),
        (
            "11_check_if_ancestor",
            "Check if Ancestor",
            SCENARIOS["11_check_if_ancestor"],
        ),
    )
    rows = []
    for query, query_label, scenarios in selections:
        for row in make_rows(system_medians, query, scenarios):
            if row[0] in TREE_GRAPHS:
                rows.append(
                    [
                        compact_graph(row[0]),
                        QUERY_LABELS[query_label],
                        PARAMETER_LABELS[row[1]],
                        *row[2:],
                    ]
                )
    return rows


def selected_ldbc_rows(system_medians):
    return [
        ["SNB", LDBC_QUERY_LABELS[row[1]], "–", *row[2:]]
        for row in make_ldbc_rows(system_medians)
    ]


def rounded_runtime(value):
    if value == ">6 h":
        return value
    return f"{float(value.replace(',', '')):,.1f}"


def rounded_speedup(value):
    if value == "–":
        return value
    lower_bound = value.startswith(">")
    number = float(value.lstrip(">").rstrip("x").replace(",", ""))
    prefix = ">" if lower_bound else ""
    if number < 1:
        rendered = f"{number:.2f}"
    elif number < 10:
        rendered = f"{number:.1f}"
    elif number < 100:
        rendered = f"{number:.0f}"
    elif number < 1000:
        rendered = f"{math.floor(number / 10 + 0.5) * 10:,.0f}"
    else:
        thousands = number / 1000
        if thousands < 10:
            rendered = f"{thousands:.1f}K"
        elif thousands < 100:
            rendered = f"{thousands:.0f}K"
        elif thousands < 1000:
            rendered = f"{math.floor(thousands / 10 + 0.5) * 10:,.0f}K"
        else:
            rendered = f"{math.floor(thousands / 100 + 0.5) * 100:,.0f}K"
    return f"{prefix}{rendered}x"


def rounded_rows(rows):
    result = []
    for row in rows:
        rounded = list(row[:3])
        for system in range(3):
            offset = 3 + system * 5
            rounded.extend(
                (
                    rounded_runtime(row[offset]),
                    rounded_runtime(row[offset + 1]),
                    rounded_runtime(row[offset + 2]),
                    rounded_speedup(row[offset + 3]),
                    rounded_speedup(row[offset + 4]),
                )
            )
        result.append(rounded)
    return result


def compact_rows(rows):
    result = []
    for row in rows:
        compact = list(row[:3])
        for system in range(3):
            offset = 3 + system * 5
            compact.extend((row[offset], row[offset + 3], row[offset + 4]))
        result.append(compact)
    return result


def content_widths(rows, subheadings):
    headers = ("Graph", "Query", "Parameters") + tuple(subheadings) * 3
    widths = []
    for column, header in enumerate(headers):
        if column == 0:
            # Graph labels contain comparatively wide uppercase glyphs (notably
            # the trailing K in WT100K), so give that column a little more room.
            character_width, padding, minimum = 5.2, 12, 42
        elif column < 3:
            character_width, padding, minimum = 4.6, 8, 34
        else:
            if len(subheadings) == 5:
                character_width, padding, minimum = 4.8, 14, 32
            else:
                character_width, padding, minimum = 4.4, 8, 28
        content_width = len(header) * character_width
        for row in rows:
            value = row[column]
            value_width = len(value) * character_width
            if column >= 3 and not value.endswith("x"):
                plain_number = value.lstrip(">").replace(",", "")
                try:
                    unusually_large_runtime = float(plain_number) >= 1_000_000
                except ValueError:
                    unusually_large_runtime = False
                if unusually_large_runtime:
                    value_width = len(value) * (
                        4.0 if len(subheadings) == 5 else 3.75
                    )
            content_width = max(content_width, value_width)
        widths.append(max(minimum, content_width + padding))
    return widths[:3], widths[3:]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--age", required=True, type=Path)
    parser.add_argument("--kuzu", required=True, type=Path)
    parser.add_argument("--neo4j", required=True, type=Path)
    parser.add_argument("--age-ldbc", required=True, type=Path)
    parser.add_argument("--kuzu-ldbc", required=True, type=Path)
    parser.add_argument("--neo4j-ldbc", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--rounded-output", type=Path)
    parser.add_argument("--table-output", type=Path)
    args = parser.parse_args()

    tree_medians = {
        "Apache AGE": load_medians(args.age),
        "Kuzu": load_medians(args.kuzu),
        "Neo4j": load_medians(args.neo4j),
    }
    ldbc_medians = {
        "Apache AGE": load_ldbc_medians(args.age_ldbc),
        "Kuzu": load_ldbc_medians(args.kuzu_ldbc),
        "Neo4j": load_ldbc_medians(args.neo4j_ldbc),
    }
    if set(tree_medians) != set(SYSTEMS) or set(ldbc_medians) != set(SYSTEMS):
        raise ValueError("Missing DBMS input")

    exact_rows = selected_tree_rows(tree_medians) + selected_ldbc_rows(ldbc_medians)
    if len(exact_rows) != 35:
        raise ValueError(f"Expected 35 selected rows, found {len(exact_rows)}")
    rounded = rounded_rows(exact_rows)
    compact = compact_rows(rounded)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="combined-overview-") as temp_name:
        temp = Path(temp_name)
        rounded_output = args.rounded_output or args.output.with_name(
            "runtime_tables_rounded.pdf"
        )
        table_output = args.table_output or args.output.with_name("runtime_table_only.pdf")
        rounded_output.parent.mkdir(parents=True, exist_ok=True)
        table_output.parent.mkdir(parents=True, exist_ok=True)
        common_options = {
            "leading_headings": ("Graph", "Query", "Parameters"),
            "graph_notes": False,
            "notes_override": (
                "Queries: Q_desc = All Descendants; Q_a&d = Check if Ancestor; "
                "IC12 = Interactive Complex 12; IS2 = Interactive Short 2; "
                "IS6 = Interactive Short 6.",
                "Parameters: Root = root of largest tree; Leaf parent = parent of a leaf; "
                "Root–leaf = root-farthest-leaf pair; Deep parent–leaf = deepest "
                "parent-leaf pair.",
                "Methods: B = Baseline; D = Dewey; P = PrePost; "
                "S_D = B / D; S_P = B / P. All times are in ms.",
                "Graphs: F = forest; WT = ultrawide tree; K = 1,000 nodes; "
                "SNB/C, SNB/P, SNB/T = SNB SF1 trees; SNB = full SNB SF1 graph.",
            ),
            # Keep the original total table height of 460 SVG units while
            # moving a little vertical space from the headers into body rows.
            "row_height": 12.3,
            "header_height": 15,
            "subheader_height": 14.5,
            "body_font_size": 9.1,
        }
        variants = (
            ("exact", args.output, exact_rows, ("B (ms)", "D", "P", "S_D", "S_P"), False),
            ("rounded", rounded_output, rounded, ("B (ms)", "D", "P", "S_D", "S_P"), False),
            ("compact", table_output, compact, ("B (ms)", "S_D", "S_P"), True),
        )
        for name, output, rows, subheadings, table_only in variants:
            leading_widths, metric_widths = content_widths(rows, subheadings)
            render_options = dict(common_options)
            if name == "rounded":
                render_options["notes_override"] = common_options["notes_override"] + (
                    "Rounding: runtimes use one decimal place; speedups use about two "
                    "significant digits and K denotes 1,000x.",
                )
            svg = temp / f"{name}.svg"
            pdf = temp / f"{name}.pdf"
            svg.write_text(
                svg_page(
                    "Selected Tree Queries and LDBC SNB SF1",
                    rows,
                    **render_options,
                    leading_widths=leading_widths,
                    metric_widths=metric_widths,
                    subheadings=subheadings,
                    table_only=table_only,
                    fit_content=not table_only,
                ),
                encoding="utf-8",
            )
            subprocess.run(["rsvg-convert", "-f", "pdf", "-o", pdf, svg], check=True)
            shutil.copyfile(pdf, output)
    print(args.output)
    print(rounded_output)
    print(table_output)


if __name__ == "__main__":
    main()
