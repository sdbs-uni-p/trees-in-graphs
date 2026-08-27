#!/usr/bin/env python3
"""Create one selected Tree/LDBC comparison table for AGE, Kuzu, and Neo4j."""

import argparse
import shutil
import subprocess
import tempfile
from pathlib import Path

from create_combined_runtime_tables import (
    ANCESTOR_NEGATIVE_SCENARIOS,
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
    "Shallow sibling pair": "Siblings",
    "Most distant leaf pair": "Distant leaves",
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
            SCENARIOS["11_check_if_ancestor"] + ANCESTOR_NEGATIVE_SCENARIOS,
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


def content_widths(rows):
    headers = ("Graph", "Query", "Parameters") + ("B", "D", "P", "S_D", "S_P") * 3
    widths = []
    for column, header in enumerate(headers):
        longest = max(len(header), *(len(row[column]) for row in rows))
        if column == 0:
            # Graph labels contain comparatively wide uppercase glyphs (notably
            # the trailing K in WT100K), so give that column a little more room.
            character_width, padding, minimum = 5.2, 12, 42
        elif column < 3:
            character_width, padding, minimum = 4.6, 8, 34
        else:
            character_width, padding, minimum = 4.15, 8, 28
        widths.append(max(minimum, longest * character_width + padding))
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

    rows = selected_tree_rows(tree_medians) + selected_ldbc_rows(ldbc_medians)
    if len(rows) != 51:
        raise ValueError(f"Expected 51 selected rows, found {len(rows)}")

    leading_widths, metric_widths = content_widths(rows)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="combined-overview-") as temp_name:
        temp = Path(temp_name)
        table_output = args.table_output or args.output.with_name("runtime_table_only.pdf")
        table_output.parent.mkdir(parents=True, exist_ok=True)
        render_args = ("Selected Tree Queries and LDBC SNB SF1", rows)
        render_options = {
            "leading_headings": ("Graph", "Query", "Parameters"),
            "graph_notes": False,
            "leading_widths": leading_widths,
            "metric_widths": metric_widths,
            "subheadings": ("B", "D", "P", "S_D", "S_P"),
            "notes_override": (
                "Queries: Q_desc = All Descendants; Q_a&d = Check if Ancestor; "
                "IC12 = Interactive Complex 12; IS2 = Interactive Short 2; "
                "IS6 = Interactive Short 6.",
                "Parameters: Root = root of largest tree; Leaf parent = parent of a leaf; "
                "Root–leaf = root-farthest-leaf pair; Deep parent–leaf = deepest "
                "parent-leaf pair; Siblings = shallow sibling pair; "
                "Distant leaves = most distant leaf pair.",
                "Methods: B = Baseline; D = Dewey; P = PrePost; "
                "S_D = B / D; S_P = B / P. All times are in ms.",
                "Graphs: F = forest; WT = ultrawide tree; K = 1,000 nodes; "
                "SNB/C, SNB/P, SNB/T = SNB SF1 trees; SNB = full SNB SF1 graph.",
            ),
            "row_height": 12,
        }
        for name, output, table_only in (
            ("overview", args.output, False),
            ("table", table_output, True),
        ):
            svg = temp / f"{name}.svg"
            pdf = temp / f"{name}.pdf"
            svg.write_text(
                svg_page(
                    *render_args,
                    **render_options,
                    table_only=table_only,
                    fit_content=not table_only,
                ),
                encoding="utf-8",
            )
            subprocess.run(["rsvg-convert", "-f", "pdf", "-o", pdf, svg], check=True)
            shutil.copyfile(pdf, output)
    print(args.output)
    print(table_output)


if __name__ == "__main__":
    main()
