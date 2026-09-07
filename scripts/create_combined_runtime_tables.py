#!/usr/bin/env python3
"""Create combined AGE/Kuzu/Neo4j runtime tables as a multi-page PDF."""

import argparse
import csv
import html
import math
import shutil
import subprocess
import tempfile
from collections import defaultdict
from pathlib import Path

from create_runtime_tables import (
    speedup_color, runtime_text, runtime_speedup, read_runtime, median_runtime,
    add_timeout_arguments, timeout_directories, TIMEOUT_NOTE, BOUND_NOTE,
    new_combined_directory, write_sources,
)


def render_pdf(svg, pdf):
    """Render with the installed DejaVu Serif fonts; reject silent substitution."""
    for style in ("Book", "Bold", "Italic", "Bold Italic"):
        family = subprocess.check_output(
            ["fc-match", "--format=%{family}", f"DejaVu Serif:style={style}"], text=True,
        ).strip()
        if "DejaVu Serif" not in family.split(","):
            raise ValueError(
                "DejaVu Serif is not available through Fontconfig. "
                "Install it (Debian/Ubuntu: fonts-dejavu-core) before generating PDFs."
            )
    subprocess.run(["rsvg-convert", "-f", "pdf", "-o", pdf, svg], check=True)


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

ANCESTOR_NEGATIVE_SCENARIOS = (
    ("shallow", "Shallow sibling pair", ("q09",)),
    ("distant", "Most distant leaf pair", ("q10",)),
)


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


def load_medians(path, log_directories=None):
    values = defaultdict(list)
    with path.open(newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            graph, method = split_graph(row["graph"].strip())
            values[(graph, row["query"], row["scenario"], method)].append(
                read_runtime(path, row, log_directories)
            )
    medians = {}
    for key, runs in values.items():
        if len(runs) != 5:
            raise ValueError(f"Expected five runs for {key}, found {len(runs)}")
        medians[key] = median_runtime(runs, key)
    return medians


def select_scenario(medians, graph, query, method, candidates):
    for scenario in candidates:
        key = (graph, query, scenario, method)
        if key in medians:
            return scenario
    raise KeyError(f"No scenario {candidates} for {graph}, {query}, {method}")


format_runtime = runtime_text
format_speedup = runtime_speedup


def make_rows(system_medians, query, scenarios=None):
    graphs = set.intersection(
        *({key[0] for key in medians if key[1] == query} for medians in system_medians.values())
    )
    rows = []
    for graph in sorted(graphs, key=graph_sort_key):
        for _, parameter_label, candidates in scenarios or SCENARIOS[query]:
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


def report_pages(system_medians):
    scenario_sets = [
        {(key[1], key[2]) for key in medians}
        for medians in system_medians.values()
    ]
    common_scenarios = set.intersection(*scenario_sets)
    all_scenarios = set.union(*scenario_sets)

    def supports(query, scenarios):
        return all(
            any((query, candidate) in common_scenarios for candidate in candidates)
            for _, _, candidates in scenarios
        )

    pages = []
    for query in QUERIES:
        scenarios = SCENARIOS[query]
        if supports(query, scenarios):
            title = QUERY_LABELS[query]
            if query == "11_check_if_ancestor" and any(
                pair[0] == query and pair[1] in {"q09", "q10"}
                for pair in all_scenarios
            ):
                title = "Query 11 - Check if Ancestor (Positive)"
            pages.append((title, query, scenarios))

    negative_query = "11_check_if_ancestor"
    negative_names = {"q09", "q10"}
    has_common_negative = any(
        query == negative_query and scenario in negative_names
        for query, scenario in common_scenarios
    )
    if has_common_negative:
        if not supports(negative_query, ANCESTOR_NEGATIVE_SCENARIOS):
            raise ValueError(
                "q09/q10 must be present in all AGE, Kuzu, and Neo4j inputs"
            )
        pages.append(
            (
                "Query 11 - Check if Ancestor (Negative)",
                negative_query,
                ANCESTOR_NEGATIVE_SCENARIOS,
            )
        )
    if not pages:
        raise ValueError("No common supported query scenarios found")
    return pages


def numeric_speedup(text):
    if text == "–":
        return None
    number = text.lstrip("><").rstrip("x").replace(",", "")
    multiplier = 1000 if number.endswith("K") else 1
    if multiplier != 1:
        number = number[:-1]
    return float(number) * multiplier


def svg_label(value):
    indexed = {
        "Q_desc": ("Q", "desc"),
        "Q_a&d": ("Q", "a&amp;d"),
        "S_D": ("S", "D"),
        "S_P": ("S", "P"),
    }
    if value not in indexed:
        return html.escape(value)
    symbol, index = indexed[value]
    symbol_style = ' font-style="italic"' if symbol == "Q" else ""
    index_style = ' font-style="italic"' if symbol == "Q" else ""
    return (
        f'<tspan{symbol_style}>{symbol}</tspan>'
        f'<tspan baseline-shift="sub" font-size="6"{index_style}>'
        f"{index}</tspan>"
    )


def svg_rich_text(value):
    rendered = html.escape(value)
    for token in ("Q_desc", "Q_a&d", "S_D", "S_P"):
        rendered = rendered.replace(html.escape(token), svg_label(token))
    return rendered


def svg_page(
    title,
    rows,
    leading_headings=("Graph", "Parameters"),
    graph_notes=True,
    leading_widths=None,
    metric_widths=None,
    subheadings=None,
    notes_override=None,
    row_height=15,
    header_height=21,
    subheader_height=19,
    body_font_size=8.2,
    table_only=False,
    fit_content=False,
    timeout_notes=(),
):
    if leading_widths is None:
        leading_widths = [50, 126]
    if metric_widths is None:
        metric_widths = [62, 62, 62, 68, 68]
    metrics_per_system = len(subheadings) if subheadings is not None else 5
    if len(metric_widths) == metrics_per_system:
        metric_widths = list(metric_widths) * 3
    elif len(metric_widths) != metrics_per_system * 3:
        raise ValueError(
            "metric_widths must contain one system group or all three groups"
        )
    has_timeout = any(value.startswith((">", "<")) for row in rows for value in row)
    widths = list(leading_widths) + list(metric_widths)
    table_width = sum(widths)
    total_header = header_height + subheader_height
    if table_only:
        left, top = 1, 1
        width = table_width + 2
        height = total_header + len(rows) * row_height + 2 + (15 + len(timeout_notes) * 12 if timeout_notes else 0)
        physical_size = f'width="{width}" height="{height}"'
    elif fit_content:
        left, top = 30, 52
        width = max(table_width + 60, 920)
        height = top + total_header + len(rows) * row_height + 130 + (60 if has_timeout else 0)
        physical_size = f'width="{width}" height="{height}"'
    else:
        width, height = 1191, 842
        left, top = 30, 52
        physical_size = 'width="420mm" height="297mm"'
    xs = [left]
    for value in widths:
        xs.append(xs[-1] + value)
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" {physical_size} viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        '<g font-family="DejaVu Serif" fill="#111">',
    ]
    if not table_only:
        parts.append(
            f'<text x="{left + table_width / 2}" y="30" text-anchor="middle" font-size="16" '
            f'font-weight="bold">{html.escape(title)}</text>'
        )
    parts.append(
        f'<rect x="{left}" y="{top}" width="{table_width}" height="{total_header}" '
        'fill="#eeeeee" stroke="#111"/>'
    )
    for index, heading in enumerate(leading_headings):
        center = (xs[index] + xs[index + 1]) / 2
        parts.append(
            f'<text x="{center}" y="{top + total_header / 2}" text-anchor="middle" '
            f'dominant-baseline="middle" font-size="8.2">{heading}</text>'
        )
    if subheadings is None:
        subheadings = (
            "Baseline (ms)",
            "Dewey (ms)",
            "Prepost (ms)",
            "Speedup Dewey",
            "Speedup Prepost",
        )
    leading_columns = len(leading_headings)
    for system_index, system in enumerate(SYSTEMS):
        start_column = leading_columns + system_index * metrics_per_system
        group_left = xs[start_column]
        group_right = xs[start_column + metrics_per_system]
        parts.append(
            f'<text x="{(group_left + group_right) / 2}" y="{top + header_height / 2}" '
            f'text-anchor="middle" dominant-baseline="middle" font-size="9" '
            f'font-weight="bold">{system}</text>'
        )
        parts.append(
            f'<line x1="{group_left}" y1="{top + header_height}" x2="{group_right}" '
            f'y2="{top + header_height}" stroke="#333" stroke-width="0.6"/>'
        )
        for offset, heading in enumerate(subheadings):
            column = start_column + offset
            parts.append(
                f'<text x="{(xs[column] + xs[column + 1]) / 2}" '
                f'y="{top + header_height + subheader_height / 2}" text-anchor="middle" '
                f'dominant-baseline="middle" font-size="7.7">{svg_label(heading)}</text>'
            )
    speedup_offsets = {
        offset
        for offset, heading in enumerate(subheadings)
        if heading in {"S_D", "S_P", "Speedup Dewey", "Speedup Prepost"}
    }
    speedup_columns = {
        leading_columns + system_index * metrics_per_system + offset
        for system_index in range(len(SYSTEMS))
        for offset in speedup_offsets
    }
    previous_graph = None
    for row_index, row in enumerate(rows):
        y = top + total_header + row_index * row_height
        for column, value in enumerate(row):
            fill, foreground = "white", "#111"
            if column in speedup_columns:
                ratio = numeric_speedup(value)
                if ratio is not None:
                    fill = "#d6d6d6" if ratio < 1 else speedup_color(ratio)
                    red, green, blue = (int(fill[pos : pos + 2], 16) for pos in (1, 3, 5))
                    if 0.2126 * red + 0.7152 * green + 0.0722 * blue < 105:
                        foreground = "white"
            parts.append(
                f'<rect x="{xs[column]}" y="{y}" width="{widths[column]}" height="{row_height}" '
                f'fill="{fill}" stroke="#444" stroke-width="0.4"/>'
            )
            numeric = column >= leading_columns
            center_x = (xs[column] + xs[column + 1]) / 2
            center_y = y + row_height / 2
            weight = "bold" if value.startswith((">", "<")) else "normal"
            if numeric:
                # Tabular digits retain column alignment while rendering Kx
                # and x as normal text, avoiding collisions between glyphs.
                numeric_font_size = body_font_size
                plain_number = value.lstrip(">").replace(",", "")
                try:
                    unusually_large_runtime = (
                        not value.endswith("x")
                        and float(plain_number) >= 1_000_000
                    )
                except ValueError:
                    unusually_large_runtime = False
                if unusually_large_runtime:
                    numeric_font_size = body_font_size - 2.0
                elif value.endswith("x"):
                    ratio = numeric_speedup(value)
                    if ratio is not None and ratio >= 100_000:
                        numeric_font_size = body_font_size - 1.1
                parts.append(
                    f'<text x="{xs[column + 1] - 3}" y="{center_y}" text-anchor="end" '
                    f'dominant-baseline="middle" font-size="{numeric_font_size}" '
                    f'font-weight="{weight}" fill="{foreground}" '
                    f'font-variant-numeric="tabular-nums">{html.escape(value)}</text>'
                )
            else:
                text_font_size = (
                    body_font_size - 0.8
                    if value == "Deep parent–leaf"
                    else body_font_size
                )
                parts.append(
                    f'<text x="{xs[column] + 3}" y="{center_y}" text-anchor="start" '
                    f'dominant-baseline="middle" font-size="{text_font_size}" font-weight="{weight}" '
                    f'fill="{foreground}">{svg_label(value)}</text>'
                )
        if previous_graph is not None and row[0] != previous_graph:
            parts.append(
                f'<line x1="{left}" y1="{y}" x2="{left + table_width}" y2="{y}" '
                'stroke="#111" stroke-width="2"/>'
            )
        previous_graph = row[0]
    bottom = top + total_header + len(rows) * row_height
    group_boundaries = {
        0,
        *range(1, leading_columns + 1),
        *(
            leading_columns + system_index * metrics_per_system
            for system_index in range(len(SYSTEMS) + 1)
        ),
    }
    for column, x in enumerate(xs):
        stroke_width = "1.2" if column in group_boundaries else "0.55"
        line_top = top if column in group_boundaries else top + header_height
        parts.append(
            f'<line x1="{x}" y1="{line_top}" x2="{x}" y2="{bottom}" '
            f'stroke="#222" stroke-width="{stroke_width}"/>'
        )
    if table_only:
        for index, note in enumerate(timeout_notes):
            parts.append(f'<text x="{left}" y="{bottom + 15 + index * 12}" font-size="7.5">{html.escape(note)}</text>')
        parts.extend(("</g>", "</svg>"))
        return "\n".join(parts)
    has_runtime_timeout = any(value.startswith(">") and not value.endswith("x") for row in rows for value in row)
    has_lower_bound = any(
        row[column].startswith((">", "<"))
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
        timeout_parts.append(TIMEOUT_NOTE)
    if has_lower_bound:
        timeout_parts.append(BOUND_NOTE)
    if has_double_timeout:
        timeout_parts.append('"–" means both methods timed out')
    if timeout_parts:
        notes.extend(timeout_parts)
    if graph_notes:
        notes.append(
            "Graphs: F = forest; NT = truebase; DT = ultratall; WT = ultrawide; "
            "SNB/C, SNB/P, SNB/T = SNB SF1 trees; SNB = full SNB SF1 graph."
        )
    if notes_override is not None:
        notes = list(notes_override) + timeout_parts
    for index, note in enumerate(notes):
        parts.append(
            f'<text x="{left}" y="{bottom + 17 + index * 12}" '
            f'font-size="7.5">{svg_rich_text(note)}</text>'
        )
    legend_y = bottom + (max(75, 29 + len(notes) * 12) if has_timeout else 75)
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
        sample_fill = "#d6d6d6" if ratio < 1 else speedup_color(ratio)
        parts.append(
            f'<rect x="{bar_x + offset}" y="{legend_y - 9}" '
            f'width="{bar_width / samples + 0.5}" height="{bar_height}" '
            f'fill="{sample_fill}"/>'
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
        'gray = slowdown; yellow = 1x; green = speedup; darker green = larger speedup</text>'
    )
    parts.extend(("</g>", "</svg>"))
    return "\n".join(parts)


def load_ldbc_medians(path, log_directories=None):
    values = defaultdict(list)
    with path.open(newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            graph, method = split_graph(row["graph"].strip())
            if graph != "snb_sf1":
                raise ValueError(f"Unexpected LDBC graph: {graph}")
            values[(row["query"].strip(), method)].append(
                read_runtime(path, row, log_directories)
            )

    medians = {}
    for key, runs in values.items():
        if len(runs) != 5:
            raise ValueError(f"Expected five runs for {key}, found {len(runs)}")
        medians[key] = median_runtime(runs, key)
    return medians


def make_ldbc_rows(system_medians):
    query_sets = [
        {query for query, _ in medians}
        for medians in system_medians.values()
    ]
    if not query_sets or any(queries != query_sets[0] for queries in query_sets[1:]):
        raise ValueError("LDBC queries must match in all AGE, Kuzu, and Neo4j inputs")

    def query_sort_key(query):
        kind, number = query.rsplit("-", 1)
        return (0 if kind.endswith("complex") else 1, int(number))

    rows = []
    for query in sorted(query_sets[0], key=query_sort_key):
        if query.startswith("interactive-complex-"):
            short = "IC" + query.rsplit("-", 1)[1]
            label = "Interactive Complex " + query.rsplit("-", 1)[1]
        elif query.startswith("interactive-short-"):
            short = "IS" + query.rsplit("-", 1)[1]
            label = "Interactive Short " + query.rsplit("-", 1)[1]
        else:
            short = query
            label = query
        row = [short, label]
        for system in SYSTEMS:
            medians = system_medians[system]
            times = {method: medians[(query, method)] for method in METHODS}
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--age", required=True, type=Path)
    parser.add_argument("--kuzu", required=True, type=Path)
    parser.add_argument("--neo4j", required=True, type=Path)
    parser.add_argument("--output", type=Path, help="Output PDF (default: results/combined/<timestamp>/runtime_tables.pdf).")
    add_timeout_arguments(parser)
    args = parser.parse_args()
    log_directories = timeout_directories(args.timeout_log_dir)
    inputs = {
        "Apache AGE": args.age,
        "Kuzu": args.kuzu,
        "Neo4j": args.neo4j,
    }
    input_kinds = set()
    for path in inputs.values():
        with path.open(newline="", encoding="utf-8-sig") as handle:
            fieldnames = csv.DictReader(handle).fieldnames or []
        input_kinds.add("tree" if "scenario" in fieldnames else "ldbc")
    if len(input_kinds) != 1:
        raise ValueError("All inputs must use the same CSV format")
    input_kind = input_kinds.pop()
    loader = load_medians if input_kind == "tree" else load_ldbc_medians
    medians = {system: loader(path, log_directories) for system, path in inputs.items()}
    if args.output is None:
        args.output = new_combined_directory() / "runtime_tables.pdf"
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="combined-runtime-tables-") as temp_name:
        temp = Path(temp_name)
        page_pdfs = []
        if input_kind == "tree":
            pages = [
                (title, make_rows(medians, query, scenarios), ("Graph", "Parameters"), True)
                for title, query, scenarios in report_pages(medians)
            ]
        else:
            pages = [
                ("LDBC SNB SF1", make_ldbc_rows(medians), ("Query", "Name"), False)
            ]
        for number, (title, rows, leading_headings, graph_notes) in enumerate(
            pages, start=1
        ):
            # Tree graph selections vary; only the LDBC report has a fixed row count.
            expected_rows = 3
            if input_kind == "ldbc" and len(rows) != expected_rows:
                raise ValueError(
                    f"Expected {expected_rows} rows for {title}, found {len(rows)}"
                )
            svg = temp / f"page-{number}.svg"
            pdf = temp / f"page-{number}.pdf"
            svg.write_text(
                svg_page(title, rows, leading_headings, graph_notes),
                encoding="utf-8",
            )
            render_pdf(svg, pdf)
            page_pdfs.append(pdf)
        combined = temp / "runtime_tables.pdf"
        subprocess.run(["pdfunite", *page_pdfs, combined], check=True)
        shutil.copyfile(combined, args.output)
    write_sources(inputs, [args.output], log_directories)
    print(args.output)


if __name__ == "__main__":
    main()
