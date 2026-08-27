#!/usr/bin/env python3
"""Create runtime tables for AGE, Kuzu, Neo4j, or AGE LDBC results."""

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


TIMEOUT_MS = 6 * 60 * 60 * 1000
METHODS = ("baseline", "dewey", "prepost")
QUERY_ORDER = (
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
SCENARIO_ORDER = {
    "q01_q02": 0,
    "q01": 0,
    "q02": 1,
    "q03_q04": 2,
    "q03": 2,
    "q04": 3,
    "q05": 4,
    "q06": 5,
    "q07": 6,
    "q08": 7,
    "q09": 8,
    "q10": 9,
    "unknown": 10,
}
PARAMETER_LABELS = {
    "q01_q02": "Root of largest/deepest tree",
    "q01": "Root of largest tree",
    "q02": "Root of deepest tree",
    "q03_q04": "High/low-degree leaf parent",
    "q03": "High-degree leaf parent",
    "q04": "Low-degree leaf parent",
    "q05": "Highest-degree node",
    "q06": "Lowest-degree node",
    "q07": "Root-farthest-leaf pair",
    "q08": "Deepest parent-leaf pair",
    "q09": "Shallow sibling pair",
    "q10": "Most distant leaf pair",
    "unknown": "Unknown",
}

ANCESTOR_POSITIVE_SCENARIOS = frozenset(("q07", "q08"))
ANCESTOR_NEGATIVE_SCENARIOS = frozenset(("q09", "q10"))


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
    snb = {
        "snb_sf1_comment": "SNB/C",
        "snb_sf1_place": "SNB/P",
        "snb_sf1_tagclass": "SNB/T",
    }
    if graph in snb:
        return snb[graph]
    raise ValueError(f"Unknown graph: {graph}")


def graph_sort_key(graph):
    label = graph_label(graph)
    if label.startswith("SNB/"):
        return 4, {"SNB/C": 0, "SNB/P": 1, "SNB/T": 2}[label]
    for prefix, order in (("F", 0), ("NT", 1), ("DT", 2), ("WT", 3)):
        if label.startswith(prefix):
            return order, int(label[len(prefix) :])
    raise ValueError(f"Unknown graph abbreviation: {label}")


def runtime_text(value):
    return ">6 h" if value is None else f"{value:,.3f}"


def speedup_text(ratio, lower_bound=False):
    prefix = ">" if lower_bound else ""
    return f"{prefix}{ratio:,.3f}x"


def load_groups(path):
    values = defaultdict(list)
    with path.open(newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            graph, method = split_graph(row["graph"].strip())
            runtime = row["runtime_ms"].strip()
            values[(graph, row["query"].strip(), row["scenario"].strip(), method)].append(
                None if not runtime else float(runtime)
            )

    groups = {}
    for key, runs in values.items():
        if len(runs) != 5:
            raise ValueError(f"Expected five runs for {key}, found {len(runs)}")
        if any(value is None for value in runs):
            if not all(value is None for value in runs):
                raise ValueError(f"Partial timeout in {key}; handling must be clarified")
            groups[key] = None
        else:
            groups[key] = statistics.median(runs)
    return groups


def make_rows(groups, query, scenarios=None):
    keys = sorted(
        {
            (graph, scenario)
            for graph, q, scenario, _ in groups
            if q == query and (scenarios is None or scenario in scenarios)
        },
        key=lambda item: (graph_sort_key(item[0]), SCENARIO_ORDER[item[1]], item[1]),
    )
    output = []
    for graph, scenario in keys:
        times = {method: groups[(graph, query, scenario, method)] for method in METHODS}
        speedups = []
        for method in ("dewey", "prepost"):
            if times["baseline"] is None and times[method] is None:
                speedups.append("–")
            elif times[method] is None:
                raise ValueError(
                    f"Unexpected case: {method} timed out with an available baseline: "
                    f"{graph}, {query}, {scenario}"
                )
            elif times["baseline"] is None:
                speedups.append(speedup_text(TIMEOUT_MS / times[method], True))
            else:
                speedups.append(speedup_text(times["baseline"] / times[method]))
        output.append(
            [
                graph_label(graph),
                PARAMETER_LABELS[scenario],
                runtime_text(times["baseline"]),
                runtime_text(times["dewey"]),
                runtime_text(times["prepost"]),
                *speedups,
            ]
        )
    return output


def report_pages(groups):
    available = {
        (query, scenario)
        for _, query, scenario, _ in groups
    }
    pages = [
        (QUERY_LABELS[query], query, None)
        for query in QUERY_ORDER
        if query != "11_check_if_ancestor"
        and any(available_query == query for available_query, _ in available)
    ]
    available_ancestor_scenarios = {
        scenario
        for query, scenario in available
        if query == "11_check_if_ancestor"
    }
    if available_ancestor_scenarios & ANCESTOR_POSITIVE_SCENARIOS:
        pages.append(
            (
                "Query 11 - Check if Ancestor (Positive)",
                "11_check_if_ancestor",
                ANCESTOR_POSITIVE_SCENARIOS,
            )
        )
    if available_ancestor_scenarios & ANCESTOR_NEGATIVE_SCENARIOS:
        pages.append(
            (
                "Query 11 - Check if Ancestor (Negative)",
                "11_check_if_ancestor",
                ANCESTOR_NEGATIVE_SCENARIOS,
            )
        )
    return pages


def groff_document(groups):
    lines = [
        ".pl 16.54i",
        ".po 0.45i",
        ".ll 10.79i",
        ".ps 8",
        ".vs 9.5p",
        ".fam H",
    ]
    headings = (
        "Graph@Parameters@Baseline@Dewey@Prepost@Speedup Dewey@Speedup Prepost"
    )
    for page_number, (title, query, scenarios) in enumerate(report_pages(groups)):
        if page_number:
            lines.append(".bp")
        lines.extend(
            [
                ".sp 0.25i",
                ".ce 1",
                ".ps 16",
                f"\\fB{title}\\fP",
                ".sp 0.15i",
                ".ps 9.5",
                ".vs 11p",
                ".TS H",
                "center, expand, box, allbox, tab(@);",
                "c c c c c c c",
                "l l r r r r r.",
                headings,
                "_",
                ".TH",
            ]
        )
        rows = make_rows(groups, query, scenarios)
        has_timeout = any(">6 h" in row for row in rows)
        has_lower_bound = any(value.startswith(">") for row in rows for value in row[5:])
        has_double_timeout = any(value == "–" for row in rows for value in row[5:])
        previous_graph = None
        for row in rows:
            if previous_graph is not None and row[0] != previous_graph:
                lines.append("=")
            lines.append("@".join(row))
            previous_graph = row[0]
        lines.extend(
            [
                ".TE",
                ".sp 0.12i",
                ".ps 7.5",
                ".vs 9.5p",
                "\\fBGraph:\\fP F = forest; NT = normal tree (truebase); "
                "DT = deep tree (ultratall); WT = wide tree (ultrawide); number = node count;",
                "SNB/C = Comment, SNB/P = Place, SNB/T = Tagclass (SNB SF1).",
                ".br",
                "\\fBRuntimes:\\fP median of five runs, in ms, right-aligned."
                + (" \\(dq>6 h\\(dq denotes a timeout." if has_timeout else ""),
                ".br",
                "\\fBSpeedup Dewey\\fP = Baseline / Dewey; "
                "\\fBSpeedup Prepost\\fP = Baseline / Prepost."
                + (
                    " \\(dq>\\(dq denotes a lower bound calculated using the 6 h timeout."
                    if has_lower_bound
                    else ""
                )
                + (" \\(dq-\\(dq denotes a timeout for both methods." if has_double_timeout else ""),
            ]
        )
    return "\n".join(lines) + "\n"


def speedup_value(text):
    if text == "–":
        return None
    return float(text.lstrip(">").rstrip("x").replace(",", ""))


def interpolate_color(start, end, amount):
    amount = min(1.0, max(0.0, amount))
    start_rgb = tuple(int(start[index : index + 2], 16) for index in (1, 3, 5))
    end_rgb = tuple(int(end[index : index + 2], 16) for index in (1, 3, 5))
    rgb = tuple(round(a + (b - a) * amount) for a, b in zip(start_rgb, end_rgb))
    return "#" + "".join(f"{component:02x}" for component in rgb)


def speedup_color(ratio):
    yellow, dark_red = "#ffe680", "#8b0000"
    bright_green, dark_green, extra_dark_green = "#00e65c", "#005a24", "#002d12"
    if ratio < 1:
        transformed = math.log1p(min(1 - ratio, 1) / 0.05) / math.log1p(1 / 0.05)
        return interpolate_color(yellow, dark_red, transformed)
    if ratio <= 2:
        transformed = math.log1p((ratio - 1) / 0.05) / math.log1p(1 / 0.05)
        return interpolate_color(yellow, bright_green, transformed)
    if ratio <= 10:
        transformed = math.log(ratio / 2) / math.log(10 / 2)
        return interpolate_color(bright_green, dark_green, transformed)
    transformed = math.log(min(ratio, 100) / 10) / math.log(100 / 10)
    return interpolate_color(dark_green, extra_dark_green, transformed)


def svg_page(title, rows, columns=None, column_widths=None):
    width, height = 842, 1191
    left, table_width = 40, 762
    top, header_height, row_height = 52, 20, 15
    if columns is None:
        columns = ("Graph", "Parameters", "Baseline (ms)", "Dewey (ms)", "Prepost (ms)", "Speedup Dewey", "Speedup Prepost")
    if column_widths is None:
        column_widths = (70, 190, 100, 90, 90, 111, 111)
    speedup_columns = {index for index, column in enumerate(columns) if column.startswith("Speedup")}
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="297mm" height="420mm" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        '<g font-family="Arial, Helvetica, sans-serif" fill="#111">',
        f'<text x="{width / 2}" y="30" text-anchor="middle" font-size="16" font-weight="bold">{html.escape(title)}</text>',
    ]
    x_positions = [left]
    for column_width in column_widths:
        x_positions.append(x_positions[-1] + column_width)

    parts.append(f'<rect x="{left}" y="{top}" width="{table_width}" height="{header_height}" fill="#eeeeee" stroke="#111" stroke-width="1"/>')
    for index, heading in enumerate(columns):
        x = (x_positions[index] + x_positions[index + 1]) / 2
        parts.append(f'<text x="{x}" y="{top + 14}" text-anchor="middle" font-size="8.5">{heading}</text>')

    previous_graph = None
    for row_index, row in enumerate(rows):
        y = top + header_height + row_index * row_height
        for column_index, value in enumerate(row):
            fill = "white"
            text_color = "#111111"
            if column_index in speedup_columns:
                numeric_value = speedup_value(value)
                if numeric_value is not None:
                    fill = speedup_color(numeric_value)
                    red, green, blue = (int(fill[index : index + 2], 16) for index in (1, 3, 5))
                    if 0.2126 * red + 0.7152 * green + 0.0722 * blue < 105:
                        text_color = "#ffffff"
            parts.append(
                f'<rect x="{x_positions[column_index]}" y="{y}" width="{column_widths[column_index]}" '
                f'height="{row_height}" fill="{fill}" stroke="#333" stroke-width="0.45"/>'
            )
            right_aligned = columns[column_index] not in {"Graph", "Parameters", "Query"}
            x = x_positions[column_index + 1] - 4 if right_aligned else x_positions[column_index] + 4
            anchor = "end" if right_aligned else "start"
            weight = "bold" if value.startswith(">") else "normal"
            if right_aligned:
                # librsvg does not reliably honor OpenType's `tnum` feature.
                # Position proportional sans-serif glyphs on fixed-width slots instead.
                advance = 5.15
                parts.append(
                    f'<g font-family="Adwaita Sans, Arial, sans-serif" font-weight="{weight}" '
                    f'fill="{text_color}">'
                )
                for character_index, character in enumerate(value):
                    character_x = x - (len(value) - character_index - 0.5) * advance
                    parts.append(
                        f'<text x="{character_x}" y="{y + 10.8}" text-anchor="middle" '
                        f'font-size="8.2">{html.escape(character)}</text>'
                    )
                parts.append("</g>")
            else:
                parts.append(
                    f'<text x="{x}" y="{y + 10.8}" text-anchor="{anchor}" font-size="8.2" '
                    f'font-weight="{weight}" fill="{text_color}">{html.escape(value)}</text>'
                )
        if previous_graph is not None and row[0] != previous_graph:
            parts.append(
                f'<line x1="{left}" y1="{y}" x2="{left + table_width}" y2="{y}" '
                'stroke="#111" stroke-width="2.2"/>'
            )
        previous_graph = row[0]

    table_bottom = top + header_height + len(rows) * row_height
    for x in x_positions:
        parts.append(f'<line x1="{x}" y1="{top}" x2="{x}" y2="{table_bottom}" stroke="#222" stroke-width="0.65"/>')
    parts.append(f'<line x1="{left}" y1="{table_bottom}" x2="{left + table_width}" y2="{table_bottom}" stroke="#111"/>')

    has_timeout = any(">6 h" in row for row in rows)
    speedup_values = [row[index] for row in rows for index in speedup_columns]
    has_lower_bound = any(value.startswith(">") for value in speedup_values)
    has_double_timeout = any(value == "–" for value in speedup_values)
    notes = []
    is_ldbc = columns[0] == "Query"
    if columns[0] == "Graph":
        notes.extend((
            "Graph: F = forest; NT = normal tree (truebase); DT = deep tree (ultratall); WT = wide tree (ultrawide); number = node count;",
            "SNB/C = Comment, SNB/P = Place, SNB/T = Tagclass (SNB SF1).",
        ))
    notes.extend([
        (
            "Runtimes: median of five runs, in milliseconds (ms)."
            if is_ldbc
            else "Runtimes: median of five runs."
        ) + (' ">6 h" denotes a timeout.' if has_timeout else ""),
        "Speedup Dewey = Baseline / Dewey; "
        "Speedup Prepost = Baseline / Prepost."
        + (' ">" denotes a lower bound calculated using the 6 h timeout.' if has_lower_bound else "")
        + (' "-" denotes a timeout for both methods.' if has_double_timeout else ""),
    ])
    if has_lower_bound:
        notes.append("Bold values are derived from a timeout.")
    if is_ldbc:
        notes.append(
            "Speedup colors: red = slowdown; yellow = 1x; green = speedup; "
            "darker green = larger speedup."
        )
    note_y = table_bottom + 16
    for line_number, note in enumerate(notes):
        parts.append(f'<text x="{left}" y="{note_y + line_number * 11}" font-size="7.5">{html.escape(note)}</text>')
    legend_y = note_y + len(notes) * 11 + 2
    bar_x, bar_width, bar_height = left, 360, 12
    slowdown_width, normal_width = 90, 160
    samples = 180
    for sample in range(samples):
        offset = bar_width * sample / samples
        if offset < slowdown_width:
            transformed = 1 - offset / slowdown_width
            ratio = 1 - 0.05 * (math.exp(transformed * math.log1p(1 / 0.05)) - 1)
        elif offset < slowdown_width + normal_width:
            transformed = (offset - slowdown_width) / normal_width
            ratio = math.exp(transformed * math.log(10))
        else:
            transformed = (offset - slowdown_width - normal_width) / (bar_width - slowdown_width - normal_width)
            ratio = 10 * math.exp(transformed * math.log(10))
        parts.append(
            f'<rect x="{bar_x + offset}" y="{legend_y - 8}" width="{bar_width / samples + 0.5}" '
            f'height="{bar_height}" fill="{speedup_color(ratio)}"/>'
        )
    parts.append(f'<rect x="{bar_x}" y="{legend_y - 8}" width="{bar_width}" height="{bar_height}" fill="none" stroke="#333" stroke-width="0.6"/>')
    for position, label in ((0, "0x"), (slowdown_width, "1x"),
                            (slowdown_width + normal_width, "10x"), (bar_width, "100x+")):
        anchor = "start" if position == 0 else "end" if position == bar_width else "middle"
        parts.append(
            f'<text x="{bar_x + position}" y="{legend_y + 15}" text-anchor="{anchor}" '
            f'font-size="7.5">{html.escape(label)}</text>'
        )
    parts.extend(("</g>", "</svg>"))
    return "\n".join(parts)


def load_ldbc_rows(path):
    values = defaultdict(list)
    with path.open(newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            graph, method = split_graph(row["graph"].strip())
            if graph != "snb_sf1":
                raise ValueError(f"Unexpected LDBC graph: {graph}")
            runtime = row["runtime_ms"].strip()
            values[(row["query"].strip(), method)].append(None if not runtime else float(runtime))

    medians = {}
    for key, runs in values.items():
        if len(runs) != 5:
                raise ValueError(f"Expected five runs for {key}, found {len(runs)}")
        if any(value is None for value in runs):
            if not all(value is None for value in runs):
                raise ValueError(f"Partial timeout in {key}; handling must be clarified")
            medians[key] = None
        else:
            medians[key] = statistics.median(runs)

    query_order = list(dict.fromkeys(query for query, _ in values))
    rows = []
    for query in query_order:
        times = {method: medians[(query, method)] for method in METHODS}
        speedups = []
        for method in ("dewey", "prepost"):
            if times["baseline"] is None and times[method] is None:
                speedups.append("–")
            elif times[method] is None:
                raise ValueError(f"Unexpected {method} timeout with an available baseline: {query}")
            elif times["baseline"] is None:
                speedups.append(speedup_text(TIMEOUT_MS / times[method], True))
            else:
                speedups.append(speedup_text(times["baseline"] / times[method]))
        display_query = query.replace("interactive-complex-", "Interactive Complex ").replace(
            "interactive-short-", "Interactive Short "
        )
        rows.append(
            [display_query, runtime_text(times["baseline"]), runtime_text(times["dewey"]),
             runtime_text(times["prepost"]), *speedups]
        )
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="runtime-tables-") as temp_dir_name:
        temp_dir = Path(temp_dir_name)
        page_pdfs = []
        with args.input.open(newline="", encoding="utf-8-sig") as handle:
            fieldnames = csv.DictReader(handle).fieldnames or []
        if "scenario" in fieldnames:
            groups = load_groups(args.input)
            pages = [
                (title, make_rows(groups, query, scenarios), None, None)
                for title, query, scenarios in report_pages(groups)
            ]
            if not pages:
                raise ValueError("No supported query scenarios found")
        else:
            pages = [(
                "LDBC SNB SF1",
                load_ldbc_rows(args.input),
                ("Query", "Baseline (ms)", "Dewey (ms)", "Prepost (ms)", "Speedup Dewey", "Speedup Prepost"),
                (220, 100, 100, 100, 121, 121),
            )]
        for page_number, (title, rows, columns, widths) in enumerate(pages, start=1):
            svg_path = temp_dir / f"page-{page_number}.svg"
            pdf_path = temp_dir / f"page-{page_number}.pdf"
            svg_path.write_text(svg_page(title, rows, columns, widths), encoding="utf-8")
            subprocess.run(["rsvg-convert", "-f", "pdf", "-o", pdf_path, svg_path], check=True)
            page_pdfs.append(pdf_path)
        combined_pdf = temp_dir / "combined.pdf"
        subprocess.run(["pdfunite", *page_pdfs, combined_pdf], check=True)
        shutil.copyfile(combined_pdf, args.output)
    print(args.output)


if __name__ == "__main__":
    main()
