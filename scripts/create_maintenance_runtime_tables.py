#!/usr/bin/env python3
"""Create AGE maintenance runtime tables as a four-page PDF."""

from __future__ import annotations

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
    graph_label, graph_sort_key, interpolate_color, runtime_text, Timeout,
    read_runtime, median_runtime, add_timeout_arguments, timeout_directories, TIMEOUT_NOTE, bound_text,
)


METHODS = ("baseline", "dewey", "prepost")
QUERY_ORDER = (
    "01_insert_last_child_under_last_root",
    "02_insert_first_child_under_first_root",
    "03_insert_last_root",
    "04_insert_first_root",
)
QUERY_LABELS = {
    "01_insert_last_child_under_last_root": "Query 01 - Insert Last Child under Last Root",
    "02_insert_first_child_under_first_root": "Query 02 - Insert First Child under First Root",
    "03_insert_last_root": "Query 03 - Insert Last Root",
    "04_insert_first_root": "Query 04 - Insert First Root",
}
PARAMETER_LABELS = {
    "01_insert_last_child_under_last_root": "Last root, last child position",
    "02_insert_first_child_under_first_root": "First root, first child position",
    "03_insert_last_root": "Last root position",
    "04_insert_first_root": "First root position",
}


def split_graph(graph: str) -> tuple[str, str]:
    for method in METHODS:
        suffix = f"_{method}"
        if graph.endswith(suffix):
            return graph[: -len(suffix)], method
    raise ValueError(f"Unknown representation: {graph}")


def load_medians(path: Path, log_directories=None) -> dict[tuple[str, str, str], float | Timeout]:
    values: dict[tuple[str, str, str], list[float | Timeout]] = defaultdict(list)
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        required = {"graph", "query", "run", "runtime_ms"}
        missing = required - set(reader.fieldnames or ())
        if missing:
            raise ValueError(f"Missing columns: {sorted(missing)}")
        for row in reader:
            graph, method = split_graph(row["graph"].strip())
            values[(graph, row["query"].strip(), method)].append(read_runtime(path, row, log_directories))

    medians = {}
    for key, runs in values.items():
        if len(runs) != 5:
            raise ValueError(f"Expected five runs for {key}, found {len(runs)}")
        medians[key] = median_runtime(runs, key)
    return medians


def cost_text(value: float) -> str:
    return f"{value:+,.3f}"


def make_rows(
    medians: dict[tuple[str, str, str], float | Timeout], query: str
) -> list[tuple[list[str], tuple[float, float]]]:
    graphs = sorted(
        {graph for graph, candidate, _ in medians if candidate == query},
        key=graph_sort_key,
    )
    rows = []
    for graph in graphs:
        times = {method: medians[(graph, query, method)] for method in METHODS}
        costs = []
        cost_labels = []
        for method in ("dewey", "prepost"):
            baseline, indexed = times["baseline"], times[method]
            if isinstance(baseline, Timeout) and isinstance(indexed, Timeout):
                cost_labels.append("–")
                costs.append(math.nan)
            elif isinstance(baseline, Timeout):
                cost_labels.append(bound_text(indexed - baseline.milliseconds, "<", signed=True))
                costs.append(math.nan)
            elif isinstance(indexed, Timeout):
                cost_labels.append(bound_text(indexed.milliseconds - baseline, ">", signed=True))
                costs.append(math.nan)
            else:
                costs.append(indexed - baseline)
                cost_labels.append(cost_text(costs[-1]))
        rows.append(([
            graph_label(graph),
            PARAMETER_LABELS[query],
            runtime_text(times["baseline"]),
            runtime_text(times["dewey"]),
            runtime_text(times["prepost"]),
            *cost_labels,
        ], tuple(costs)))
    return rows


def cost_color(value: float, maximum: float) -> str:
    if not math.isfinite(value):
        return "#eeeeee"
    if maximum <= 0 or value == 0:
        return "#ffe680"
    amount = math.log1p(abs(value)) / math.log1p(maximum)
    if value > 0:
        return interpolate_color("#ffe680", "#8b0000", amount)
    return interpolate_color("#ffe680", "#007a32", amount)


def svg_page(
    title: str,
    rows: list[tuple[list[str], tuple[float, float]]],
    maximum_cost: float,
) -> str:
    width, height = 842, 1191
    left, table_width = 40, 762
    top, header_height, row_height = 52, 20, 15
    columns = (
        "Graph", "Parameters", "Baseline (ms)", "Dewey (ms)", "Prepost (ms)",
        "Dewey cost (ms)", "Prepost cost (ms)",
    )
    column_widths = (70, 190, 90, 90, 90, 116, 116)
    cost_columns = {5, 6}
    x_positions = [left]
    for column_width in column_widths:
        x_positions.append(x_positions[-1] + column_width)

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="297mm" height="420mm" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        '<g font-family="Arial, Helvetica, sans-serif" fill="#111">',
        f'<text x="{width / 2}" y="30" text-anchor="middle" font-size="16" font-weight="bold">{html.escape(title)}</text>',
        f'<rect x="{left}" y="{top}" width="{table_width}" height="{header_height}" fill="#eeeeee" stroke="#111" stroke-width="1"/>',
    ]
    for index, heading in enumerate(columns):
        x = (x_positions[index] + x_positions[index + 1]) / 2
        parts.append(f'<text x="{x}" y="{top + 14}" text-anchor="middle" font-size="8.5">{html.escape(heading)}</text>')

    previous_graph = None
    for row_index, (row, costs) in enumerate(rows):
        y = top + header_height + row_index * row_height
        for column_index, value in enumerate(row):
            fill = "white"
            text_color = "#111111"
            if column_index in cost_columns:
                fill = cost_color(costs[column_index - 5], maximum_cost)
                red, green, blue = (int(fill[index:index + 2], 16) for index in (1, 3, 5))
                if 0.2126 * red + 0.7152 * green + 0.0722 * blue < 105:
                    text_color = "#ffffff"
            parts.append(
                f'<rect x="{x_positions[column_index]}" y="{y}" width="{column_widths[column_index]}" '
                f'height="{row_height}" fill="{fill}" stroke="#333" stroke-width="0.45"/>'
            )
            right_aligned = column_index >= 2
            x = x_positions[column_index + 1] - 4 if right_aligned else x_positions[column_index] + 4
            anchor = "end" if right_aligned else "start"
            parts.append(
                f'<text x="{x}" y="{y + 10.8}" text-anchor="{anchor}" font-size="8.2" '
                f'fill="{text_color}">{html.escape(value)}</text>'
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

    notes = (
        "Graph: F = forest; NT = normal tree (truebase); DT = deep tree (ultratall); WT = wide tree (ultrawide); number = node count;",
        "SNB/C = Comment, SNB/P = Place, SNB/T = Tagclass (SNB SF1).",
        "Runtimes: median of five runs. Maintenance cost = indexed representation - Baseline, in milliseconds (ms).",
        "Cost colors: green = lower than Baseline; yellow = zero; red = higher than Baseline. Intensity uses a logarithmic global scale.",
    )
    if any(isinstance(value, str) and value.startswith(">") for row, _ in rows for value in row[2:5]):
        notes += (
            TIMEOUT_NOTE,
            "Costs: > = lower bound; < = upper bound; – = both methods timed out. Bounds use the limits in the same row.",
            "Gray cost cells are bounds or unavailable values; they are not exact measured differences.",
        )
    note_y = table_bottom + 16
    for line_number, note in enumerate(notes):
        parts.append(f'<text x="{left}" y="{note_y + line_number * 11}" font-size="7.5">{html.escape(note)}</text>')

    legend_y = note_y + len(notes) * 11 + 3
    bar_width, bar_height, samples = 360, 12, 180
    for sample in range(samples):
        signed = (2 * sample / (samples - 1)) - 1
        magnitude = math.expm1(abs(signed) * math.log1p(maximum_cost)) if maximum_cost else 0
        value = math.copysign(magnitude, signed)
        parts.append(
            f'<rect x="{left + bar_width * sample / samples}" y="{legend_y - 8}" '
            f'width="{bar_width / samples + 0.5}" height="{bar_height}" '
            f'fill="{cost_color(value, maximum_cost)}"/>'
        )
    parts.append(f'<rect x="{left}" y="{legend_y - 8}" width="{bar_width}" height="{bar_height}" fill="none" stroke="#333" stroke-width="0.6"/>')
    for position, label, anchor in ((0, "lower", "start"), (bar_width / 2, "0 ms", "middle"), (bar_width, "higher", "end")):
        parts.append(f'<text x="{left + position}" y="{legend_y + 15}" text-anchor="{anchor}" font-size="7.5">{label}</text>')
    parts.extend(("</g>", "</svg>"))
    return "\n".join(parts)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=Path)
    parser.add_argument(
        "output", nargs="?", type=Path,
        help="Output PDF (default: runtime_tables.pdf beside the input CSV).",
    )
    add_timeout_arguments(parser)
    args = parser.parse_args()
    if args.output is None:
        args.output = args.input.with_name("runtime_tables.pdf")
    log_directories = timeout_directories(args.timeout_log_dir)

    medians = load_medians(args.input, log_directories)
    missing = set(QUERY_ORDER) - {query for _, query, _ in medians}
    if missing:
        raise ValueError(f"Missing queries: {sorted(missing)}")
    page_rows = {query: make_rows(medians, query) for query in QUERY_ORDER}
    maximum_cost = max((abs(cost) for rows in page_rows.values() for _, costs in rows for cost in costs if math.isfinite(cost)), default=0)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="maintenance-runtime-tables-") as temp_name:
        temp = Path(temp_name)
        page_pdfs = []
        for page_number, query in enumerate(QUERY_ORDER, start=1):
            svg_path = temp / f"page-{page_number}.svg"
            pdf_path = temp / f"page-{page_number}.pdf"
            svg_path.write_text(
                svg_page(QUERY_LABELS[query], page_rows[query], maximum_cost),
                encoding="utf-8",
            )
            subprocess.run(["rsvg-convert", "-f", "pdf", "-o", pdf_path, svg_path], check=True)
            page_pdfs.append(pdf_path)
        combined = temp / "runtime_tables.pdf"
        subprocess.run(["pdfunite", *page_pdfs, combined], check=True)
        shutil.copyfile(combined, args.output)
    print(args.output)


if __name__ == "__main__":
    main()
