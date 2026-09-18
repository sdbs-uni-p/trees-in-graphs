#!/usr/bin/env python3
"""Create runtime tables for AGE, Kuzu, Neo4j, or AGE LDBC results."""

import argparse
import csv
import html
import json
import hashlib
from datetime import datetime, timezone
import math
import os
import re
from dataclasses import dataclass
from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING
import shutil
import statistics
import subprocess
import tempfile
from collections import defaultdict
from pathlib import Path


TIMEOUT_LOGS = defaultdict(set)


def new_combined_directory(results_root=None):
    root = results_root or Path(__file__).resolve().parents[1] / "results"
    parent = root / "combined"
    parent.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    for suffix in range(10000):
        directory = parent / (timestamp + (f"_{suffix:04d}" if suffix else ""))
        try:
            directory.mkdir()
            return directory
        except FileExistsError:
            continue
    raise ValueError("Cannot allocate a unique combined report directory")


def write_sources(inputs, outputs, log_directories):
    """Keep provenance per output, including when several reports share a directory."""
    def fingerprint(path):
        path = Path(path).resolve()
        return {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}

    sources = {}
    for name, path in inputs.items():
        path = Path(path).resolve()
        sources[name] = {
            "csv": fingerprint(path),
            "timeout_logs": [fingerprint(log) for log in sorted(TIMEOUT_LOGS[path])],
        }
        metadata = path.parent / "metadata.json"
        if metadata.is_file():
            sources[name]["metadata"] = fingerprint(metadata)
    for output in outputs:
        output = Path(output).resolve()
        destination = output.parent / "sources.json"
        payload = {"schema_version": 1, "reports": {}}
        if destination.exists():
            payload = json.loads(destination.read_text(encoding="utf-8"))
            if payload.get("schema_version") != 1 or not isinstance(payload.get("reports"), dict):
                raise ValueError(f"Unsupported provenance file: {destination}")
        payload["reports"][output.name] = {
            "created_at": datetime.now(timezone.utc).isoformat(),
            "output": fingerprint(output), "inputs": sources,
        }
        def portable(value):
            if isinstance(value, dict):
                result = {}
                for key, item in value.items():
                    if key == "timeout_log_directories":
                        continue  # Only actual input files belong in the manifest.
                    if key == "path":
                        source = Path(item)
                        source = source.resolve() if source.is_absolute() else (destination.parent / source).resolve()
                        if output.parent.name == "paper_results":
                            results_root = Path(__file__).resolve().parents[1] / "results"
                            try:
                                relative = source.relative_to(results_root)
                            except ValueError:
                                raise ValueError("Paper provenance may reference only files inside results/<system>/paper_results")
                            if len(relative.parts) < 3 or relative.parts[1] != "paper_results" or not source.is_file():
                                raise ValueError("Paper provenance requires existing paper_results files; copy the required inputs/logs there first")
                        result[key] = Path(os.path.relpath(source, destination.parent)).as_posix()
                    else:
                        result[key] = portable(item)
                return result
            if isinstance(value, list):
                return [portable(item) for item in value]
            return value

        payload = portable(payload)
        payload["path_base"] = "directory containing sources.json"
        temporary = destination.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        temporary.replace(destination)


@dataclass(frozen=True)
class Timeout:
    milliseconds: int


def add_timeout_arguments(parser):
    parser.add_argument(
        "--timeout-log-dir", action="append", default=[], metavar="CSV=ERRORS_DIR",
        help="Use an explicit errors directory for a CSV (repeatable for merged inputs).",
    )


def timeout_directories(specifications):
    directories = defaultdict(list)
    for specification in specifications:
        csv_name, separator, directory = specification.partition("=")
        if not separator or not csv_name or not directory:
            raise ValueError("Expected --timeout-log-dir CSV=ERRORS_DIR")
        location = Path(directory).resolve()
        if not location.is_dir():
            raise ValueError(f"Timeout log directory does not exist: {location}")
        directories[Path(csv_name).resolve()].append(location)
    return directories


def read_runtime(path, row, log_directories=None):
    """Read a measurement, requiring per-run log evidence for every missing value."""
    if row["runtime_ms"].strip():
        value = float(row["runtime_ms"])
        if not math.isfinite(value) or value < 0:
            raise ValueError(f"Invalid runtime in {path}: {row}")
        return value
    graph = row["graph"].strip()
    _, method = split_graph(graph)
    stem = f"{graph}_{method}_{row['query'].strip()}"
    if "scenario" in row:
        stem += f"_{row['scenario'].strip()}"
    stem = re.sub(r"[/\\: ]", "_", stem)
    filename = f"{stem}_run{row['run'].strip()}.log"
    directories = (log_directories or {}).get(Path(path).resolve(), [Path(path).parent / "errors"])
    logs = [directory / filename for directory in directories if (directory / filename).is_file()]
    if not logs:
        raise ValueError(
            f"Missing runtime in {path}, but no matching timeout log {filename}. "
            "Provide --timeout-log-dir CSV=ERRORS_DIR for original/merged results."
        )
    limits = set()
    for log in logs:
        content = log.read_text(encoding="utf-8").strip()
        match = re.fullmatch(
            r"(?:Query timed out after |Skipped run \d+: run 1 timed out after )(\d+) ms\.",
            content,
        )
        if not match or int(match[1]) <= 0:
            raise ValueError(f"Missing runtime is not a confirmed timeout with a positive limit: {log}")
        limits.add(int(match[1]))
        TIMEOUT_LOGS[Path(path).resolve()].add(log.resolve())
    if len(limits) != 1:
        raise ValueError(f"Conflicting timeout limits for {filename}: {sorted(limits)}; select the correct source logs")
    return Timeout(limits.pop())


def median_runtime(runs, key):
    timed_out = [value for value in runs if isinstance(value, Timeout)]
    if timed_out:
        if len(timed_out) != len(runs):
            raise ValueError(f"Mixed successful measurements and timeouts for {key}")
        if len(set(timed_out)) != 1:
            raise ValueError(f"Different timeout limits within measurement group {key}")
        return timed_out[0]
    return statistics.median(runs)


def timeout_text(value):
    for unit, divisor in (("h", 3600000), ("min", 60000), ("s", 1000), ("ms", 1)):
        if value.milliseconds % divisor == 0:
            return f">{value.milliseconds // divisor:,} {unit}"


def runtime_speedup(baseline, method):
    if isinstance(baseline, Timeout) and isinstance(method, Timeout):
        return "–"
    if isinstance(method, Timeout):
        return bound_text(Decimal(str(baseline)) / method.milliseconds, "<") + "x"
    if isinstance(baseline, Timeout):
        return bound_text(Decimal(baseline.milliseconds) / Decimal(str(method)), ">") + "x"
    return speedup_text(baseline / method)


def bound_text(value, direction, signed=False):
    """Round outward so a displayed bound never claims more than the evidence."""
    rounded = Decimal(str(value)).quantize(
        Decimal("0.001"), rounding=ROUND_FLOOR if direction == ">" else ROUND_CEILING
    )
    return direction + format(rounded, "+,.3f" if signed else ",.3f")


TIMEOUT_NOTE = 'Timeouts: each >T runtime cell gives the logged limit T for that row and method (unit shown).'
BOUND_NOTE = 'Speedup bounds use those limits in the same row/system; > is a lower bound, < an upper bound.'

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
    return timeout_text(value) if isinstance(value, Timeout) else f"{value:,.3f}"


def speedup_text(ratio, lower_bound=False):
    prefix = ">" if lower_bound else ""
    return f"{prefix}{ratio:,.3f}x"


def load_groups(path, log_directories=None):
    values = defaultdict(list)
    with path.open(newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            graph, method = split_graph(row["graph"].strip())
            values[(graph, row["query"].strip(), row["scenario"].strip(), method)].append(
                read_runtime(path, row, log_directories)
            )

    groups = {}
    for key, runs in values.items():
        if len(runs) != 5:
            raise ValueError(f"Expected five runs for {key}, found {len(runs)}")
        groups[key] = median_runtime(runs, key)
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
        speedups = [runtime_speedup(times["baseline"], times[method]) for method in ("dewey", "prepost")]
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
        has_timeout = any(value.startswith((">", "<")) and not value.endswith("x") for row in rows for value in row)
        has_lower_bound = any(value.startswith((">", "<")) for row in rows for value in row[5:])
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
                + (" \\(dq>T\\(dq gives the logged timeout for that row/method, with units." if has_timeout else ""),
                ".br",
                "\\fBSpeedup Dewey\\fP = Baseline / Dewey; "
                "\\fBSpeedup Prepost\\fP = Baseline / Prepost."
                + (
                    " \\(dq>\\(dq denotes a lower bound; < an upper bound, using the logged limit in the same row."
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
    return float(text.lstrip("><").rstrip("x").replace(",", ""))


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
            weight = "bold" if value.startswith((">", "<")) else "normal"
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

    has_timeout = any(value.startswith((">", "<")) and not value.endswith("x") for row in rows for value in row)
    speedup_values = [row[index] for row in rows for index in speedup_columns]
    has_lower_bound = any(value.startswith((">", "<")) for value in speedup_values)
    has_double_timeout = any(value == "–" for value in speedup_values)
    notes = []
    is_ldbc = columns[0] == "Query"
    if columns[0] == "Graph":
        notes.extend((
            "Graph: F = forest; NT = normal tree (truebase); DT = deep tree (ultratall); WT = wide tree (ultrawide); number = node count;",
            "SNB/C = Comment, SNB/P = Place, SNB/T = Tagclass (SNB SF1).",
        ))
    notes.extend([
        "Runtimes: median of measured runs, in milliseconds (ms)." if is_ldbc else "Runtimes: median of measured runs.",
        "Speedup Dewey = Baseline / Dewey; Speedup Prepost = Baseline / Prepost.",
    ])
    if has_timeout:
        notes.append(TIMEOUT_NOTE)
    if has_lower_bound:
        notes.extend((BOUND_NOTE, "Bold values are derived from a timeout."))
    if has_double_timeout:
        notes.append('"–" denotes a timeout for both methods; no speedup bound can be determined.')
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


def load_ldbc_rows(path, log_directories=None):
    values = defaultdict(list)
    with path.open(newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            graph, method = split_graph(row["graph"].strip())
            if graph != "snb_sf1":
                raise ValueError(f"Unexpected LDBC graph: {graph}")
            values[(row["query"].strip(), method)].append(read_runtime(path, row, log_directories))

    medians = {}
    for key, runs in values.items():
        if not runs:
            raise ValueError(f"No runs found for {key}")
        medians[key] = median_runtime(runs, key)

    query_order = list(dict.fromkeys(query for query, _ in values))
    rows = []
    for query in query_order:
        times = {method: medians[(query, method)] for method in METHODS}
        speedups = [runtime_speedup(times["baseline"], times[method]) for method in ("dewey", "prepost")]
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
    add_timeout_arguments(parser)
    args = parser.parse_args()
    log_directories = timeout_directories(args.timeout_log_dir)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="runtime-tables-") as temp_dir_name:
        temp_dir = Path(temp_dir_name)
        page_pdfs = []
        with args.input.open(newline="", encoding="utf-8-sig") as handle:
            fieldnames = csv.DictReader(handle).fieldnames or []
        if "scenario" in fieldnames:
            groups = load_groups(args.input, log_directories)
            pages = [
                (title, make_rows(groups, query, scenarios), None, None)
                for title, query, scenarios in report_pages(groups)
            ]
            if not pages:
                raise ValueError("No supported query scenarios found")
        else:
            pages = [(
                "LDBC SNB SF1",
                load_ldbc_rows(args.input, log_directories),
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
