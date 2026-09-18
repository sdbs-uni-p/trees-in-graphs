#!/usr/bin/env python3
"""Create one selected Tree/LDBC comparison table for AGE, Kuzu, and Neo4j."""

import argparse
import math
import re
from datetime import datetime
from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING
import shutil
import subprocess
import tempfile
from pathlib import Path

from create_runtime_tables import add_timeout_arguments, timeout_directories, new_combined_directory, write_sources

from create_combined_runtime_tables import (
    SCENARIOS,
    SYSTEMS,
    load_ldbc_medians,
    load_medians,
    make_ldbc_rows,
    make_rows,
    svg_page,
    render_pdf,
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
    if re.fullmatch(r"(?:F|NT|DT|WT)\d+000", graph):
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
    if value.startswith(">"):
        return value
    return f"{float(value.replace(',', '')):,.1f}"


def rounded_speedup(value):
    if value == "–":
        return value
    if value.startswith((">", "<")):
        number = Decimal(value[1:].rstrip("x").replace(",", ""))
        step = Decimal(1).scaleb(max(-2, number.adjusted() - 1))
        rounded = number.quantize(step, rounding=ROUND_FLOOR if value[0] == ">" else ROUND_CEILING)
        return value[0] + rounded_speedup(f"{rounded}x")
    number = float(value.lstrip("><").rstrip("x").replace(",", ""))
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
    # Choose the display precision from the rounded value, so crossing a
    # boundary cannot produce both 1.00/1.0, 10.0/10, or 1,000/1.0K.
    rounded = float(rendered.rstrip("K").replace(",", ""))
    if rendered.endswith("K"):
        rounded *= 1000
    if rounded < 1:
        rendered = f"{rounded:.2f}"
    elif rounded < 10:
        rendered = f"{rounded:.1f}"
    elif rounded < 1000:
        rendered = f"{rounded:,.0f}"
    elif rounded < 10000:
        rendered = f"{rounded / 1000:.1f}K"
    else:
        rendered = f"{rounded / 1000:,.0f}K"
    return f"{rendered}x"


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
                plain_number = value.lstrip("><").replace(",", "")
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


INPUT_FOLDERS = {
    "age": "age", "kuzu": "kuzu", "neo4j": "neo4j",
    "age_ldbc": "age_ldbc", "kuzu_ldbc": "kuzu_ldbc", "neo4j_ldbc": "neo4j_ldbc",
}
OUTPUT_NAMES = {
    "detailed_output": "runtime_table_exact.pdf",
    "rounded_output": "runtime_table_rounded.pdf",
    "table_output": "runtime_table_compact.pdf",
}


def latest_input(directory):
    """Select by the timestamp in the folder name, not filesystem modification time."""
    candidates = []
    if directory.is_dir():
        for folder in directory.iterdir():
            match = re.fullmatch(r"(\d{8}_\d{6})(?:_.+)?", folder.name)
            if not folder.is_dir() or not match:
                continue
            try:
                timestamp = datetime.strptime(match[1], "%Y%m%d_%H%M%S")
            except ValueError:
                continue
            candidates.append((timestamp, folder.name, folder))
    if not candidates:
        selected = directory / "paper_results" / "runtimes.csv"
    else:
        # A suffix breaks ties deterministically (e.g. a merged folder after the raw run).
        selected = max(candidates)[2] / "runtimes.csv"
    if not selected.is_file():
        raise ValueError(f"Selected result directory has no runtimes.csv: {selected.parent}; supply an explicit input CSV")
    return selected


def parse_arguments(argv=None, results_root=None):
    results_root = results_root or Path(__file__).resolve().parents[1] / "results"
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--results", choices=("paper", "further"),
        help="Reproduce frozen paper (35 rows) or further (243 rows) results; cannot be combined with input overrides.",
    )
    for name, folder in INPUT_FOLDERS.items():
        parser.add_argument(
            "--" + name.replace("_", "-"), type=Path,
            help=f"Input CSV or paper (default: runtimes.csv in the latest timestamped results/{folder}/ folder, or paper_results if none).",
        )
    for name, filename in OUTPUT_NAMES.items():
        parser.add_argument(
            "--" + name.replace("_", "-"), nargs="?", const=True, type=Path,
            help=f"Generate this variant; optional PDF path (default: results/combined/<timestamp>/{filename}).",
        )
    add_timeout_arguments(parser)
    args = parser.parse_args(argv)
    try:
        if args.results and (any(getattr(args, name) is not None for name in INPUT_FOLDERS)
                             or args.timeout_log_dir):
            raise ValueError("--results selects a complete frozen input set; omit per-system inputs and --timeout-log-dir")
        for name, folder in INPUT_FOLDERS.items():
            if args.results == "further":
                setattr(args, name, results_root / "combined" / "further_results" / "inputs" / folder / "runtimes.csv")
            elif args.results == "paper" or getattr(args, name) == Path("paper"):
                setattr(args, name, results_root / folder / "paper_results" / "runtimes.csv")
            elif getattr(args, name) is None:
                setattr(args, name, latest_input(results_root / folder))
        generate_all = all(getattr(args, name) is None for name in OUTPUT_NAMES)
        default_directory = None
        for name, filename in OUTPUT_NAMES.items():
            value = getattr(args, name)
            if generate_all or value is True:
                if default_directory is None:
                    default_directory = new_combined_directory(results_root)
                setattr(args, name, default_directory / filename)
        outputs = [getattr(args, name).resolve() for name in OUTPUT_NAMES if getattr(args, name) is not None]
        if len(outputs) != len(set(outputs)):
            raise ValueError("Output variants must use different PDF paths")
    except ValueError as exc:
        parser.error(str(exc))
    return args


def main(argv=None):
    args = parse_arguments(argv)
    if args.results == "further":
        from create_further_runtime_tables import generate
        generate(
            {variant: getattr(args, option) for variant, option in (
                ("exact", "detailed_output"), ("rounded", "rounded_output"), ("compact", "table_output"),
            ) if getattr(args, option) is not None},
            input_directory=args.age.parent.parent.parent,
        )
        return
    log_directories = timeout_directories(args.timeout_log_dir)
    for name in INPUT_FOLDERS:
        print(f"{name}: {getattr(args, name)}")

    tree_medians = {
        "Apache AGE": load_medians(args.age, log_directories),
        "Kuzu": load_medians(args.kuzu, log_directories),
        "Neo4j": load_medians(args.neo4j, log_directories),
    }
    ldbc_medians = {
        "Apache AGE": load_ldbc_medians(args.age_ldbc, log_directories),
        "Kuzu": load_ldbc_medians(args.kuzu_ldbc, log_directories),
        "Neo4j": load_ldbc_medians(args.neo4j_ldbc, log_directories),
    }
    if set(tree_medians) != set(SYSTEMS) or set(ldbc_medians) != set(SYSTEMS):
        raise ValueError("Missing DBMS input")

    exact_rows = selected_tree_rows(tree_medians) + selected_ldbc_rows(ldbc_medians)
    if len(exact_rows) != 35:
        raise ValueError(f"Expected 35 selected rows, found {len(exact_rows)}")
    rounded = rounded_rows(exact_rows)
    compact = compact_rows(rounded)
    compact_timeout_notes = []
    for row_number, row in enumerate(exact_rows, start=1):
        for system_index, system in enumerate(SYSTEMS):
            offset = 3 + system_index * 5
            for method_index, method in ((1, "Dewey"), (2, "Prepost")):
                value = row[offset + method_index]
                if value.startswith(">"):
                    compact_timeout_notes.append(
                        f"Row {row_number} ({', '.join(row[:3])}), {system}: {method} timeout {value[1:]}."
                    )

    with tempfile.TemporaryDirectory(prefix="combined-overview-") as temp_name:
        temp = Path(temp_name)
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
        if any(value.startswith(">") and not value.endswith("x") for row in exact_rows for value in row):
            common_options["notes_override"] = tuple(
                note.replace("All times are in ms.", "Measured times are in ms; timeout units are shown in cells.")
                for note in common_options["notes_override"]
            )
        variants = (
            ("exact", args.detailed_output, exact_rows, ("B (ms)", "D", "P", "S_D", "S_P"), False),
            ("rounded", args.rounded_output, rounded, ("B (ms)", "D", "P", "S_D", "S_P"), False),
            ("compact", args.table_output, compact, ("B (ms)", "S_D", "S_P"), True),
        )
        for name, output, rows, subheadings, table_only in variants:
            if output is None:
                continue
            output.parent.mkdir(parents=True, exist_ok=True)
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
                    timeout_notes=compact_timeout_notes if table_only else (),
                ),
                encoding="utf-8",
            )
            render_pdf(svg, pdf)
            shutil.copyfile(pdf, output)
            print(output)
    write_sources(
        {name: getattr(args, name) for name in INPUT_FOLDERS},
        [getattr(args, name) for name in OUTPUT_NAMES if getattr(args, name) is not None],
        log_directories,
    )


if __name__ == "__main__":
    main()
