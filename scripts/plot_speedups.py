#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-only

import argparse
import csv
import glob
import math
import sys
import os
import re
import statistics
from collections import defaultdict
from pathlib import Path

TIMESTAMP_PATTERN = re.compile(r"^\d{8}_\d{6}$")

MPL_IMPORT_ERROR = None
try:
    import matplotlib.pyplot as plt
    import matplotlib.patches as patches
    import matplotlib.ticker as mticker
    from matplotlib.lines import Line2D
except ModuleNotFoundError as exc:
    MPL_IMPORT_ERROR = exc
    plt = None
    patches = None
    mticker = None
    Line2D = None

DB_ORDER = ["neo4j", "kuzu", "age"]
DB_LABELS = {
    "neo4j": "Neo4j",
    "kuzu": "Kuzu",
    "age": "AGE",
}

AGE_QUERY_NUMBER_BY_NAME = {}

AGE_SUFFIXES_ALL = [
    "baseline",
    "dewey",
    "prepost",
]

AGE_SUFFIXES_PLOT = [
    "dewey",
    "prepost",
]

OTHER_SUFFIXES_PLOT = [
    "dewey",
    "prepost",
]

PLOT_COLORS = {
    "dewey": "#FFFFB3",
    "prepost": "#BC80BDCC",
    "baseline": "#6A80B8",
    "baseline_band": "#DDE8FF",
}

SUFFIX_LABELS = {
    "dewey": "Dewey",
    "prepost": "PrePost",
}

FIG_HEIGHT = 5.2
FIG_WIDTH_PER_GRAPH = 1.5
LEGEND_FONT_SIZE = 16
LABEL_FONT_SIZE = 16
TICK_FONT_SIZE = 16
TIMEOUT_RUNTIME_MS = 300000.0
BASELINE_LINE_WIDTH = 3
BASELINE_LINE_ZORDER = 4
Y_MAX_VISUAL_HEADROOM_RATIO = 3.0 / 2.0

BAR_WIDTH = 0.39
DB_GAP_LEFT = 0.0
DB_GAP_RIGHT = 0.0
GRAPH_GAP_WITHIN = 0.16
GRAPH_GAP_BETWEEN = 0.39
SECTION_GAP = 0.58
UNDERFLOW_MARKER_THRESHOLD = 1e-1
UNDERFLOW_LABEL_FONT_SIZE = 16
UNDERFLOW_LABEL_MIN_GAP_PX = 6
UNDERFLOW_HIGH_LABEL_ARROW_SCALE = 2.0
UNDERFLOW_HIGH_LABEL_EXTRA_Y_POINTS = 10
UNDERFLOW_HIGH_LABEL_OPPOSITE_SHIFT_BAR_WIDTHS = 1.75
UNDERFLOW_PAIR_INWARD_SHIFT_BAR_WIDTHS = 0.2

X_MARGIN = 0.2

LAYOUT_LEFT = 0.11
LAYOUT_RIGHT = 0.99
LAYOUT_BOTTOM = 0.18
LAYOUT_TOP_INSIDE = 0.96
LAYOUT_TOP_OUTSIDE = 0.83
LEGEND_Y_ANCHOR_INSIDE = 1
LEGEND_Y_ANCHOR_OUTSIDE = 1

DEFAULT_DB_HATCHES = {
    "neo4j": "///",
    "kuzu": "...",
    "age": "",
}

DISPLAY_GRAPHS = [
    ("artificial_trees_ultrawide_100", "WT 1"),
    ("artificial_trees_ultrawide_1000", "WT 2"),
    ("artificial_trees_ultrawide_10000", "WT 3"),
    ("artificial_trees_ultratall_10000", "DT"),
    ("artificial_forests_40", "TF"),
    ("snb_sf1_comment", "SNB/C"),
    ("snb_sf1_place", "SNB/P"),
    ("snb_sf1_tagclass", "SNB/T"),
]

def parse_age_graph_variant(graph_name: str):
    for suffix in sorted(AGE_SUFFIXES_ALL, key=len, reverse=True):
        token = f"_{suffix}"
        if graph_name.endswith(token):
            return graph_name[: -len(token)], suffix
    return graph_name, "baseline"


def normalize_query(query_name: str, source: str) -> str:
    query_name = query_name.strip().lower()
    if source == "age":
        query_name = re.sub(r"^\d+_", "", query_name)

    if query_name == "check_if_ancestor_true":
        return "check_if_ancestor"

    if query_name.endswith("_true"):
        prefix = query_name[: -len("_true")]
        if prefix == "check_if_ancestor":
            return "check_if_ancestor"

    return query_name


def normalize_graph_name(name: str) -> str:
    name = name.strip().lower()
    if name.endswith(".json"):
        name = name[: -len(".json")]
    if name == "s_all_comment":
        return "snb_sf1_comment"
    if name == "s_all_place":
        return "snb_sf1_place"
    if name == "s_all_tagclass":
        return "snb_sf1_tagclass"
    if name == "comment":
        return "snb_sf1_comment"
    if name == "place":
        return "snb_sf1_place"
    if name == "tagclass":
        return "snb_sf1_tagclass"
    if name == "artificial_forest_40":
        return "artificial_forests_40"
    if name == "forests_40":
        return "artificial_forests_40"
    if name.startswith("truebase_"):
        return f"artificial_trees_{name}"
    if name.startswith("ultratall_"):
        return f"artificial_trees_{name}"
    if name.startswith("ultrawide_"):
        return f"artificial_trees_{name}"
    return name


def format_graph_label(name: str) -> str:
    return name


def tint_color(hex_color: str, factor: float) -> str:
    hex_color = hex_color.lstrip("#")
    r = int(hex_color[0:2], 16) / 255.0
    g = int(hex_color[2:4], 16) / 255.0
    b = int(hex_color[4:6], 16) / 255.0
    if factor >= 1.0:
        r = r + (1.0 - r) * (factor - 1.0)
        g = g + (1.0 - g) * (factor - 1.0)
        b = b + (1.0 - b) * (factor - 1.0)
    else:
        r *= factor
        g *= factor
        b *= factor
    r = min(1.0, max(0.0, r))
    g = min(1.0, max(0.0, g))
    b = min(1.0, max(0.0, b))
    return f"#{int(r * 255):02x}{int(g * 255):02x}{int(b * 255):02x}"


def parse_other_row(row):
    header_map = {key.strip().lower().lstrip("\ufeff"): key for key in row.keys()}

    def find_key(candidates):
        for cand in candidates:
            for key in header_map:
                if cand in key:
                    return header_map[key]
        return None

    graph_key = find_key(["graph name", "graph"])
    query_key = find_key(["query name", "query"])
    method_key = find_key(["annotation method", "method"])
    runtime_key = find_key(["average client-side runtime", "runtime"])

    if not graph_key or not query_key or not method_key or not runtime_key:
        return None

    graph = row.get(graph_key, "").strip()
    query = row.get(query_key, "").strip()
    method = row.get(method_key, "").strip().lower()
    runtime = row.get(runtime_key, "").strip()
    return graph, query, method, runtime


def parse_other_row_sequence(row):
    values = [str(value).strip() for value in row]
    if not values:
        return None

    if len(values) >= 5 and values[0].isdigit():
        values = values[1:]

    if len(values) < 4:
        return None

    graph, query, method, runtime = values[:4]
    return graph, query, method.lower(), runtime


def row_looks_like_other_header(row):
    lowered = [str(value).strip().lower().lstrip("\ufeff") for value in row]
    if not lowered:
        return False
    joined = " ".join(lowered)
    return (
        ("graph" in joined)
        and ("query" in joined)
        and (("annotation" in joined) or ("method" in joined))
        and ("runtime" in joined)
    )


def iter_csv_paths(path: str):
    if os.path.isdir(path):
        return sorted(glob.glob(os.path.join(path, "*.csv")))
    if any(ch in path for ch in "*?["):
        return sorted(glob.glob(path))
    return [path]


def load_age(csv_path: str):
    data = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: {"values": [], "has_timeout": False})))
    paths = iter_csv_paths(csv_path)
    if not paths:
        print(f"No CSV files found for AGE: {csv_path}", file=sys.stderr)
        return data
    for path in paths:
        with open(path, newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                graph = row.get("graph", "").strip()
                query = row.get("query", "").strip()
                runtime = row.get("runtime_ms", "").strip()
                if not graph or not query:
                    continue
                base, suffix = parse_age_graph_variant(graph)
                base = normalize_graph_name(base)
                query_number = extract_query_number(query)
                query = normalize_query(query, "age")
                if query_number is not None:
                    AGE_QUERY_NUMBER_BY_NAME.setdefault(query, query_number)
                if runtime == "":
                    data[query][base][suffix]["has_timeout"] = True
                    continue
                try:
                    runtime_ms = float(runtime)
                except ValueError:
                    continue
                data[query][base][suffix]["values"].append(runtime_ms)
    return data


def load_other(csv_path: str, source: str):
    data = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: {"values": [], "has_timeout": False})))
    paths = iter_csv_paths(csv_path)
    if not paths:
        print(f"No CSV files found for {source}: {csv_path}", file=sys.stderr)
        return data
    for path in paths:
        with open(path, newline="", encoding="utf-8") as handle:
            sample_rows = list(csv.reader(handle))

        if not sample_rows:
            continue

        if row_looks_like_other_header(sample_rows[0]):
            with open(path, newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    parsed = parse_other_row(row)
                    if not parsed:
                        continue
                    graph, query, method, runtime = parsed
                    if not graph or not query or not method:
                        continue
                    base = normalize_graph_name(graph)
                    query = normalize_query(query, source)
                    if runtime == "":
                        data[query][base][method]["has_timeout"] = True
                        continue
                    try:
                        runtime_ms = float(runtime)
                    except ValueError:
                        continue
                    data[query][base][method]["values"].append(runtime_ms)
        else:
            for row in sample_rows:
                parsed = parse_other_row_sequence(row)
                if not parsed:
                    continue
                graph, query, method, runtime = parsed
                if not graph or not query or not method:
                    continue
                base = normalize_graph_name(graph)
                query = normalize_query(query, source)
                if runtime == "":
                    data[query][base][method]["has_timeout"] = True
                    continue
                try:
                    runtime_ms = float(runtime)
                except ValueError:
                    continue
                data[query][base][method]["values"].append(runtime_ms)
    return data


def compute_medians(series):
    medians = {}
    for suffix, payload in series.items():
        values = payload.get("values", [])
        has_timeout = bool(payload.get("has_timeout", False))
        medians[suffix] = {
            "median": statistics.median(values) if values else None,
            "has_timeout": has_timeout,
            "has_value": bool(values),
        }
    return medians


def compute_speedups(medians_by_base):
    adjusted = {}
    for base, medians in medians_by_base.items():
        baseline_info = medians.get("baseline")
        if not baseline_info:
            adjusted[base] = {}
            continue
        adjusted[base] = {}
        baseline_value = baseline_info.get("median")
        baseline_timeout = baseline_info.get("has_timeout", False)

        for suffix, info in medians.items():
            if suffix == "baseline":
                continue

            value = info.get("median")
            value_timeout = info.get("has_timeout", False)

            if baseline_timeout and value_timeout:
                continue

            if baseline_timeout and value is not None and value > 0:
                lower_bound = TIMEOUT_RUNTIME_MS / value
                adjusted[base][suffix] = {
                    "kind": "timeout_overflow",
                    "lower_bound": lower_bound,
                }
                continue

            if baseline_value is None or baseline_value == 0:
                continue

            if value is None or value == 0:
                continue

            ratio = baseline_value / value
            adjusted[base][suffix] = {
                "kind": "value",
                "value": ratio,
            }
    return adjusted


def compute_group_gaps(display_entries, within_gap=0.5, between_gap=0.7):
    gaps = []
    for prev, curr in zip(display_entries, display_entries[1:]):
        prev_group = prev["label"][0]
        curr_group = curr["label"][0]
        gaps.append(within_gap if prev_group == curr_group else between_gap)
    return gaps


def compute_centers(start, group_width, gaps):
    centers = [start]
    current = start
    for gap in gaps:
        current += group_width + gap
        centers.append(current)
    return centers


def extract_query_number(query: str):
    match = re.match(r"^(\d+)", query.strip())
    if not match:
        return None
    return str(int(match.group(1)))


def infer_query_number_from_name(query: str):
    normalized = query.strip().lower()
    known = {
        "all_descendants": "1",
        "all_children": "2",
    }
    return known.get(normalized)


def resolve_query_number(query: str):
    direct = extract_query_number(query)
    if direct is not None:
        return direct
    normalized = query.strip().lower()
    learned = AGE_QUERY_NUMBER_BY_NAME.get(normalized)
    if learned is not None:
        return learned
    return infer_query_number_from_name(query)


def query_sort_key(query: str):
    query_number = resolve_query_number(query)
    if query_number is None:
        return (1, query)
    return (0, int(query_number), query)


def parse_label_shifts(raw_entries):
    shifts = defaultdict(list)
    for raw in raw_entries:
        parts = raw.split(":")
        if len(parts) not in (4, 5):
            raise ValueError(
                f"Invalid --label-shift '{raw}'. Expected QUERY:BAR:DX:DY or QUERY:BAR:DX:DY:CURV"
            )

        query_raw = parts[0].strip()
        bar_raw = parts[1].strip()
        dx_raw = parts[2].strip()
        dy_raw = parts[3].strip()
        curv_raw = parts[4].strip() if len(parts) == 5 else None
        if not query_raw.isdigit():
            raise ValueError(
                f"Invalid query in --label-shift '{raw}'. Query must be numeric (e.g. 01)."
            )
        bar_indices = []
        for bar_token in bar_raw.split("/"):
            bar_token = bar_token.strip()
            if bar_token == "":
                raise ValueError(f"Invalid BAR selector in --label-shift '{raw}'.")
            try:
                bar_indices.append(int(bar_token))
            except ValueError as exc:
                raise ValueError(
                    f"Invalid bar index in --label-shift '{raw}'."
                ) from exc

        if len(bar_indices) != len(set(bar_indices)):
            raise ValueError(
                f"Duplicate BAR indices in --label-shift '{raw}'."
            )

        try:
            dx = float(dx_raw)
            dy = float(dy_raw)
        except ValueError as exc:
            raise ValueError(
                f"Invalid DX/DY in --label-shift '{raw}'."
            ) from exc

        arrow_enabled = curv_raw is not None
        curvature = 0
        if curv_raw is not None:
            try:
                curvature = int(curv_raw)
            except ValueError as exc:
                raise ValueError(
                    f"Invalid CURV in --label-shift '{raw}'. CURV must be integer."
                ) from exc

        query_number = str(int(query_raw))
        shifts[query_number].append(
            {
                "selector": bar_raw,
                "bars": bar_indices,
                "dx": dx,
                "dy": dy,
                "curvature": curvature,
                "arrow_enabled": arrow_enabled,
            }
        )

    return {query_number: list(values) for query_number, values in shifts.items()}


def compute_label_slot_order(underflow_labels, timeout_markers):
    slots = sorted({item["slot"] for item in underflow_labels} | {item["slot"] for item in timeout_markers})
    return {slot: idx for idx, slot in enumerate(slots)}, len(slots)


def resolve_bar_index(raw_index, total_slots):
    resolved_index = raw_index if raw_index >= 0 else total_slots + raw_index
    if 0 <= resolved_index < total_slots:
        return resolved_index
    return None


def resolve_label_shift_configuration(query, query_number, query_shift_entries, slot_to_index, total_slots, slot_meta):
    index_to_slot = {idx: slot for slot, idx in slot_to_index.items()}
    single_shift_by_slot = {}
    grouped_specs = []
    debug_entries = []

    for entry in query_shift_entries:
        selector = entry["selector"]
        dx = entry["dx"]
        dy = entry["dy"]
        curvature = entry["curvature"]
        arrow_enabled = bool(entry.get("arrow_enabled", False))

        resolved_indices = []
        invalid_bars = []
        for raw_bar in entry["bars"]:
            resolved = resolve_bar_index(raw_bar, total_slots)
            if resolved is None:
                invalid_bars.append(raw_bar)
            else:
                resolved_indices.append(resolved)

        if invalid_bars:
            left_range = "empty" if total_slots == 0 else f"0..{total_slots - 1}"
            right_range = "empty" if total_slots == 0 else f"-1..-{total_slots}"
            debug_entries.append(
                {
                    "query": query,
                    "query_number": query_number,
                    "requested_selector": selector,
                    "dx": dx,
                    "dy": dy,
                    "curvature": curvature,
                    "arrow_enabled": arrow_enabled,
                    "status": "not-found",
                    "reason": (
                        f"bar index out of range: {invalid_bars}; "
                        f"valid left range {left_range}, valid right range {right_range}"
                    ),
                }
            )
            continue

        resolved_slots = [index_to_slot[idx] for idx in resolved_indices if idx in index_to_slot]
        if len(resolved_slots) != len(resolved_indices):
            debug_entries.append(
                {
                    "query": query,
                    "query_number": query_number,
                    "requested_selector": selector,
                    "dx": dx,
                    "dy": dy,
                    "curvature": curvature,
                    "arrow_enabled": arrow_enabled,
                    "status": "not-found",
                    "reason": "slot mapping missing",
                }
            )
            continue

        if len(resolved_slots) == 1:
            slot = resolved_slots[0]
            single_shift_by_slot[slot] = (dx, dy, curvature, arrow_enabled)
            meta = slot_meta.get(slot, {"db": "?", "suffix": "?", "label": "?"})
            debug_entries.append(
                {
                    "query": query,
                    "query_number": query_number,
                    "requested_selector": selector,
                    "resolved_selector": str(resolved_indices[0]),
                    "dx": dx,
                    "dy": dy,
                    "curvature": curvature,
                    "arrow_enabled": arrow_enabled,
                    "status": "ok",
                    "db": meta["db"],
                    "suffix": meta["suffix"],
                    "label": meta["label"],
                }
            )
            continue

        label_values = [slot_meta.get(slot, {}).get("label") for slot in resolved_slots]
        if any(value is None for value in label_values) or len(set(label_values)) != 1:
            debug_entries.append(
                {
                    "query": query,
                    "query_number": query_number,
                    "requested_selector": selector,
                    "dx": dx,
                    "dy": dy,
                    "curvature": curvature,
                    "arrow_enabled": arrow_enabled,
                    "status": "not-applied",
                    "reason": "labels are not equal",
                }
            )
            continue

        grouped_specs.append(
            {
                "slots": resolved_slots,
                "resolved_indices": resolved_indices,
                "dx": dx,
                "dy": dy,
                "curvature": curvature,
                "arrow_enabled": arrow_enabled,
                "label": label_values[0],
                "selector": selector,
            }
        )

        first_meta = slot_meta.get(resolved_slots[0], {"db": "?", "suffix": "?"})
        debug_entries.append(
            {
                "query": query,
                "query_number": query_number,
                "requested_selector": selector,
                "resolved_selector": "/".join(str(idx) for idx in resolved_indices),
                "dx": dx,
                "dy": dy,
                "curvature": curvature,
                "arrow_enabled": arrow_enabled,
                "status": "ok-grouped",
                "db": first_meta["db"],
                "suffix": first_meta["suffix"],
                "label": label_values[0],
            }
        )

    return single_shift_by_slot, grouped_specs, debug_entries


def offset_data_by_points(ax, x_data, y_data, dx_points, dy_points):
    if dx_points == 0 and dy_points == 0:
        return x_data, y_data

    x_px, y_px = ax.transData.transform((x_data, y_data))
    points_to_px = ax.figure.dpi / 72.0
    x_px += dx_points * points_to_px
    y_px += dy_points * points_to_px
    return ax.transData.inverted().transform((x_px, y_px))


def should_show_legend(query: str, legend_numbers):
    if legend_numbers is None:
        return True
    if not legend_numbers:
        return False
    query_number = resolve_query_number(query)
    return query_number in legend_numbers if query_number is not None else False


def get_active_dbs(args):
    active = []
    for db in DB_ORDER:
        if getattr(args, db):
            active.append(db)
    return active


def compute_separator_and_sides(active_dbs):
    has_neo4j = "neo4j" in active_dbs
    has_relational = any(db in active_dbs for db in ("kuzu", "age"))
    show_separator = has_neo4j and has_relational
    if show_separator:
        return True, ["neo4j"], [db for db in ("kuzu", "age") if db in active_dbs]
    return False, [], active_dbs[:]


def compute_db_hatches(active_dbs):
    if len(active_dbs) <= 1:
        return {db: "" for db in active_dbs}
    if len(active_dbs) == 2:
        color_only_db = "age" if "age" in active_dbs else "kuzu"
        hatches = {db: "" for db in active_dbs}
        hatched_db = [db for db in active_dbs if db != color_only_db][0]
        hatches[hatched_db] = "///"
        return hatches
    return {db: DEFAULT_DB_HATCHES.get(db, "") for db in active_dbs}


def make_combo_handle(db: str, suffix: str, db_hatches, include_db_name=True):
    label = f"{DB_LABELS[db]}/{SUFFIX_LABELS[suffix]}" if include_db_name else SUFFIX_LABELS[suffix]
    return patches.Patch(
        facecolor=PLOT_COLORS[suffix],
        edgecolor="#333333",
        linewidth=0.6,
        hatch=db_hatches.get(db, ""),
        label=label,
    )


def build_legend_handles(dbs, suffixes_by_db, db_hatches, include_db_name, legend_order):
    if legend_order == "suffix-db":
        handles = []
        ordered_suffixes = list(SUFFIX_LABELS.keys())
        for suffix in ordered_suffixes:
            for db in dbs:
                if suffix in suffixes_by_db[db]:
                    handles.append(
                        make_combo_handle(db, suffix, db_hatches, include_db_name=include_db_name)
                    )
        return handles

    handles = [
        make_combo_handle(db, suffix, db_hatches, include_db_name=include_db_name)
        for db in dbs
        for suffix in suffixes_by_db[db]
    ]
    if legend_order == "reverse":
        handles.reverse()
    return handles


def with_baseline_handle(handles, baseline_legend_position):
    baseline_handle = make_baseline_line_handle()
    items = list(handles)
    if not items:
        return [baseline_handle]

    if baseline_legend_position is None:
        return items + [baseline_handle]

    position = int(baseline_legend_position)
    if position < 0:
        position = len(items) + 1 + position
    position = max(0, min(position, len(items)))
    return items[:position] + [baseline_handle] + items[position:]


def make_baseline_line_handle():
    return Line2D(
        [0],
        [0],
        color=PLOT_COLORS["baseline"],
        linewidth=BASELINE_LINE_WIDTH,
        linestyle="-",
        label="Baseline",
    )


def make_empty_legend_handle():
    return Line2D(
        [],
        [],
        linestyle="None",
        linewidth=0.0,
        alpha=0.0,
        label="",
    )


def apply_fixed_layout(fig, top=None):
    fig.subplots_adjust(
        left=LAYOUT_LEFT,
        right=LAYOUT_RIGHT,
        bottom=LAYOUT_BOTTOM,
        top=LAYOUT_TOP_INSIDE if top is None else top,
    )


def compute_cropped_limits_and_top(query, global_y_limits, crop_query_max, layout_top_default):
    y_min, global_y_max = global_y_limits
    query_number = resolve_query_number(query)
    target_y_max = crop_query_max.get(query_number) if query_number is not None else None
    if target_y_max is None:
        return global_y_limits, layout_top_default

    forced_y_max = max(14.0, target_y_max)
    if forced_y_max >= global_y_max:
        return (y_min, forced_y_max), layout_top_default

    full_decades = math.log10(global_y_max / y_min)
    cropped_decades = math.log10(forced_y_max / y_min)
    if full_decades <= 0 or cropped_decades <= 0:
        return (y_min, forced_y_max), layout_top_default

    full_axis_height = layout_top_default - LAYOUT_BOTTOM
    cropped_axis_height = full_axis_height * (cropped_decades / full_decades)
    top_for_query = LAYOUT_BOTTOM + cropped_axis_height

    return (y_min, forced_y_max), top_for_query


def compute_global_y_limits(all_data_by_db, active_dbs, suffixes_by_db):
    max_value = 1.0
    for db in active_dbs:
        db_queries = all_data_by_db.get(db, {})
        for query_data in db_queries.values():
            medians_by_base = {
                base: compute_medians(series)
                for base, series in query_data.items()
            }
            speedups = compute_speedups(medians_by_base)
            for base_values in speedups.values():
                for suffix in suffixes_by_db[db]:
                    payload = base_values.get(suffix)
                    if not payload:
                        continue

                    if payload.get("kind") == "timeout_overflow":
                        lower_bound = payload.get("lower_bound")
                        if lower_bound is None or math.isnan(lower_bound) or lower_bound <= 0:
                            continue
                        exponent = max(0, int(math.floor(math.log10(lower_bound))))
                        timeout_height = 10 ** exponent
                        max_value = max(max_value, timeout_height)
                        continue

                    if payload.get("kind") == "value":
                        value = payload.get("value")
                        if value is None or math.isnan(value):
                            continue
                        max_value = max(max_value, value)

    y_min = 0.07
    y_max = max(14.0, max_value * Y_MAX_VISUAL_HEADROOM_RATIO)
    return y_min, y_max


def compute_query_y_limits(speedups_by_db, suffixes_by_db):
    max_value = 1.0
    for db, base_map in speedups_by_db.items():
        for suffix_map in base_map.values():
            for suffix in suffixes_by_db[db]:
                payload = suffix_map.get(suffix)
                if not payload:
                    continue

                if payload.get("kind") == "timeout_overflow":
                    lower_bound = payload.get("lower_bound")
                    if lower_bound is None or math.isnan(lower_bound) or lower_bound <= 0:
                        continue
                    exponent = max(0, int(math.floor(math.log10(lower_bound))))
                    timeout_height = 10 ** exponent
                    max_value = max(max_value, timeout_height)
                    continue

                if payload.get("kind") == "value":
                    value = payload.get("value")
                    if value is None or math.isnan(value):
                        continue
                    max_value = max(max_value, value)

    y_min = 0.07
    y_max = max(14.0, max_value * Y_MAX_VISUAL_HEADROOM_RATIO)
    return y_min, y_max


def compute_inside_legend_special_ymax(max_visible_value):
    if max_visible_value is None or max_visible_value <= 0:
        return None

    base_top = max(14.0, 1.2 * max_visible_value)
    extra_decades = 1.64
    return base_top * (10 ** extra_decades)


def underflow_label_exponent(value: float) -> int:
    if value <= 0:
        return 1
    return max(1, int(math.ceil(-math.log10(value)) - 1))


def compute_required_fig_height(
    base_fig_height,
    y_min,
    final_y_max,
    global_y_max,
    layout_top_default,
    layout_top_for_query,
):
    full_decades = math.log10(global_y_max / y_min)
    final_decades = math.log10(final_y_max / y_min)
    if full_decades <= 0 or final_decades <= 0:
        return base_fig_height

    reference_axis_height_in = base_fig_height * (layout_top_default - LAYOUT_BOTTOM)
    required_axis_height_in = reference_axis_height_in * (final_decades / full_decades)
    axis_fraction = max(1e-6, layout_top_for_query - LAYOUT_BOTTOM)
    return required_axis_height_in / axis_fraction


def log_query_values(query, speedups_by_db, display_entries, suffixes_by_db):
    display_bases = {entry["base"] for entry in display_entries}

    ordered_dbs = [db for db in DB_ORDER if db in speedups_by_db]
    for db in ordered_dbs:
        base_map = speedups_by_db.get(db, {})
        for base in sorted(base_map.keys()):
            suffix_map = base_map.get(base, {})
            for suffix in suffixes_by_db[db]:
                payload = suffix_map.get(suffix)
                if not payload:
                    continue

                if payload.get("kind") == "timeout_overflow":
                    lower_bound = payload.get("lower_bound")
                    if lower_bound is None or math.isnan(lower_bound):
                        continue
                    render_mode = "timeout-overflow"
                    value_repr = f">{lower_bound:.6f}"
                    print(
                        f"value query={query} graph={base} db={db} suffix={suffix} "
                        f"value={value_repr} render={render_mode}"
                    )
                    continue

                value = payload.get("value")
                if value is None or math.isnan(value):
                    continue

                if base not in display_bases:
                    render_mode = "not-in-display"
                elif value < UNDERFLOW_MARKER_THRESHOLD:
                    render_mode = "v-marker"
                else:
                    render_mode = "bar"

                print(
                    f"value query={query} graph={base} db={db} suffix={suffix} "
                    f"value={value:.6f} render={render_mode}"
                )


def compute_x_fraction(x_data, x_min, x_max):
    return (x_data - x_min) / (x_max - x_min)


def bboxes_overlap_with_gap(bbox_a, bbox_b, gap_px):
    return not (
        (bbox_a.x1 + gap_px) < bbox_b.x0
        or (bbox_b.x1 + gap_px) < bbox_a.x0
        or (bbox_a.y1 + gap_px) < bbox_b.y0
        or (bbox_b.y1 + gap_px) < bbox_a.y0
    )


def region_anchor_x(region_left, region_right, align):
    width = region_right - region_left
    if isinstance(align, (int, float)):
        fraction = min(1.0, max(0.0, float(align)))
        return region_left + fraction * width
    if align == "left":
        return region_left + 0.10 * width
    if align == "right":
        return region_left + 0.90 * width
    return region_left + 0.50 * width


def legend_loc(placement, align):
    if isinstance(align, (int, float)):
        return "lower center" if placement == "outside" else "upper center"
    if placement == "outside":
        return {
            "left": "lower left",
            "center": "lower center",
            "right": "lower right",
        }[align]
    return {
        "left": "upper left",
        "center": "upper center",
        "right": "upper right",
    }[align]


def build_output_db_suffix(active_dbs):
    if set(active_dbs) == set(DB_ORDER):
        return ""
    return "_" + "_".join(active_dbs)


def build_layout(display_entries, active_dbs, suffixes_by_db):
    show_separator, left_dbs, right_dbs = compute_separator_and_sides(active_dbs)

    db_group_widths = {
        db: len(suffixes_by_db[db]) * BAR_WIDTH for db in active_dbs
    }
    group_gaps = compute_group_gaps(
        display_entries,
        within_gap=GRAPH_GAP_WITHIN,
        between_gap=GRAPH_GAP_BETWEEN,
    )

    left_graph_group_width = 0.0
    if left_dbs:
        left_graph_group_width = (
            sum(db_group_widths[db] for db in left_dbs)
            + (len(left_dbs) - 1) * DB_GAP_LEFT
        )

    right_graph_group_width = (
        sum(db_group_widths[db] for db in right_dbs)
        + (len(right_dbs) - 1) * DB_GAP_RIGHT
    )

    left_block_width = (
        len(display_entries) * left_graph_group_width + sum(group_gaps)
        if left_dbs
        else 0.0
    )
    right_block_width = len(display_entries) * right_graph_group_width + sum(group_gaps)

    if show_separator:
        total_right = left_block_width + SECTION_GAP + right_block_width
        left_start = left_graph_group_width / 2
        right_start = left_block_width + SECTION_GAP + right_graph_group_width / 2
    else:
        total_right = right_block_width
        left_start = None
        right_start = right_graph_group_width / 2

    return {
        "show_separator": show_separator,
        "left_dbs": left_dbs,
        "right_dbs": right_dbs,
        "db_group_widths": db_group_widths,
        "group_gaps": group_gaps,
        "left_graph_group_width": left_graph_group_width,
        "right_graph_group_width": right_graph_group_width,
        "left_block_width": left_block_width,
        "right_block_width": right_block_width,
        "total_right": total_right,
        "left_start": left_start,
        "right_start": right_start,
    }


def plot_query(
    query,
    data_by_db,
    out_dir,
    y_limits,
    active_dbs,
    suffixes_by_db,
    db_hatches,
    output_db_suffix,
    crop_query_max,
    legend_numbers,
    legend_placement,
    legend_align,
    legend_columns,
    legend_order,
    baseline_legend_position,
    label_shifts_by_query,
    baseline_label_placement,
):
    base_values = {}
    for db in active_dbs:
        medians_by_base = {
            base: compute_medians(data_by_db.get(db, {}).get(base, {}))
            for base in data_by_db.get(db, {})
        }
        base_values[db] = compute_speedups(medians_by_base)

    display_entries = []
    for base, label in DISPLAY_GRAPHS:
        display_entries.append({
            "base": base,
            "label": label,
        })

    speedups_by_db = {db: base_values.get(db, {}) for db in active_dbs}

    log_query_values(query, speedups_by_db, display_entries, suffixes_by_db)

    fig, ax = plt.subplots(
        figsize=(max(11, len(display_entries) * FIG_WIDTH_PER_GRAPH), FIG_HEIGHT)
    )

    layout = build_layout(display_entries, active_dbs, suffixes_by_db)

    x_positions = []
    x_labels = []

    layout_top_default = LAYOUT_TOP_OUTSIDE if legend_placement == "outside" else LAYOUT_TOP_INSIDE
    layout_top_for_query = layout_top_default
    query_y_limits = compute_query_y_limits(speedups_by_db, suffixes_by_db)
    active_y_limits = (y_limits[0], query_y_limits[1])
    query_number = resolve_query_number(query)
    query_label_shifts = label_shifts_by_query.get(query_number, []) if query_number is not None else []
    if query_number is not None and query_number in crop_query_max:
        target_y_max = max(14.0, crop_query_max[query_number])
        active_y_limits = (active_y_limits[0], target_y_max)
    show_legend_for_query = should_show_legend(query, legend_numbers)

    tiny_markers = []
    timeout_overflow_markers = []
    slot_meta = {}
    out_of_axes_artists = []
    drawn_bar_tops = []
    bar_slot_counter = 0
    def draw_block(block_dbs, block_centers, block_width, gap_between_dbs):
        nonlocal bar_slot_counter
        for base_idx, entry in enumerate(display_entries):
            base = entry["base"]
            label = entry["label"]
            base_center = block_centers[base_idx]
            cursor = base_center - block_width / 2

            for db in block_dbs:
                suffixes = suffixes_by_db[db]
                group_width = layout["db_group_widths"][db]
                group_center = cursor + group_width / 2

                for s_idx, suffix in enumerate(suffixes):
                    slot_id = bar_slot_counter
                    bar_slot_counter += 1
                    offset = (s_idx - (len(suffixes) - 1) / 2) * BAR_WIDTH
                    payload = speedups_by_db[db].get(base, {}).get(suffix)
                    if not payload:
                        continue

                    if payload.get("kind") == "timeout_overflow":
                        lower_bound = payload.get("lower_bound")
                        if lower_bound is None or math.isnan(lower_bound) or lower_bound <= 0:
                            continue

                        plot_x = group_center + offset
                        exponent = max(0, int(math.floor(math.log10(lower_bound))))
                        timeout_height = 10 ** exponent
                        bar_top = min(timeout_height, active_y_limits[1])

                        ax.bar(
                            plot_x,
                            bar_top,
                            width=BAR_WIDTH,
                            color=PLOT_COLORS[suffix],
                            edgecolor="#333333",
                            linewidth=0.4,
                            hatch=db_hatches.get(db, ""),
                            zorder=2,
                        )
                        drawn_bar_tops.append((plot_x, bar_top))

                        timeout_overflow_markers.append({
                            "x": plot_x,
                            "y": bar_top,
                            "exp": exponent,
                            "color": PLOT_COLORS[suffix],
                            "db": db,
                            "suffix": suffix,
                            "slot": slot_id,
                        })
                        slot_meta[slot_id] = {
                            "db": DB_LABELS[db],
                            "suffix": SUFFIX_LABELS[suffix],
                            "label": rf">10^{exponent}",
                        }
                        continue

                    value = payload.get("value")
                    if value is None or math.isnan(value):
                        continue

                    color = PLOT_COLORS[suffix]
                    plot_value = value
                    if plot_value < UNDERFLOW_MARKER_THRESHOLD:
                        exponent = underflow_label_exponent(plot_value)
                        tiny_markers.append({
                            "x": group_center + offset,
                            "color": color,
                            "db": db,
                            "suffix": suffix,
                            "exponent": exponent,
                            "slot": slot_id,
                        })
                        slot_meta[slot_id] = {
                            "db": DB_LABELS[db],
                            "suffix": SUFFIX_LABELS[suffix],
                            "label": rf"<10^-{exponent}",
                        }
                        continue

                    ax.bar(
                        group_center + offset,
                        plot_value,
                        width=BAR_WIDTH,
                        color=color,
                        edgecolor="#333333",
                        linewidth=0.4,
                        hatch=db_hatches.get(db, ""),
                    )
                    drawn_bar_tops.append((group_center + offset, plot_value))

                cursor += group_width + gap_between_dbs

            x_positions.append(base_center)
            x_labels.append(format_graph_label(label))

    if layout["left_dbs"]:
        left_centers = compute_centers(
            layout["left_start"],
            layout["left_graph_group_width"],
            layout["group_gaps"],
        )
        draw_block(layout["left_dbs"], left_centers, layout["left_graph_group_width"], DB_GAP_LEFT)

    right_centers = compute_centers(
        layout["right_start"],
        layout["right_graph_group_width"],
        layout["group_gaps"],
    )
    draw_block(layout["right_dbs"], right_centers, layout["right_graph_group_width"], DB_GAP_RIGHT)

    if layout["show_separator"]:
        separator_x = layout["left_block_width"] + SECTION_GAP / 2
        ax.axvline(
            separator_x,
            color="#888888",
            linewidth=0.9,
            alpha=0.6,
            zorder=0.5,
        )

    ax.axhspan(0.9, 1.1, color=PLOT_COLORS["baseline_band"], alpha=0.4, zorder=0)
    ax.axhline(1.0, color=PLOT_COLORS["baseline"], linewidth=BASELINE_LINE_WIDTH, zorder=BASELINE_LINE_ZORDER)
    ax.set_xlabel("")
    ax.set_ylabel("Speedup", fontsize=LABEL_FONT_SIZE)
    ax.set_yscale("log")
    ax.set_ylim(active_y_limits[0], active_y_limits[1])
    ax.yaxis.set_major_locator(mticker.LogLocator(base=10.0))
    ax.yaxis.set_major_formatter(mticker.LogFormatterMathtext(base=10.0))
    ax.yaxis.set_minor_locator(mticker.LogLocator(base=10.0, subs=(2, 3, 4, 5, 6, 7, 8, 9), numticks=100))
    ax.yaxis.set_minor_formatter(mticker.NullFormatter())
    ax.set_xticks(x_positions)
    ax.set_xticklabels(x_labels, rotation=0, ha="center", fontsize=TICK_FONT_SIZE)
    ax.tick_params(axis="y", labelsize=TICK_FONT_SIZE)
    ax.grid(axis="y", which="major", linestyle="--", alpha=0.4)
    ax.grid(axis="y", which="minor", linestyle=":", alpha=0.22)

    marker_tip = active_y_limits[0] * 1.04
    marker_base = active_y_limits[0] * 2.96
    marker_half_width = BAR_WIDTH * 0.50
    underflow_labels = []
    for marker_info in tiny_markers:
        x_val = marker_info["x"]
        color = marker_info["color"]
        marker_db = marker_info["db"]
        exponent = marker_info["exponent"]
        marker = patches.Polygon(
            [
                (x_val - marker_half_width, marker_base),
                (x_val + marker_half_width, marker_base),
                (x_val, marker_tip),
            ],
            closed=True,
            facecolor=color,
            edgecolor="#333333",
            linewidth=0.5,
            hatch=db_hatches.get(marker_db, ""),
            zorder=3,
        )
        ax.add_patch(marker)
        underflow_labels.append({
            "x": x_val,
            "top_y": marker_base,
            "text": rf"$<10^{{-{exponent}}}$",
            "slot": marker_info["slot"],
        })

    slot_to_index, total_label_slots = compute_label_slot_order(underflow_labels, timeout_overflow_markers)
    single_shift_by_slot, grouped_shift_specs, shift_debug_entries = resolve_label_shift_configuration(
        query,
        query_number,
        query_label_shifts,
        slot_to_index,
        total_label_slots,
        slot_meta,
    )
    grouped_slots = {slot for spec in grouped_shift_specs for slot in spec["slots"]}
    label_default_positions = {}

    if underflow_labels:
        sorted_labels = sorted(underflow_labels, key=lambda entry: entry["slot"])
        for item in sorted_labels:
            x_val = item["x"]
            top_y = item["top_y"]
            label_text = item["text"]
            slot = item["slot"]

            base_x = x_val
            base_y = top_y * 1.05
            label_default_positions[slot] = {
                "base_x": base_x,
                "base_y": base_y,
                "anchor_x": x_val,
                "anchor_y": top_y,
                "text": label_text,
            }

            if slot in grouped_slots:
                continue

            shift_dx, shift_dy, shift_curvature, shift_arrow_enabled = single_shift_by_slot.get(
                slot,
                (0.0, 0.0, 0, False),
            )
            label_x, label_y = offset_data_by_points(ax, base_x, base_y, shift_dx, shift_dy)

            if shift_arrow_enabled:
                annotation = ax.annotate(
                    label_text,
                    xy=(x_val, top_y),
                    xytext=(label_x, label_y),
                    textcoords="data",
                    ha="center",
                    va="bottom",
                    fontsize=UNDERFLOW_LABEL_FONT_SIZE,
                    zorder=6,
                    annotation_clip=False,
                    arrowprops=dict(
                        arrowstyle="->",
                        color="#333333",
                        linewidth=0.8,
                        connectionstyle=f"arc3,rad={shift_curvature / 10.0}",
                    ),
                )
                out_of_axes_artists.append(annotation)
            else:
                artist = ax.text(
                    label_x,
                    label_y,
                    label_text,
                    ha="center",
                    va="bottom",
                    fontsize=UNDERFLOW_LABEL_FONT_SIZE,
                    zorder=6,
                )
                out_of_axes_artists.append(artist)

    sorted_timeout_markers = sorted(timeout_overflow_markers, key=lambda item: item["x"])

    for marker in sorted_timeout_markers:
        x_val = marker["x"]
        y_val = marker["y"]
        color = marker["color"]
        ax.plot(
            [x_val],
            [y_val],
            marker="^",
            markersize=8,
            markerfacecolor=color,
            markeredgecolor="#333333",
            markeredgewidth=0.6,
            linestyle="None",
            clip_on=False,
            zorder=5,
        )

    for marker in sorted_timeout_markers:
        exponent = marker["exp"]
        group_slot = marker["slot"]

        center_x = marker["x"]
        highest_marker_y = marker["y"]
        default_label_x, default_label_y = offset_data_by_points(ax, center_x, highest_marker_y, 0.0, 6.0)
        if default_label_y >= active_y_limits[1] * 0.96:
            default_label_y = active_y_limits[1] * 0.95

        label_default_positions[group_slot] = {
            "base_x": default_label_x,
            "base_y": default_label_y,
            "anchor_x": marker["x"],
            "anchor_y": marker["y"],
            "text": rf"$>10^{{{exponent}}}$",
        }

        if group_slot in grouped_slots:
            continue

        shift_dx, shift_dy, shift_curvature, shift_arrow_enabled = single_shift_by_slot.get(
            group_slot,
            (0.0, 0.0, 0, False),
        )

        if shift_arrow_enabled:
            label_artist = ax.annotate(
                rf"$>10^{{{exponent}}}$",
                xy=(marker["x"], marker["y"]),
                xytext=(shift_dx, 6.0 + shift_dy),
                textcoords="offset points",
                ha="center",
                va="bottom",
                fontsize=UNDERFLOW_LABEL_FONT_SIZE,
                color="#222222",
                arrowprops=dict(
                    arrowstyle="->",
                    color="#333333",
                    linewidth=0.8,
                    connectionstyle=f"arc3,rad={shift_curvature / 10.0}",
                ),
                annotation_clip=False,
                zorder=6,
            )
            out_of_axes_artists.append(label_artist)
        else:
            label_artist = ax.annotate(
                rf"$>10^{{{exponent}}}$",
                xy=(marker["x"], marker["y"]),
                xytext=(shift_dx, 6.0 + shift_dy),
                textcoords="offset points",
                ha="center",
                va="bottom",
                fontsize=UNDERFLOW_LABEL_FONT_SIZE,
                color="#222222",
                annotation_clip=False,
                zorder=6,
            )
            out_of_axes_artists.append(label_artist)

    for spec in grouped_shift_specs:
        members = [label_default_positions.get(slot) for slot in spec["slots"]]
        members = [member for member in members if member is not None]
        if len(members) != len(spec["slots"]):
            continue

        center_x = sum(member["base_x"] for member in members) / len(members)
        center_y = sum(member["base_y"] for member in members) / len(members)

        shift_dx = spec["dx"]
        shift_dy = spec["dy"]
        shift_curvature = spec["curvature"]
        shift_arrow_enabled = bool(spec.get("arrow_enabled", False))
        label_x, label_y = offset_data_by_points(ax, center_x, center_y, shift_dx, shift_dy)
        label_text = members[0]["text"]

        if shift_arrow_enabled:
            label_artist = ax.text(
                label_x,
                label_y,
                label_text,
                ha="center",
                va="bottom",
                fontsize=UNDERFLOW_LABEL_FONT_SIZE,
                color="#222222",
                clip_on=False,
                zorder=6,
            )
            out_of_axes_artists.append(label_artist)

            for member in members:
                arrow = ax.annotate(
                    "",
                    xy=(member["anchor_x"], member["anchor_y"]),
                    xytext=(label_x, label_y),
                    textcoords="data",
                    arrowprops=dict(
                        arrowstyle="->",
                        color="#333333",
                        linewidth=0.8,
                        connectionstyle=f"arc3,rad={shift_curvature / 10.0}",
                    ),
                    annotation_clip=False,
                    zorder=5.8,
                )
                out_of_axes_artists.append(arrow)
        else:
            label_artist = ax.text(
                label_x,
                label_y,
                label_text,
                ha="center",
                va="bottom",
                fontsize=UNDERFLOW_LABEL_FONT_SIZE,
                color="#222222",
                clip_on=False,
                zorder=6,
            )
            out_of_axes_artists.append(label_artist)

    ax.set_xlim(-X_MARGIN, layout["total_right"] + X_MARGIN)
    x_min, x_max = ax.get_xlim()
    show_baseline_text_on_line = baseline_label_placement == "line"
    show_baseline_in_legend = baseline_label_placement == "legend"
    if show_baseline_text_on_line:
        baseline_x = x_min + 0.02 * (x_max - x_min)
        ax.text(
            baseline_x,
            1.03,
            "Baseline",
            fontsize=LABEL_FONT_SIZE,
            va="bottom",
            ha="left",
        )

    apply_fixed_layout(fig, top=layout_top_for_query)

    if show_legend_for_query:
        include_db_name = len(active_dbs) > 1
        y_anchor = LEGEND_Y_ANCHOR_OUTSIDE if legend_placement == "outside" else LEGEND_Y_ANCHOR_INSIDE
        loc = legend_loc(legend_placement, legend_align)
        legend_artists = []

        if layout["show_separator"]:
            left_region_left = 0.0
            left_region_right = layout["left_block_width"]
            right_region_left = layout["left_block_width"] + SECTION_GAP
            right_region_right = layout["total_right"]

            left_anchor_x_data = region_anchor_x(left_region_left, left_region_right, legend_align)
            right_anchor_x_data = region_anchor_x(right_region_left, right_region_right, legend_align)
            left_anchor_x = compute_x_fraction(left_anchor_x_data, x_min, x_max)
            right_anchor_x = compute_x_fraction(right_anchor_x_data, x_min, x_max)

            left_handles = [
                handle for handle in build_legend_handles(
                    layout["left_dbs"],
                    suffixes_by_db,
                    db_hatches,
                    include_db_name,
                    legend_order,
                )
            ]
            if show_baseline_in_legend:
                left_handles = with_baseline_handle(left_handles, baseline_legend_position)
            right_handles = [
                handle for handle in build_legend_handles(
                    layout["right_dbs"],
                    suffixes_by_db,
                    db_hatches,
                    include_db_name,
                    legend_order,
                )
            ]

            if left_handles:
                left_legend = ax.legend(
                    left_handles,
                    [h.get_label() for h in left_handles],
                    ncol=max(1, min(legend_columns, len(left_handles))),
                    fontsize=LEGEND_FONT_SIZE,
                    loc=loc,
                    bbox_to_anchor=(left_anchor_x, y_anchor),
                    frameon=False,
                    columnspacing=1.0,
                    handletextpad=0.6,
                    title="Native GDBMS",
                    title_fontsize=LABEL_FONT_SIZE,
                )
                ax.add_artist(left_legend)
                legend_artists.append(left_legend)

            if right_handles:
                right_legend = ax.legend(
                    right_handles,
                    [h.get_label() for h in right_handles],
                    ncol=max(1, min(legend_columns, len(right_handles))),
                    fontsize=LEGEND_FONT_SIZE,
                    loc=loc,
                    bbox_to_anchor=(right_anchor_x, y_anchor),
                    frameon=False,
                    columnspacing=1.2,
                    handletextpad=0.6,
                    title="Relational backends",
                    title_fontsize=LABEL_FONT_SIZE,
                )
                ax.add_artist(right_legend)
                legend_artists.append(right_legend)
        else:
            region_left = 0.0
            region_right = layout["total_right"]
            anchor_x_data = region_anchor_x(region_left, region_right, legend_align)
            anchor_x = compute_x_fraction(anchor_x_data, x_min, x_max)
            handles = build_legend_handles(
                layout["right_dbs"],
                suffixes_by_db,
                db_hatches,
                include_db_name,
                legend_order,
            )
            if show_baseline_in_legend:
                handles = with_baseline_handle(handles, baseline_legend_position)
            if handles:
                legend = ax.legend(
                    handles,
                    [h.get_label() for h in handles],
                    ncol=max(1, min(legend_columns, len(handles))),
                    fontsize=LEGEND_FONT_SIZE,
                    loc=loc,
                    bbox_to_anchor=(anchor_x, y_anchor),
                    frameon=False,
                    columnspacing=1.2,
                    handletextpad=0.6,
                )
                ax.add_artist(legend)
                legend_artists.append(legend)
    else:
        legend_artists = []

    required_fig_height = compute_required_fig_height(
        FIG_HEIGHT,
        active_y_limits[0],
        active_y_limits[1],
        y_limits[1],
        layout_top_default,
        layout_top_for_query,
    )
    fig_width = max(11, len(display_entries) * FIG_WIDTH_PER_GRAPH)
    fig.set_size_inches(fig_width, required_fig_height, forward=True)

    apply_fixed_layout(fig, top=layout_top_for_query)
    file_name = f"speedup_{sanitize_filename(query)}{output_db_suffix}.pdf"
    output_path = os.path.join(out_dir, file_name)
    save_kwargs = {}
    if legend_placement == "outside":
        save_kwargs = {
            "bbox_inches": "tight",
            "pad_inches": 0.08,
            "bbox_extra_artists": legend_artists + out_of_axes_artists,
        }
    elif legend_placement == "inside":
        save_kwargs = {
            "bbox_inches": "tight",
            "pad_inches": 0.04,
            "bbox_extra_artists": out_of_axes_artists,
        }
    fig.savefig(output_path, **save_kwargs)
    fig_size = fig.get_size_inches()
    plt.close(fig)
    return float(fig_size[0]), float(fig_size[1]), shift_debug_entries


def sanitize_filename(name: str) -> str:
    keep = []
    for ch in name:
        if ch.isalnum() or ch in ("_", "-", "."):
            keep.append(ch)
        else:
            keep.append("_")
    return "".join(keep)


def infer_source_tag(path_value: str) -> str:
    abs_path = os.path.abspath(path_value)
    parts = [part for part in re.split(r"[\\/]", abs_path) if part]
    parts_lower = [part.lower() for part in parts]

    for part in reversed(parts):
        if TIMESTAMP_PATTERN.match(part):
            return part

    if "paper_results" in parts_lower:
        return "paper_results"

    base_name = os.path.basename(abs_path)
    stem, ext = os.path.splitext(base_name)
    if ext:
        parent = os.path.basename(os.path.dirname(abs_path))
        return parent or stem or "input"
    return base_name or "input"


def select_latest_results_dir(base_dir: Path):
    if not base_dir.exists():
        return None

    timestamp_dirs = [
        path for path in base_dir.iterdir()
        if path.is_dir() and TIMESTAMP_PATTERN.match(path.name)
    ]
    if timestamp_dirs:
        return sorted(timestamp_dirs, key=lambda path: path.name, reverse=True)[0]

    paper_dir = base_dir / "paper_results"
    if paper_dir.is_dir():
        return paper_dir

    any_dirs = [path for path in base_dir.iterdir() if path.is_dir()]
    if any_dirs:
        return sorted(any_dirs, key=lambda path: path.stat().st_mtime, reverse=True)[0]

    return None


def resolve_input_path(raw_value: str, source: str, repo_root: Path) -> str:
    token = (raw_value or "").strip().lower()
    if token in {"paper", "latest"}:
        source_root = repo_root / "results" / source
        if token == "paper":
            selected = source_root / "paper_results"
            if not selected.is_dir():
                raise SystemExit(f"{source.upper()} paper results directory not found: {selected}")
        else:
            selected = select_latest_results_dir(source_root)
            if selected is None:
                raise SystemExit(f"No {source.upper()} results found under: {source_root}")

        if source in {"kuzu", "neo4j"}:
            analysis_dir = selected / "analysis"
            if analysis_dir.is_dir():
                selected = analysis_dir
            elif (selected / "raw").is_dir() and not any(selected.glob("*.csv")):
                raise SystemExit(
                    f"No analysis CSVs found under {selected}. "
                    f"Run scripts/kuzu_neo4j_report.py on '{selected / 'raw'}' first."
                )
        return str(selected.resolve())

    return str(Path(raw_value).resolve())


def build_combined_dir_name(input_paths, active_dbs):
    parts = []
    for db in active_dbs:
        db_path = input_paths.get(db)
        if not db_path:
            continue
        parts.append(db)
        parts.append(infer_source_tag(db_path))
    return "_".join(parts) if parts else "combined"


def main():
    global TIMEOUT_RUNTIME_MS
    parser = argparse.ArgumentParser(
        description=(
            "Create per-query speedup comparison PDFs from AGE/Kuzu/Neo4j result CSVs. "
            "Speedups below 1 are shown below 1 on a log scale."
        ),
        epilog=(
            "Examples:\n"
            "  python scripts/plot_speedups.py --age path/to/age_runtimes.csv --kuzu path/to/kuzu.csv --neo4j path/to/neo4j.csv\n"
            "  python scripts/plot_speedups.py --kuzu results/kuzu/paper_results/analysis --neo4j results/neo4j/paper_results/analysis\n"
            "  python scripts/plot_speedups.py --neo4j results/neo4j/latest --legend-placement outside --legend-align right\n"
            "  python scripts/plot_speedups.py --age results/age.csv --crop-query-max 1:300 --crop-query-max 2:120\n\n"
            "Behavior:\n"
            "  - One PDF is generated per query found in the provided input CSV(s): speedup_<query>.pdf.\n"
            "  - At least one of --age, --kuzu, or --neo4j must be provided.\n"
            "  - If --out-dir is omitted, output is written to <repo>/results/combined/<db_tag_pairs>."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--age",
        required=False,
        help="Path to AGE runtimes CSV (file, directory, or glob pattern), or shorthand: paper/latest.",
    )
    parser.add_argument(
        "--kuzu",
        required=False,
        help="Path to Kuzu CSV (file, directory, or glob pattern), or shorthand: paper/latest.",
    )
    parser.add_argument(
        "--neo4j",
        required=False,
        help="Path to Neo4j CSV (file, directory, or glob pattern), or shorthand: paper/latest.",
    )
    parser.add_argument(
        "--out-dir",
        default=None,
        help="Directory for PDF output (default: <repo>/results/combined/<db_tag_pairs>)",
    )
    parser.add_argument(
        "--timeout-ms",
        type=float,
        default=TIMEOUT_RUNTIME_MS,
        help="Timeout threshold in milliseconds used for timeout-derived speedup lower bounds (default: 300000).",
    )
    parser.add_argument(
        "--crop-query-max",
        action="append",
        default=[],
        help="Optional repeated mapping NUM:MAX_Y (example: --crop-query-max 01:300)",
    )
    parser.add_argument(
        "--legend-query-numbers",
        default="all",
        help="all (default), none, or comma-separated query numbers (example: 01,2,10)",
    )
    parser.add_argument(
        "--legend-placement",
        choices=["inside", "outside"],
        default="inside",
        help="Legend placement relative to axes area.",
    )
    parser.add_argument(
        "--legend-align",
        default="center",
        help="Legend horizontal alignment: left|center|right or numeric (0..1, or 0..100 as percent).",
    )
    parser.add_argument(
        "--legend-columns",
        type=int,
        default=2,
        help="Maximum number of legend columns (default: 2).",
    )
    parser.add_argument(
        "--legend-order",
        choices=["db-suffix", "suffix-db", "reverse"],
        default="db-suffix",
        help="Legend item order: db-suffix (default), suffix-db, or reverse.",
    )
    parser.add_argument(
        "--baseline-legend-position",
        type=int,
        default=None,
        help=(
            "Insertion index for Baseline in legend when shown (0-based). "
            "Negative values count from end (-1 = last). Default: last."
        ),
    )
    parser.add_argument(
        "--baseline-label-placement",
        choices=["none", "line", "legend"],
        default="none",
        help="Baseline label placement: none (default), line, or legend.",
    )
    parser.add_argument(
        "--label-shift",
        action="append",
        default=[],
        help="Repeatable: QUERY:BAR:DX:DY[:CURV] (BAR may be grouped like 0/1; example: 01:0/1:0:8 or 01:0/1:0:8:0).",
    )
    args = parser.parse_args()

    if args.timeout_ms <= 0:
        parser.error("--timeout-ms must be > 0")

    TIMEOUT_RUNTIME_MS = float(args.timeout_ms)

    baseline_label_placement = args.baseline_label_placement

    if args.legend_columns < 1:
        parser.error("--legend-columns must be >= 1")

    try:
        label_shifts_by_query = parse_label_shifts(args.label_shift)
    except ValueError as exc:
        parser.error(str(exc))

    active_dbs = get_active_dbs(args)
    if not active_dbs:
        parser.error("At least one of --age, --kuzu, or --neo4j must be provided.")

    crop_query_max = {}
    for raw in args.crop_query_max:
        if ":" not in raw:
            parser.error(f"Invalid --crop-query-max value '{raw}'. Expected NUM:MAX_Y")
        query_no_raw, max_y_raw = raw.split(":", 1)
        query_no_raw = query_no_raw.strip()
        max_y_raw = max_y_raw.strip()
        if not query_no_raw.isdigit():
            parser.error(f"Invalid query number in --crop-query-max: '{query_no_raw}'")
        try:
            max_y = float(max_y_raw)
        except ValueError:
            parser.error(f"Invalid max y-value in --crop-query-max: '{max_y_raw}'")
        crop_query_max[str(int(query_no_raw))] = max_y

    legend_arg = args.legend_query_numbers.strip().lower()
    if legend_arg == "all":
        legend_numbers = None
    elif legend_arg == "none":
        legend_numbers = set()
    else:
        legend_numbers = set()
        for token in legend_arg.split(","):
            token = token.strip()
            if not token:
                continue
            if not token.isdigit():
                parser.error(f"Invalid query number in --legend-query-numbers: '{token}'")
            legend_numbers.add(str(int(token)))

    legend_align_raw = str(args.legend_align).strip().lower()
    if legend_align_raw in {"left", "center", "right"}:
        legend_align = legend_align_raw
    else:
        try:
            legend_align_numeric = float(legend_align_raw)
        except ValueError:
            parser.error(
                "Invalid --legend-align value. Use left|center|right or a number (0..1, 0..100%)."
            )
        if 0.0 <= legend_align_numeric <= 1.0:
            legend_align = legend_align_numeric
        elif 1.0 < legend_align_numeric <= 100.0:
            legend_align = legend_align_numeric / 100.0
        else:
            parser.error("Numeric --legend-align must be in [0,1] or [0,100].")

    repo_root = Path(__file__).resolve().parents[1]
    resolved_input_paths = {
        db: resolve_input_path(getattr(args, db), db, repo_root)
        for db in active_dbs
    }
    combined_dir_name = build_combined_dir_name(resolved_input_paths, active_dbs)
    default_out_dir = os.path.join(str(repo_root), "results", "combined", combined_dir_name)
    out_dir = os.path.abspath(args.out_dir or default_out_dir)
    os.makedirs(out_dir, exist_ok=True)

    data_by_db = {}
    if args.age:
        data_by_db["age"] = load_age(resolved_input_paths["age"])
    if args.kuzu:
        data_by_db["kuzu"] = load_other(resolved_input_paths["kuzu"], "kuzu")
    if args.neo4j:
        data_by_db["neo4j"] = load_other(resolved_input_paths["neo4j"], "neo4j")

    suffixes_by_db = {
        db: (AGE_SUFFIXES_PLOT if db == "age" else OTHER_SUFFIXES_PLOT)
        for db in active_dbs
    }
    db_hatches = compute_db_hatches(active_dbs)
    output_db_suffix = build_output_db_suffix(active_dbs)

    y_limits = compute_global_y_limits(data_by_db, active_dbs, suffixes_by_db)

    query_keys = set()
    for db in active_dbs:
        query_keys.update(data_by_db.get(db, {}).keys())
    queries = sorted(query_keys, key=query_sort_key)
    if not queries:
        parser.error("No queries found in the provided input CSV(s).")
    if MPL_IMPORT_ERROR is not None:
        parser.error("Missing dependency 'matplotlib'. Install it with: pip install matplotlib")

    all_shift_debug_entries = []
    for query in queries:
        per_db = {db: data_by_db.get(db, {}).get(query, {}) for db in active_dbs}
        _, _, shift_debug_entries = plot_query(
            query,
            per_db,
            out_dir,
            y_limits,
            active_dbs,
            suffixes_by_db,
            db_hatches,
            output_db_suffix,
            crop_query_max,
            legend_numbers,
            args.legend_placement,
            legend_align,
            args.legend_columns,
            args.legend_order,
            args.baseline_legend_position,
            label_shifts_by_query,
            baseline_label_placement,
        )
        all_shift_debug_entries.extend(shift_debug_entries)

    if args.label_shift:
        emitted_keys = {
            (str(entry.get("query_number")), str(entry.get("requested_selector")))
            for entry in all_shift_debug_entries
        }

        for query_number, shift_entries in label_shifts_by_query.items():
            for shift_entry in shift_entries:
                key = (str(query_number), str(shift_entry.get("selector")))
                if key in emitted_keys:
                    continue
                all_shift_debug_entries.append(
                    {
                        "query": f"query#{query_number}",
                        "query_number": query_number,
                        "requested_selector": shift_entry.get("selector"),
                        "dx": shift_entry.get("dx"),
                        "dy": shift_entry.get("dy"),
                        "curvature": shift_entry.get("curvature"),
                        "arrow_enabled": bool(shift_entry.get("arrow_enabled", False)),
                        "status": "not-applied",
                        "reason": "query number not found among rendered queries",
                    }
                )

        print("\n=== label-shift resolution ===")
        if not all_shift_debug_entries:
            print("No shift targets were resolved.")
        for entry in all_shift_debug_entries:
            query_text = entry.get("query", "")
            requested = entry.get("requested_selector")
            dx = entry.get("dx")
            dy = entry.get("dy")
            curvature = entry.get("curvature")
            arrow_enabled = bool(entry.get("arrow_enabled", False))
            status = entry.get("status")

            if status not in {"ok", "ok-grouped"}:
                reason = entry.get("reason", "unknown")
                print(
                    f"query={query_text} bar={requested} shift=({dx},{dy},{curvature}) arrow={arrow_enabled} -> NOT APPLIED ({reason})"
                )
                continue

            resolved = entry.get("resolved_selector")
            db = entry.get("db")
            suffix = entry.get("suffix")
            label = entry.get("label")
            mode = "grouped" if status == "ok-grouped" else "single"
            print(
                f"query={query_text} bar={requested}->{resolved} mode={mode} target={db}/{suffix} label={label} shift=({dx},{dy},{curvature}) arrow={arrow_enabled}"
            )


if __name__ == "__main__":
    main()
