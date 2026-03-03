#!/usr/bin/env python3
import argparse
import csv
import math
import re
import statistics
from collections import defaultdict
from pathlib import Path

TIMESTAMP_PATTERN = re.compile(r"^\d{8}_\d{6}$")


def infer_source_tag(path_value: str) -> str:
    path_obj = Path(path_value).resolve()
    parts = [part for part in path_obj.parts if part]
    parts_lower = [part.lower() for part in parts]

    for part in reversed(parts):
        if TIMESTAMP_PATTERN.match(part):
            return part

    if "paper" in parts_lower:
        return "paper"

    if path_obj.suffix:
        return path_obj.parent.name or path_obj.stem or "input"
    return path_obj.name or "input"


def build_combined_dir_name(age_path: str, kuzu_path: str) -> str:
    return f"age_{infer_source_tag(age_path)}_kuzu_{infer_source_tag(kuzu_path)}"


def select_latest_results_dir(base_dir: Path):
    if not base_dir.exists():
        return None

    timestamp_dirs = [
        path for path in base_dir.iterdir()
        if path.is_dir() and TIMESTAMP_PATTERN.match(path.name)
    ]
    if timestamp_dirs:
        return sorted(timestamp_dirs, key=lambda path: path.name, reverse=True)[0]

    paper_dir = base_dir / "paper"
    if paper_dir.is_dir():
        return paper_dir

    any_dirs = [path for path in base_dir.iterdir() if path.is_dir()]
    if any_dirs:
        return sorted(any_dirs, key=lambda path: path.stat().st_mtime, reverse=True)[0]

    return None


def select_csv_from_directory(directory: Path):
    preferred_names = ["runtimes.csv", "results.csv", "result.csv"]
    for file_name in preferred_names:
        candidate = directory / file_name
        if candidate.is_file():
            return candidate

    csv_files = [path for path in directory.glob("*.csv") if path.is_file()]
    if not csv_files:
        return None
    if len(csv_files) == 1:
        return csv_files[0]
    return sorted(csv_files, key=lambda path: path.stat().st_mtime, reverse=True)[0]


def resolve_csv_input(raw_value: str, source: str, repo_root: Path) -> Path:
    token = (raw_value or "").strip().lower()
    if token in {"paper", "latest"}:
        source_root = repo_root / "results" / source
        if token == "paper":
            selected = source_root / "paper"
            if not selected.is_dir():
                raise SystemExit(f"{source.upper()} paper results directory not found: {selected}")
        else:
            selected = select_latest_results_dir(source_root)
            if selected is None:
                raise SystemExit(f"No {source.upper()} results found under: {source_root}")
        selected_csv = select_csv_from_directory(selected)
        if selected_csv is None:
            raise SystemExit(f"No CSV file found in {source.upper()} results directory: {selected}")
        return selected_csv.resolve()

    resolved_path = Path(raw_value).resolve()
    if resolved_path.is_dir():
        selected_csv = select_csv_from_directory(resolved_path)
        if selected_csv is None:
            raise SystemExit(f"No CSV file found in directory: {resolved_path}")
        return selected_csv.resolve()
    return resolved_path


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


def normalize_query_name(query_name: str) -> str:
    query_name = query_name.strip().lower()
    parts = query_name.split("_", 1)
    if len(parts) == 2 and parts[0].isdigit():
        query_name = parts[1]

    if query_name == "check_if_ancestor_true":
        return "check_if_ancestor"

    if query_name.endswith("_true"):
        prefix = query_name[: -len("_true")]
        if prefix == "check_if_ancestor":
            return "check_if_ancestor"

    return query_name


def parse_age_graph_variant(graph_name: str):
    graph_name = graph_name.strip().lower()
    for suffix in ("baseline", "dewey", "prepost"):
        token = f"_{suffix}"
        if graph_name.endswith(token):
            return graph_name[: -len(token)], suffix
    return graph_name, "baseline"


def safe_float(value: str):
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(parsed) or math.isinf(parsed):
        return None
    return parsed


def load_kuzu_baseline_medians(csv_path: Path):
    result = {}
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"Graph Name", "Query Name", "Annotation Method", "Median Client-Side Runtime"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Kuzu CSV missing columns: {', '.join(sorted(missing))}")

        for row in reader:
            annotation = (row.get("Annotation Method") or "").strip().lower()
            if annotation != "baseline":
                continue

            graph = normalize_graph_name(row.get("Graph Name") or "")
            query = normalize_query_name(row.get("Query Name") or "")
            runtime_ms = safe_float(row.get("Median Client-Side Runtime") or "")
            if not graph or not query or runtime_ms is None:
                continue

            result[(graph, query)] = runtime_ms

    return result


def load_age_baseline_medians(csv_path: Path):
    runtime_by_key = defaultdict(list)
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"graph", "query", "run", "runtime_ms"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"AGE CSV missing columns: {', '.join(sorted(missing))}")

        for row in reader:
            raw_graph = (row.get("graph") or "").strip()
            base_graph, variant = parse_age_graph_variant(raw_graph)
            if variant != "baseline":
                continue

            graph = normalize_graph_name(base_graph)
            query = normalize_query_name(row.get("query") or "")
            runtime_ms = safe_float(row.get("runtime_ms") or "")
            if not graph or not query or runtime_ms is None:
                continue

            runtime_by_key[(graph, query)].append(runtime_ms)

    medians = {}
    for key, values in runtime_by_key.items():
        if not values:
            continue
        medians[key] = statistics.median(values)
    return medians


def faster_info(kuzu_ms: float, age_ms: float):
    if kuzu_ms < age_ms:
        faster = "Kuzu"
        speedup = age_ms / kuzu_ms if kuzu_ms > 0 else 0.0
    elif age_ms < kuzu_ms:
        faster = "AGE"
        speedup = kuzu_ms / age_ms if age_ms > 0 else 0.0
    else:
        faster = "Tie"
        speedup = 1.0

    return faster, speedup


def build_rows(kuzu_by_key, age_by_key):
    rows = []
    common_keys = sorted(set(age_by_key) & set(kuzu_by_key))
    for graph, query in common_keys:
        kuzu_value = kuzu_by_key[(graph, query)]
        age_value = age_by_key[(graph, query)]
        faster, speedup = faster_info(kuzu_value, age_value)
        rows.append(
            {
                "Graph": graph,
                "Query": query,
                "Kuzu": f"{kuzu_value:.3f}",
                "AGE": f"{age_value:.3f}",
                "Faster": faster,
                "Speedup": f"{speedup:.3f}",
            }
        )

    return rows


def write_output(rows, output_path: Path):
    fieldnames = ["Graph", "Query", "Kuzu", "AGE", "Faster", "Speedup"]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main():
    parser = argparse.ArgumentParser(
        description="Compares baseline median runtimes of Kuzu and AGE per graph."
    )
    parser.add_argument(
        "--kuzu",
        required=True,
        help="Path to Kuzu CSV, a Kuzu results directory, or shorthand: paper/latest",
    )
    parser.add_argument(
        "--age",
        required=True,
        help="Path to AGE runtimes CSV, an AGE results directory, or shorthand: paper/latest",
    )
    parser.add_argument(
        "--out",
        default=None,
        help=(
            "Output CSV path (default: "
            "<repo>/results/combined/age_<tag>_kuzu_<tag>/baseline_kuzu_age_compare.csv)"
        ),
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    kuzu_path = resolve_csv_input(args.kuzu, "kuzu", repo_root)
    age_path = resolve_csv_input(args.age, "age", repo_root)

    if not kuzu_path.exists():
        raise SystemExit(f"Kuzu CSV not found: {kuzu_path}")
    if not age_path.exists():
        raise SystemExit(f"AGE CSV not found: {age_path}")

    if args.out:
        output_path = Path(args.out).resolve()
    else:
        combined_dir_name = build_combined_dir_name(str(age_path), str(kuzu_path))
        output_path = repo_root / "results" / "combined" / combined_dir_name / "baseline_kuzu_age_compare.csv"

    kuzu_by_key = load_kuzu_baseline_medians(kuzu_path)
    age_by_key = load_age_baseline_medians(age_path)
    rows = build_rows(kuzu_by_key, age_by_key)
    write_output(rows, output_path)

    print(f"Kuzu baseline entries: {len(kuzu_by_key)}")
    print(f"AGE baseline entries (median from runs): {len(age_by_key)}")
    print(f"Output rows (common baseline graph/query pairs): {len(rows)}")
    print(f"Wrote: {output_path}")


if __name__ == "__main__":
    main()
