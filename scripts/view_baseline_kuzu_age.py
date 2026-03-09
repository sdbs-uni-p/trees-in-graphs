#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-only

import argparse
import csv
from pathlib import Path

GRAPH_WHITELIST = {
    "artificial_forests_40",
    "artificial_trees_ultrawide_100",
    "artificial_trees_ultrawide_1000",
    "artificial_trees_ultrawide_10000",
    "artificial_trees_ultratall_10000",
}

QUERY_ALIASES = {
    "01": "all_descendants",
    "1": "all_descendants",
    "02": "all_children",
    "2": "all_children",
    "05": "all_leaves",
    "5": "all_leaves",
    "11": "check_if_ancestor",
}


def normalize_query_token(token: str) -> str:
    token = token.strip().lower()
    if not token:
        return ""
    if token in QUERY_ALIASES:
        return QUERY_ALIASES[token]
    return token


def parse_query_filters(raw_values):
    filters = set()
    for raw in raw_values:
        for token in raw.split(","):
            normalized = normalize_query_token(token)
            if normalized:
                filters.add(normalized)
    return filters


def graph_is_selected(graph: str) -> bool:
    graph = graph.strip().lower()
    if graph in GRAPH_WHITELIST:
        return True
    if graph.startswith("snb"):
        return True
    return False


def read_rows(csv_path: Path):
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"Graph", "Query", "Kuzu", "AGE", "Faster", "Speedup"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"CSV missing columns: {', '.join(sorted(missing))}")
        return list(reader)


def filter_rows(rows, query_filters):
    selected = []
    for row in rows:
        graph = (row.get("Graph") or "").strip().lower()
        query = (row.get("Query") or "").strip().lower()
        if not graph_is_selected(graph):
            continue
        if query_filters is not None and query not in query_filters:
            continue
        selected.append(row)

    selected.sort(key=lambda r: ((r.get("Query") or "").lower(), (r.get("Graph") or "").lower()))
    return selected


def resolve_default_csv(repo_root: Path) -> Path:
    combined_root = repo_root / "results" / "combined"
    if combined_root.exists():
        candidates = sorted(
            combined_root.glob("**/baseline_kuzu_age_compare.csv"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        if candidates:
            return candidates[0]

    return repo_root / "results" / "baseline_kuzu_age_compare.csv"


def print_table(rows):
    if not rows:
        print("No matching rows found.")
        return

    headers = ["Graph", "Query", "Kuzu", "AGE", "Faster", "Speedup"]
    widths = {h: len(h) for h in headers}
    for row in rows:
        for header in headers:
            widths[header] = max(widths[header], len((row.get(header) or "").strip()))

    separator = "  "
    header_line = separator.join(f"{h:<{widths[h]}}" for h in headers)
    divider_line = separator.join("-" * widths[h] for h in headers)
    print(header_line)
    print(divider_line)

    for row in rows:
        print(
            separator.join(
                f"{(row.get(header) or '').strip():<{widths[header]}}"
                for header in headers
            )
        )


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Displays filtered views from baseline_kuzu_age_compare.csv in the console. "
            "Only considers artificial_forests_40, selected ultrawide/ultratall graphs, and snb*."
        )
    )
    parser.add_argument(
        "--csv",
        default=None,
        help=(
            "Path to the comparison CSV "
            "(default: latest <repo>/results/combined/**/baseline_kuzu_age_compare.csv)"
        ),
    )
    parser.add_argument(
        "--query",
        action="append",
        required=False,
        help=(
            "Query filter (repeatable or comma-separated). "
            "Accepts IDs 01,02,05,11 or names all_descendants, all_children, all_leaves, check_if_ancestor. "
            "If omitted, all queries are included."
        ),
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    csv_path = Path(args.csv).resolve() if args.csv else resolve_default_csv(repo_root)

    if not csv_path.exists():
        raise SystemExit(f"CSV not found: {csv_path}")

    query_filters = None
    if args.query:
        query_filters = parse_query_filters(args.query)
        if not query_filters:
            raise SystemExit("No valid query filter provided.")

    rows = read_rows(csv_path)
    filtered = filter_rows(rows, query_filters)

    print(f"CSV: {csv_path}")
    print(f"Query filter: {', '.join(sorted(query_filters)) if query_filters is not None else 'all'}")
    print(f"Rows: {len(filtered)}")
    print()
    print_table(filtered)


if __name__ == "__main__":
    main()
