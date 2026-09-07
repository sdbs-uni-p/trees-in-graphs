# SPDX-License-Identifier: GPL-3.0-only

"""Run Kuzu/Neo4j with the fixed parameter scenarios shared with AGE."""

import csv
import fnmatch
import json
import re
from collections import defaultdict
from pathlib import Path

from .ResultMetadata import write_run_metadata

from tqdm import tqdm


QUERY_FILES = {
    "01_all_descendants": "01_all_descendants.sql",
    "02_all_children": "02_all_children.sql",
    "05_all_leaves": "05_all_leaves.sql",
    "11_check_if_ancestor": "11_check_if_ancestor_true.sql",
}
METHODS = ("baseline", "dewey", "prepost")


def _matches_filter(value, pattern_list):
    if not pattern_list:
        return True
    return any(
        fnmatch.fnmatchcase(value, pattern.strip())
        for pattern in pattern_list.split(",")
        if pattern.strip()
    )


def _graph_labels(graph):
    if graph == "snb_sf1_comment":
        return "Comment", "REPLY_OF"
    if graph == "snb_sf1_place":
        return "Place", "IS_PART_OF"
    if graph == "snb_sf1_tagclass":
        return "TagClass", "IS_SUBCLASS_OF"
    return "TreeNode", "HAS_CHILD"


def load_parameter_scenarios(path):
    grouped = defaultdict(dict)
    with Path(path).open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        expected = ["graph", "query", "scenario", "parameter", "value"]
        if reader.fieldnames != expected:
            raise ValueError(f"Unexpected parameter columns: {reader.fieldnames}")
        for row in reader:
            key = (row["graph"], row["query"], row["scenario"])
            parameter = row["parameter"]
            if parameter in grouped[key]:
                raise ValueError(f"Duplicate parameter {parameter} for {key}")
            grouped[key][parameter] = int(row["value"])
    return grouped


def _read_queries(query_path):
    query_path = Path(query_path)
    queries = {}
    for method in METHODS:
        for query, filename in QUERY_FILES.items():
            queries[(method, query)] = (query_path / method / filename).read_text(
                encoding="utf-8"
            )
    return queries


def _replace_parameters(query, values, node_label, relation_label):
    replacements = {
        "$NODE_TYPE": node_label,
        "$REL_TYPE": relation_label,
        "$rootID": values.get("rootid", ""),
        "$id1": values.get("id1", ""),
        "$id2": values.get("id2", ""),
        "$id1_t": values.get("id1", ""),
        "$id1_f": values.get("id1", ""),
        "$id2_t": values.get("id2", ""),
        "$id2_f": values.get("id2", ""),
        "$deweyRoot": values.get("dewey_root", ""),
        "$prepostRoot": values.get("prepost_root", ""),
    }
    for suffix, source in (("1", "id1"), ("2", "id2")):
        replacements[f"$deweyId{suffix}_t"] = values.get(f"dewey_id{suffix}", "")
        replacements[f"$deweyId{suffix}_f"] = values.get(f"dewey_id{suffix}", "")
        replacements[f"$prepostId{suffix}_t"] = values.get(f"prepost_id{suffix}", "")
        replacements[f"$prepostId{suffix}_f"] = values.get(f"prepost_id{suffix}", "")
    for token in sorted(replacements, key=len, reverse=True):
        query = query.replace(token, str(replacements[token]))
    unresolved = sorted(set(re.findall(r"\$[A-Za-z][A-Za-z0-9_]*", query)))
    if unresolved:
        raise ValueError(f"Unresolved query parameters: {unresolved}")
    return query


def _lookup_annotation(executor, node_label, original_id, property_name):
    if original_id is None:
        return None
    _, rows = executor.execute_query(
        f"MATCH (n:{node_label}) WHERE n.id = {original_id} RETURN n.{property_name}"
    )
    if len(rows) != 1:
        raise ValueError(
            f"Expected one {node_label} with id={original_id}, found {len(rows)}"
        )
    return rows[0][0]


def _artifact_stem(graph, method, query, scenario):
    """Return an AGE-compatible, filesystem-safe artifact basename."""
    return f"{graph}_{method}_{method}_{query}_{scenario}"


def _write_text(path, value):
    Path(path).write_text(str(value).rstrip() + "\n", encoding="utf-8")


def _write_json(path, value):
    Path(path).write_text(
        json.dumps(value, indent=2, ensure_ascii=False, default=str) + "\n",
        encoding="utf-8",
    )


def run_fixed_scenarios(
    executors,
    query_path,
    parameters_path,
    output_path,
    *,
    heat=0,
    runs=5,
    structural_primary_keys=False,
    save_plans=True,
    save_results=True,
    save_queries=True,
    scenario_filter="",
):
    """Execute exactly the graph/query/scenario combinations from the shared CSV."""
    scenarios = load_parameter_scenarios(parameters_path)
    queries = _read_queries(query_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    write_run_metadata(
        output_path.parent, workload="tree", executors=executors, runs=runs, warmup=heat,
        query_path=query_path, parameters_path=parameters_path, scenario_filter=scenario_filter,
        structural_primary_keys=structural_primary_keys, save_plans=save_plans,
        save_results=save_results, save_queries=save_queries,
    )
    artifact_root = output_path.parent
    artifact_dirs = {
        "plans": artifact_root / "plans",
        "results": artifact_root / "results",
        "queries": artifact_root / "queries",
        "errors": artifact_root / "errors",
    }
    enabled_artifacts = {
        "plans": save_plans,
        "results": save_results,
        "queries": save_queries,
        "errors": True,
    }
    for name, path in artifact_dirs.items():
        if enabled_artifacts[name]:
            path.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["graph", "query", "scenario", "run", "runtime_ms"])

        selected_scenarios = [
            (key, values)
            for key, values in scenarios.items()
            if key[1] in QUERY_FILES and _matches_filter(key[2], scenario_filter)
        ]
        graph_names = list(dict.fromkeys(key[0] for key, _ in selected_scenarios))
        progress = tqdm(
            total=len(graph_names),
            desc="Processing string graphs",
        )
        previous_graph = None
        previous_query = None
        for (graph, query_name, scenario), plain_values in selected_scenarios:
            if graph != previous_graph:
                if previous_graph is not None:
                    progress.update()
                print(f"Processing {graph}_dewey")
                previous_graph = graph
                previous_query = None
            if query_name != previous_query:
                description = Path(QUERY_FILES[query_name]).stem[3:]
                print(f"Processing {description}")
                previous_query = query_name
            node_label, relation_label = _graph_labels(graph)
            for method in METHODS:
                executor = executors[method]
                executor.set_graph(f"{graph}_{method}")
                values = dict(plain_values)

                # Kuzu structural databases use the annotation as primary key.
                if structural_primary_keys and method != "baseline":
                    property_name = "dewey" if method == "dewey" else "pre"
                    prefix = "dewey" if method == "dewey" else "prepost"
                    values[f"{prefix}_root"] = _lookup_annotation(
                        executor, node_label, values.get("rootid"), property_name
                    )
                    values[f"{prefix}_id1"] = _lookup_annotation(
                        executor, node_label, values.get("id1"), property_name
                    )
                    values[f"{prefix}_id2"] = _lookup_annotation(
                        executor, node_label, values.get("id2"), property_name
                    )

                rendered = _replace_parameters(
                    queries[(method, query_name)], values, node_label, relation_label
                )
                stem = _artifact_stem(graph, method, query_name, scenario)
                if save_queries:
                    _write_text(artifact_dirs["queries"] / f"{stem}.sql", rendered)
                for _ in range(heat):
                    try:
                        executor.execute_query(rendered)
                    except Exception as exc:
                        _write_text(
                            artifact_dirs["errors"] / f"{stem}_warmup.log",
                            f"{type(exc).__name__}: {exc}",
                        )
                        break
                last_results = None
                for run in range(1, runs + 1):
                    try:
                        runtime_ms, last_results = executor.execute_query(rendered)
                        runtime = f"{runtime_ms:.3f}"
                    except Exception as exc:
                        runtime = ""
                        _write_text(
                            artifact_dirs["errors"] / f"{stem}_run{run}.log",
                            f"{type(exc).__name__}: {exc}",
                        )
                    writer.writerow([f"{graph}_{method}", query_name, scenario, run, runtime])

                if save_results and last_results is not None:
                    _write_json(artifact_dirs["results"] / f"{stem}.json", last_results)
                if save_plans:
                    try:
                        _, plan, _, _ = executor.collect_query_plan(rendered)
                        _write_text(artifact_dirs["plans"] / f"{stem}.plan.txt", plan)
                    except Exception as exc:
                        _write_text(
                            artifact_dirs["errors"] / f"{stem}_plan.log",
                            f"{type(exc).__name__}: {exc}",
                        )

        if previous_graph is not None:
            progress.update()
        progress.close()

    return output_path
