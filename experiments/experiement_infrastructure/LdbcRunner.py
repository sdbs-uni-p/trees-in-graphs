# SPDX-License-Identifier: GPL-3.0-only
"""Shared artifact and measurement loop for native Kuzu/Neo4j LDBC runs."""

import csv
import fnmatch
import json
import subprocess
from pathlib import Path

from .ResultMetadata import write_run_metadata

METHODS = ("baseline", "dewey", "prepost")
STRUCTURAL_PARAMETERS = {
    "interactive-complex-12": ("TagClass", "name", "Monarch", "base"),
    "interactive-short-6": ("Comment", "id", 1236950581249, "message"),
}


def _matches(name, expression):
    if not expression:
        return True
    return any(
        fnmatch.fnmatch(name, item.strip())
        or fnmatch.fnmatch(Path(name).stem, item.strip())
        for item in expression.split(",") if item.strip()
    )


def _write(path, value):
    text = value if isinstance(value, str) else json.dumps(
        value, indent=2, ensure_ascii=False, default=str
    )
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def _render_structural_parameters(executor, query_id, method, query):
    """Resolve fixed logical parameters before the measured execution.

    This mirrors FixedScenarioRunner: annotated databases are queried once by
    the logical parameter and the timed query receives the indexed structural
    key directly.
    """
    if method == "baseline" or query_id not in STRUCTURAL_PARAMETERS:
        return query
    label, property_name, logical_value, prefix = STRUCTURAL_PARAMETERS[query_id]
    literal = f"'{logical_value}'" if isinstance(logical_value, str) else str(logical_value)
    annotation_properties = ("dewey",) if method == "dewey" else ("pre", "post")
    returns = ", ".join(f"n.{name}" for name in annotation_properties)
    _, rows = executor.execute_query(
        f"MATCH (n:{label}) WHERE n.{property_name} = {literal} RETURN {returns}"
    )
    if len(rows) != 1:
        raise ValueError(
            f"Expected one {label} with {property_name}={logical_value!r}, found {len(rows)}"
        )
    replacements = {}
    for name, value in zip(annotation_properties, rows[0]):
        token = f"${prefix}{name.title()}"
        replacements[token] = f"'{value}'" if isinstance(value, str) else str(value)
    if query_id == "interactive-short-6":
        if method == "dewey":
            root_value = str(rows[0][0]).split(".", 1)[0]
            replacements["$rootDewey"] = f"'{root_value}'"
        else:
            message_pre, message_post = rows[0]
            _, root_rows = executor.execute_query(
                "MATCH (root:Comment) "
                f"WHERE root.depth = 0 AND root.pre <= {message_pre} "
                f"AND root.post >= {message_post} RETURN root.pre"
            )
            if len(root_rows) != 1:
                raise ValueError(
                    f"Expected one Comment root for interval ({message_pre}, {message_post}), "
                    f"found {len(root_rows)}"
                )
            replacements["$rootPre"] = str(root_rows[0][0])
    for token, value in replacements.items():
        query = query.replace(token, value)
    return query


def run_ldbc(executors, query_path, output_dir, *, runs=5, heat=0,
             query_filter="", save_plans=True, save_results=True,
             save_queries=True, report_script=None):
    """Run each native query against its matching graph representation."""
    if runs < 1 or heat < 0:
        raise ValueError("runs must be positive and heat must not be negative")
    query_path, output_dir = Path(query_path), Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    write_run_metadata(
        output_dir, workload="ldbc", executors=executors, runs=runs, warmup=heat,
        query_path=query_path, query_filter=query_filter, save_plans=save_plans,
        save_results=save_results, save_queries=save_queries,
    )
    dirs = {name: output_dir / name for name in ("queries", "plans", "results", "errors")}
    enabled = {"queries": save_queries, "plans": save_plans,
               "results": save_results, "errors": True}
    for name, path in dirs.items():
        if enabled[name]:
            path.mkdir(parents=True, exist_ok=True)

    query_names = sorted(path.name for path in (query_path / "baseline").glob("*.sql"))
    query_names = [name for name in query_names if _matches(name, query_filter)]
    if not query_names:
        raise ValueError(f"No LDBC queries matched {query_filter!r}")

    total_jobs = len(query_names) * len(METHODS)
    print("Starting LDBC experiments", flush=True)
    print(f"  Output directory: {output_dir}", flush=True)
    print(f"  Queries selected: {len(query_names)}", flush=True)
    print(f"  Representations: {', '.join(METHODS)}", flush=True)
    print(f"  Warmup runs per job: {heat}", flush=True)
    print(f"  Measurement runs per job: {runs}", flush=True)
    print(f"  Total graph-query jobs: {total_jobs}", flush=True)

    csv_path = output_dir / "runtimes.csv"
    current_job = 0
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["graph", "query", "run", "runtime_ms"])
        for query_name in query_names:
            query_id = Path(query_name).stem
            for method in METHODS:
                current_job += 1
                graph = f"snb_sf1_{method}"
                executor = executors[method]
                executor.set_graph(graph)
                query = (query_path / method / query_name).read_text(encoding="utf-8")
                query = _render_structural_parameters(
                    executor, query_id, method, query
                )
                stem = f"{graph}_{method}_{query_id}"
                print(
                    f"[{current_job}/{total_jobs}] graph={graph} query={query_id}",
                    flush=True,
                )
                if save_queries:
                    _write(dirs["queries"] / f"{stem}.sql", query)
                try:
                    for warmup in range(1, heat + 1):
                        executor.execute_query(query)
                        print(f"  warmup {warmup}/{heat}: ok", flush=True)
                    for run in range(1, runs + 1):
                        runtime, _ = executor.execute_query(query)
                        writer.writerow([graph, query_id, run, f"{runtime:.3f}"])
                        handle.flush()
                        print(f"  run {run}/{runs}: {runtime:.3f} ms", flush=True)
                    if save_plans:
                        _, plan, _, _ = executor.collect_query_plan(query)
                        _write(dirs["plans"] / f"{stem}.plan.txt", plan)
                        print("  plan: saved", flush=True)
                    if save_results:
                        _, rows = executor.execute_query(query)
                        _write(dirs["results"] / f"{stem}.results.json", rows)
                        print(f"  results: saved ({len(rows)} rows)", flush=True)
                except Exception as exc:
                    _write(dirs["errors"] / f"{stem}.log", f"{type(exc).__name__}: {exc}")
                    print(f"  ERROR: {type(exc).__name__}: {exc}", flush=True)
                    raise

    if report_script:
        print("Creating runtime_tables.pdf...", flush=True)
        subprocess.run(["python", str(report_script), str(csv_path),
                        str(output_dir / "runtime_tables.pdf")], check=True)
    print(f"LDBC experiments completed: {output_dir}", flush=True)
    return csv_path
