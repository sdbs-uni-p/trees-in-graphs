# SPDX-License-Identifier: GPL-3.0-only
import os
from datetime import datetime
from pathlib import Path

from experiments.experiement_infrastructure import Neo4jExecutor, run_ldbc


def run_experiment():
    project = Path(os.getenv("PROJECT_PATH", Path(__file__).resolve().parents[2]))
    output = project / os.getenv("RESULTS_SUBPATH", f"results/neo4j_ldbc/{datetime.now():%Y%m%d_%H%M%S}")
    uri = os.getenv("NEO4J_URI", "bolt://neo4j_ldbc_treebench_db:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "treebenchPW")
    query_path = project / os.getenv("QUERIES_SUBPATH", "queries/neo4j_ldbc")
    print("Configuration:", flush=True)
    print(f"  Neo4j URI: {uri}", flush=True)
    print(f"  Project Path: {project}", flush=True)
    print(f"  Query Path: {query_path}", flush=True)
    print(f"  Results Path: {output}", flush=True)
    executors = {method: Neo4jExecutor(
        uri, user, password)
        for method in ("baseline", "dewey", "prepost")}
    try:
        return run_ldbc(
            executors, query_path, output,
            runs=int(os.getenv("EXPERIMENT_N", "5")), heat=int(os.getenv("EXPERIMENT_HEAT", "0")),
            query_filter=os.getenv("QUERY_FILTER", ""),
            save_plans=os.getenv("SAVE_PLANS", "1") == "1",
            save_results=os.getenv("SAVE_RESULTS", "1") == "1",
            save_queries=os.getenv("SAVE_QUERIES", "1") == "1",
            report_script=project / "scripts/create_runtime_tables.py")
    finally:
        for executor in executors.values():
            executor.driver.close()


if __name__ == "__main__":
    run_experiment()
