# SPDX-License-Identifier: GPL-3.0-only
import os
from datetime import datetime
from pathlib import Path

from experiments.experiement_infrastructure import KuzuExecutor, run_ldbc


def run_experiment():
    project = Path(os.getenv("PROJECT_PATH", Path(__file__).resolve().parents[2]))
    output = project / os.getenv("RESULTS_SUBPATH", f"results/kuzu_ldbc/{datetime.now():%Y%m%d_%H%M%S}")
    db_path = os.getenv("KUZU_DB_PATH", "/kuzu_data")
    query_path = project / os.getenv("QUERIES_SUBPATH", "queries/kuzu_ldbc")
    print("Configuration:", flush=True)
    print(f"  Kuzu DB Path: {db_path}", flush=True)
    print(f"  Project Path: {project}", flush=True)
    print(f"  Query Path: {query_path}", flush=True)
    print(f"  Results Path: {output}", flush=True)
    executors = {method: KuzuExecutor(db_path)
                 for method in ("baseline", "dewey", "prepost")}
    return run_ldbc(
        executors, query_path, output,
        runs=int(os.getenv("EXPERIMENT_N", "5")), heat=int(os.getenv("EXPERIMENT_HEAT", "0")),
        query_filter=os.getenv("QUERY_FILTER", ""),
        save_plans=os.getenv("SAVE_PLANS", "1") == "1",
        save_results=os.getenv("SAVE_RESULTS", "1") == "1",
        save_queries=os.getenv("SAVE_QUERIES", "1") == "1",
        report_script=(project / "scripts/create_runtime_tables.py"
                       if os.getenv("SAVE_REPORT", "1") == "1" else None))


if __name__ == "__main__":
    run_experiment()
