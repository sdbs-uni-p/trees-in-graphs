import os
import sys
from pathlib import Path
from datetime import datetime
from experiments.experiement_infrastructure import assess_db, KuzuExecutor, KuzuParametrizer, ReducedKuzuParametrizer

# Add parent directory to path for imports when running as script
sys.path.insert(0, str(Path(__file__).parent.parent))


def get_config():
    """
    Get configuration from environment variables with sensible defaults.

    Environment variables:
        KUZU_DB_PATH: Base path containing Kuzu database directories (default: /kuzu_data)
        PROJECT_PATH: Base project path (default: current working directory's parent)
        QUERIES_SUBPATH: Path to query files relative to PROJECT_PATH
        RESULTS_SUBPATH: Path to results directory relative to PROJECT_PATH
        METADATA_SUBPATH: Path to graph metadata directory
        EXPERIMENT_HEAT: Number of warmup iterations (default: 5)
        EXPERIMENT_N: Number of timed iterations (default: 20)

    For Docker container execution (default):
        KUZU_DB_PATH=/kuzu_data
        PROJECT_PATH=/data/..  (the mounted project root)

    For local execution:
        KUZU_DB_PATH=<path_to_kuzu_databases>
        PROJECT_PATH=<project_root>
    """
    default_project_path = Path(__file__).parent.parent.parent

    return {
        "db": {
            "db_base_path": os.getenv("KUZU_DB_PATH", "/kuzu_data"),
        },
        "paths": {
            "project": Path(os.getenv("PROJECT_PATH", str(default_project_path))),
            "queries_subpath": os.getenv("QUERIES_SUBPATH", "queries/kuzu/cypher"),
            "results_subpath": os.getenv("RESULTS_SUBPATH", f"results/kuzu/results_raw_{datetime.now().strftime('%Y%m%d_%H%M%S')}"),
            "metadata_subpath": os.getenv("METADATA_SUBPATH", "data/graph_metadata"),
        },
        "experiment": {
            "heat": int(os.getenv("EXPERIMENT_HEAT", "0")),
            "n": int(os.getenv("EXPERIMENT_N", "5")),
        }
    }


def run_experiment(config=None):
    """Run the Kuzu experiment with the given configuration."""
    if config is None:
        config = get_config()

    db_config = config["db"]
    paths = config["paths"]
    exp_config = config["experiment"]

    project_path = paths["project"]

    # Build full paths
    query_path = project_path / paths["queries_subpath"]
    result_log_base = project_path / paths["results_subpath"]
    metadata_path = project_path / paths["metadata_subpath"]

    # Ensure results directory exists
    result_log_base.mkdir(parents=True, exist_ok=True)

    print(f"Configuration:")
    print(f"  Kuzu DB Path: {db_config['db_base_path']}")
    print(f"  Project Path: {project_path}")
    print(f"  Query Path: {query_path}")
    print(f"  Results Path: {result_log_base}")
    print(f"  Metadata Path: {metadata_path}")

    # Create executors — all three share the same base path;
    # set_graph() in assess_db will point each to the right database
    plain_ke = KuzuExecutor(db_base_path=db_config["db_base_path"])
    dewey_ke = KuzuExecutor(db_base_path=db_config["db_base_path"])
    prepost_ke = KuzuExecutor(db_base_path=db_config["db_base_path"])

    assess_db(
        plain_ex=plain_ke,
        dewey_ex=dewey_ke,
        prepost_ex=prepost_ke,
        result_log_base=result_log_base,
        query_path=query_path,
        metadata_path=metadata_path,
        heat=exp_config["heat"],
        n=exp_config["n"],
        parametrizer_cls=ReducedKuzuParametrizer
    )


if __name__ == "__main__":
    run_experiment()
