import os
import sys
from pathlib import Path
from experiments.experiement_infrastructure import assess_db, Neo4jExecutor, ReducedKuzuParametrizer

# Add parent directory to path for imports when running as script
sys.path.insert(0, str(Path(__file__).parent.parent))


def get_config():
    """
    Get configuration from environment variables with sensible defaults.

    Environment variables:
        NEO4J_URI: Bolt URI for Neo4j (default: bolt://neo4j_treebench_db:7687)
        NEO4J_USER: Neo4j username (default: neo4j)
        NEO4J_PASSWORD: Neo4j password (default: treebenchPW)
        PROJECT_PATH: Base project path (default: current working directory's parent)
        QUERIES_SUBPATH: Path to query files relative to PROJECT_PATH
        RESULTS_SUBPATH: Path to results directory relative to PROJECT_PATH
        METADATA_SUBPATH: Path to graph metadata directory
        EXPERIMENT_HEAT: Number of warmup iterations (default: 0)
        EXPERIMENT_N: Number of timed iterations (default: 5)

    For Docker container execution (default):
        NEO4J_URI=bolt://neo4j_treebench_db:7687
        PROJECT_PATH=/project

    For local execution:
        NEO4J_URI=bolt://localhost:7687
        PROJECT_PATH=<project_root>
    """
    default_project_path = Path(__file__).parent.parent.parent

    return {
        "db": {
            "uri": os.getenv("NEO4J_URI", "bolt://neo4j_treebench_db:7687"),
            "user": os.getenv("NEO4J_USER", "neo4j"),
            "password": os.getenv("NEO4J_PASSWORD", "treebenchPW"),
        },
        "paths": {
            "project": Path(os.getenv("PROJECT_PATH", str(default_project_path))),
            "queries_subpath": os.getenv("QUERIES_SUBPATH", "queries/neo4j/cypher"),
            "results_subpath": os.getenv("RESULTS_SUBPATH", "results/neo4j/raw_expanded_mega_3"),
            "metadata_subpath": os.getenv("METADATA_SUBPATH", "data/graph_metadata"),
        },
        "experiment": {
            "heat": int(os.getenv("EXPERIMENT_HEAT", "0")),
            "n": int(os.getenv("EXPERIMENT_N", "5")),
        }
    }


def run_experiment(config=None):
    """Run the Neo4j experiment with the given configuration."""
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
    print(f"  Neo4j URI: {db_config['uri']}")
    print(f"  Project Path: {project_path}")
    print(f"  Query Path: {query_path}")
    print(f"  Results Path: {result_log_base}")
    print(f"  Metadata Path: {metadata_path}")

    # Create executors — all three share the same driver connection;
    # set_graph() in assess_db will point each to the right database
    plain_ne = Neo4jExecutor(
        uri=db_config["uri"],
        user=db_config["user"],
        password=db_config["password"],
    )

    dewey_ne = Neo4jExecutor(
        uri=db_config["uri"],
        user=db_config["user"],
        password=db_config["password"],
    )

    prepost_ne = Neo4jExecutor(
        uri=db_config["uri"],
        user=db_config["user"],
        password=db_config["password"],
    )

    assess_db(
        plain_ex=plain_ne,
        dewey_ex=dewey_ne,
        prepost_ex=prepost_ne,
        result_log_base=result_log_base,
        query_path=query_path,
        metadata_path=metadata_path,
        heat=exp_config["heat"],
        n=exp_config["n"],
        parametrizer_cls=ReducedKuzuParametrizer
    )


if __name__ == "__main__":
    run_experiment()
