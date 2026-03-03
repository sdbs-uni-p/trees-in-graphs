# trees-in-graphs-bench
Experiments on indexing and querying tree structures inside property graphs

## Kuzu

### Setup

Start the container from the `docker/kuzu/` directory:

```bash
cd docker/kuzu
docker compose up -d
```

The entrypoint automatically runs `docker/kuzu/init/00_init_kuzu.py` on first startup, which creates one Kuzu database per graph variant under the `kuzu_treebench_data` Docker volume. The container is ready when the healthcheck passes (it polls for a `.initialized` sentinel file).

### Running experiments

Execute the experiment runner inside the container from the project root:

```bash
docker exec -it -w /project kuzu_treebench python -m experiments.kuzu.kuzu_experiment_def
```

Results are written to `results/kuzu/raw_expanded_mega_8/`.

---

## Neo4j

### Setup

The Neo4j stack consists of two containers: the database (`neo4j_treebench_db`) and an init container (`neo4j_treebench_init`) that waits for the database to be healthy before populating it.

A `.env` file is required in `docker/neo4j/` with the database credentials. The defaults are:

```
NEO4J_USER=neo4j
NEO4J_PASSWORD=treebenchPW
```

Start both containers from the `docker/neo4j/` directory:

```bash
cd docker/neo4j
docker compose up -d
```

The init container automatically runs `docker/neo4j/init/00_init_neo4j.py`, which creates one Neo4j database per graph variant. It is ready when the healthcheck passes (it polls for a `.initialized` sentinel file).

### Running experiments

Execute the experiment runner inside the init container from the project root:

```bash
docker exec -it -w /project neo4j_treebench_init python -m experiments.neo4j.neo4j_experiment_def
```

Results are written to `results/neo4j/raw_expanded_mega_2/`.

---

## Generating reports

`results/results_analysis/kuzu_report.py` takes a directory of raw JSON result files and produces a self-contained output directory containing:

| File | Description |
|---|---|
| `results.csv` | Median client-side runtime per graph / query / annotation |
| `slowdown.csv` | Every annotated (graph, query, annotation) combination ranked by slowdown relative to baseline |
| `speedup_heatmap.png` | Side-by-side heatmap of dewey and prepost speedup over baseline |

### Usage

```bash
python results/results_analysis/kuzu_report.py \
    --raw-results <path-to-json-dir> \
    --output <output-dir> \
    [--gdms <name>]
```

| Argument | Required | Description |
|---|---|---|
| `--raw-results` | Yes | Directory containing the raw `.json` result files |
| `--output` | Yes | Output directory to create (created if it does not exist) |
| `--gdms` | No | Database system name shown in the heatmap title (default: `Kuzu`) |

### Examples

Kuzu results:
```bash
python results/results_analysis/kuzu_report.py \
    --raw-results results/kuzu/raw_expanded_mega_8 \
    --output results/results_analysis/kuzu_report
```

Neo4j results:
```bash
python results/results_analysis/kuzu_report.py \
    --raw-results results/neo4j/raw_expanded_mega_2 \
    --output results/results_analysis/neo4j_report \
    --gdms Neo4j
```
