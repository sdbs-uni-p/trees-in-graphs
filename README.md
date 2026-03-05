# Trees in Graphs
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

Results are written to `results/kuzu/results_raw_<TIMESTAMP>/`, where `<TIMESTAMP>` is the starting time of the experiment.

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

Results are written to `results/neo4j/results_raw__<TIMESTAMP>/`, where `<TIMESTAMP>` is the starting time of the experiment.

---

## AGE

### Setup

Start the container from the `docker/age/` directory:

```bash
cd docker/age
docker compose up -d
```

The entrypoint runs a resumable init chain (`entrypoint-resumable-init.sh` + `run-all-init.sh`) that creates graphs, loads prepared data, and builds tree indexes. The container is ready when the healthcheck passes (it checks for `.init_complete` and `pg_isready`).

Default credentials are read from `docker/age/.env`:

```
POSTGRES_USER=postgresUser
POSTGRES_PASSWORD=postgresPW
POSTGRES_DB=postgresDB
```

### Running experiments

Run the experiment script inside the container from the project root:

```bash
docker exec -it -w /experiments age_treebench bash run_experiments.sh
```

Show available options:

```bash
docker exec -it -w /experiments age_treebench bash run_experiments.sh --help
```

Example with selected queries/datasets and multiple runs:

```bash
docker exec -it -w /experiments age_treebench bash run_experiments.sh \
    --queries 01,02,05,11 \
    --datasets snb_sf1_comment,artificial_trees_ultrawide_100 \
    --runs 3 --warmup
```

Results are written to timestamped folders under `results/age/<YYYYMMDD_HHMMSS>/runtimes.csv`.

---

## Scripts

The scripts in `scripts/` are helper utilities for combining, plotting, and viewing cross-system baseline comparisons.

### `scripts/compare_baseline_kuzu_age.py`

Compares baseline median runtimes from Kuzu and AGE and writes a joined CSV.

```bash
python scripts/compare_baseline_kuzu_age.py --kuzu <path|dir|paper|latest> --age <path|dir|paper|latest>
```

- Inputs:
    - `--kuzu`: Kuzu CSV file, Kuzu results directory, or shorthand `paper` / `latest`
    - `--age`: AGE CSV file, AGE results directory, or shorthand `paper` / `latest`
    - `--out` (optional): explicit output CSV path
- Default output (if `--out` is omitted):
    - `results/combined/age_<tag>_kuzu_<tag>/baseline_kuzu_age_compare.csv`
- Tag selection rule:
    - Use timestamp folder (`YYYYMMDD_HHMMSS`) if present
    - else use `paper` if present
    - else use a fallback derived from the provided path

Examples:

```bash
python scripts/compare_baseline_kuzu_age.py --age paper --kuzu latest
python scripts/compare_baseline_kuzu_age.py --age results/age/20260301_130056/runtimes.csv --kuzu results/kuzu/paper/results.csv
```

### `scripts/plot_speedup_compare.py`

Creates per-query speedup PDFs for AGE/Kuzu/Neo4j.

```bash
python scripts/plot_speedup_compare.py --age <path|dir|glob|paper|latest> [--kuzu ...] [--neo4j ...]
```

- Inputs:
    - `--age`, `--kuzu`, `--neo4j` accept file, directory, glob, or shorthand `paper` / `latest`
- Default output directory (if `--out-dir` is omitted):
    - `results/combined/<db_tag_pairs>/`
- Produces one PDF per AGE query:
    - `speedup_compare_<query>.pdf`

Examples:

```bash
python scripts/plot_speedup_compare.py --age latest --kuzu paper
python scripts/plot_speedup_compare.py --age results/age/20260301_130056/runtimes.csv --kuzu results/kuzu/paper --legend-placement outside
```

### `scripts/view_baseline_kuzu_age.py`

Shows filtered rows from a combined baseline comparison CSV in a table.

```bash
python scripts/view_baseline_kuzu_age.py [--csv <file>] [--query <id|name[,id|name...]>]
```

- `--csv` (optional): explicit comparison CSV
    - default: latest `results/combined/**/baseline_kuzu_age_compare.csv`
- `--query` (optional, repeatable or comma-separated)
    - IDs: `01`, `02`, `05`, `11`
    - names: `all_descendants`, `all_children`, `all_leaves`, `check_if_ancestor`
    - if omitted: all queries are included

Examples:

```bash
python scripts/view_baseline_kuzu_age.py
python scripts/view_baseline_kuzu_age.py --query 01,11
python scripts/view_baseline_kuzu_age.py --csv results/combined/age_20260301_130056_kuzu_paper/baseline_kuzu_age_compare.csv --query all_children
```

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
python results/results_analysis/kuzu_neo4j_report.py \
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
python results/results_analysis/kuzu_neo4j_report.py \
    --raw-results results/kuzu/raw_expanded_mega_8 \
    --output results/results_analysis/kuzu_report
```

Neo4j results:
```bash
python results/results_analysis/kuzu_neo4j_report.py \
    --raw-results results/neo4j/raw_expanded_mega_2 \
    --output results/results_analysis/neo4j_report \
    --gdms Neo4j
```
