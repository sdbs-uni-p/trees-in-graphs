# Trees in Graphs: Benchmarking Tree Queries in Property Graphs
This repository contains the full experimental setup for benchmarking tree structures in property graphs. It includes database-specific query suites, execution scripts, generated reference results, and instructions to reproduce the paper setup.

In addition to raw and aggregated result files, the repository provides visual artifacts. These include speedup heatmaps for [Kuzu](results/kuzu/paper_results/analysis/speedup_heatmap.png) and [Neo4j](results/neo4j/paper_results/analysis/speedup_heatmap.png), as well as cross-system speedup plots for Kuzu and Apache AGE (one plot per query; x-axis abbreviations: `WT 1` = Wide tree (100 nodes), `WT 2` = Wide tree (1,000 nodes), `WT 3` = Wide tree (10,000 nodes), `DT` = Deep tree (10,000 nodes), `TF` = Tiny Forest (40 nodes), `SNB/C` = Comment tree in full LDBC SNB SF1 graph, `SNB/P` = Place tree in full LDBC SNB SF1 graph, `SNB/T` = Tagclass tree in full LDBC SNB SF1 graph):
- [Find all descendants of a fixed node](results/combined/paper_results/speedup_all_descendants_kuzu_age.pdf)
- [Find all children of a fixed node](results/combined/paper_results/speedup_all_children_kuzu_age.pdf)
- [Find all leaves of a fixed node](results/combined/paper_results/speedup_all_leaves_kuzu_age.pdf)
- [Check the ancestor-descendant relationship of two fixed nodes](results/combined/paper_results/speedup_check_if_ancestor_kuzu_age.pdf)

For a quick start, use the container setup below and then run the experiment/report commands in the documented order.

## Contents

- [Setup](#setup)
  - [Kuzu](#kuzu)
  - [Neo4j](#neo4j)
  - [Apache AGE](#apache-age)
- [Running Experiments](#running-experiments)
  - [Kuzu](#kuzu-1)
  - [Neo4j](#neo4j-1)
  - [Apache AGE](#apache-age-1)
- [Queries](#queries)
  - [Directory Structure](#directory-structure)
  - [Encoding Schemes](#encoding-schemes)
  - [Query Naming](#query-naming)
  - [Comparing Queries across Systems and Schemes](#comparing-queries-across-systems-and-schemes)
- [Generating Reports](#generating-reports)
  - [Kuzu and Neo4j](#kuzu-and-neo4j)
    - [Heatmap](#heatmap)
  - [Apache AGE](#apache-age-2)
  - [Cross-system Comparisons](#cross-system-comparisons)
    - [Speedup Plots per Query](#speedup-plots-per-query)
    - [Baseline Runtime Comparison for Kuzu and Apache AGE](#baseline-runtime-comparison-for-kuzu-and-apache-age)
- [Datasets](#datasets)
  - [Artificial Trees and Forests](#artificial-trees-and-forests)
  - [LDBC Social Network Benchmark](#ldbc-social-network-benchmark)
- [Citation](#citation)
- [Acknowledgment](#acknowledgment)

## Setup

### Kuzu

Start the container from the `docker/kuzu/` directory:

```bash
cd docker/kuzu
docker compose up -d
```

The entrypoint automatically runs `docker/kuzu/init/00_init_kuzu.py` on first startup, which creates one Kuzu database per graph variant under the `kuzu_treebench_data` Docker volume. The container is ready when the healthcheck passes (it polls for a `.initialized` sentinel file).

To avoid Linux permission conflicts when using `docker exec --user <uid>:<gid>`, the Kuzu entrypoint normalizes `/kuzu_data` permissions during startup.

### Neo4j

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

### Apache AGE

A `.env` file is required in `docker/age/` with the database credentials. The defaults are:

```
POSTGRES_USER=postgresUser
POSTGRES_PASSWORD=postgresPW
POSTGRES_DB=postgresDB
```

Start the container from the `docker/age/` directory:

```bash
cd docker/age
docker compose up -d
```

The AGE container automatically runs a resumable init chain (`entrypoint-resumable-init.sh` + `run-all-init.sh`) that creates graphs, loads prepared data, and builds tree indexes. It is ready when the healthcheck passes (it checks for `.init_complete` and `pg_isready`).

---

## Running Experiments

On Linux, `docker exec` runs as `root` by default. This can create root-owned files/directories on bind mounts (for example under `results/`), which then causes permission issues on the host.

To avoid this, run experiment commands with a mapped host user (`-u "$(id -u):$(id -g)"`). On Windows/macOS this is typically not required.

### Kuzu

Execute the experiment runner inside the container from the project root:

```bash
docker exec -it -w /project kuzu_treebench python -m experiments.kuzu.kuzu_experiment_def
```

On Linux, prefer using a mapped host user:

```bash
docker exec -it -u "$(id -u):$(id -g)" -w /project kuzu_treebench python -m experiments.kuzu.kuzu_experiment_def
```

Results are written to a timestamped folder `results/kuzu/<YYYYMMDD_HHMMSS>/raw/`.
Reference results are stored under `results/kuzu/paper_results/raw/`.

### Neo4j

Execute the experiment runner inside the init container from the project root:

```bash
docker exec -it -w /project neo4j_treebench_init python -m experiments.neo4j.neo4j_experiment_def
```

On Linux, prefer using a mapped host user:

```bash
docker exec -it -u "$(id -u):$(id -g)" -w /project neo4j_treebench_init python -m experiments.neo4j.neo4j_experiment_def
```

Results are written to a timestamped folder `results/neo4j/<YYYYMMDD_HHMMSS>/raw/`.
Reference results are stored under `results/neo4j/paper_results/raw/`.

### Apache AGE

AGE experiments are started via `run_experiments.sh` in the `age_treebench` container.

Options:

| Option | Description |
|---|---|
| `-q`, `--queries LIST` | Comma-separated query IDs or filenames/globs, e.g. `01,02`, `01_foo.sql`, `0*`. |
| `-d`, `--datasets LIST` | Comma-separated datasets/graph names or globs, e.g. `snb_sf1_comment`, `artificial_trees_truebase_100`, `snb*`. |
| `-n`, `--note TEXT` | Optional run note; appended to `results/age/notes.txt`. |
| `-w`, `--warmup` | Run one warmup execution per query before measurements. |
| `-r`, `--runs N` | Number of measurement runs per query (default: `1`). |
| `-t`, `--timeout-ms N` | Statement timeout in milliseconds (default: `3600000`). |
| `--timing-off` | Use `EXPLAIN (ANALYZE, TIMING OFF)` for plan runtime measurement. |
| `--save-plans` | Save one explain plan per graph/query after measurements. |
| `--save-results` | Save one result output per graph/query after measurements. |
| `--save-queries` | Save rendered query files. |
| `-h`, `--help` | Show help. |

Reproducing The Paper Setup:

```bash
docker exec -it -w /experiments age_treebench bash run_experiments.sh \
    --runs 5 \
    --save-plans \
    --save-results \
    --save-queries
```

On Linux, prefer using a mapped host user:

```bash
docker exec -it -u "$(id -u):$(id -g)" -w /experiments age_treebench bash run_experiments.sh \
  --runs 5 \
  --save-plans \
  --save-results \
  --save-queries
```

Results are written to a timestamped folder `results/age/<YYYYMMDD_HHMMSS>`.
Reference results for the paper setup are stored under `results/age/paper_results/`.

---

## Queries

The `queries/` directory contains all tree-traversal query implementations, organised by database system and tree-encoding scheme.

### Directory Structure

```
queries/
├── age/
│   ├── baseline/       # 4 queries
│   ├── dewey/          # 4 queries
│   └── prepost/        # 4 queries
├── kuzu/
│   ├── baseline/       # 12 queries
│   ├── dewey/          # 12 queries
│   └── prepost/        # 12 queries
└── neo4j/
    ├── baseline/       # 12 queries
    ├── dewey/          # 12 queries
    └── prepost/        # 12 queries
```

Neo4j and Kuzu each implement 10 distinct operations per encoding (30 files each); AGE implements a subset of 4 operations per encoding (12 files total).

### Encoding Schemes

Each database system implements the same logical operations under three different tree-encoding strategies:

| Scheme | Description |
|---|---|
| `baseline` | Recursive graph traversal following stored edge relationships directly |
| `dewey` | Hierarchical string IDs (e.g. `"1.2.3"`); descendants are found via prefix matching |
| `prepost` | DFS pre-/post-order integers; subtree membership is a range-containment check |

### Query Naming

Files follow the pattern `{NN}_{operation}.sql`, where the numeric prefix groups equivalent operations across schemes and systems:

| ID | Operation |
|---|---|
| `01` | `all_descendants` |
| `02` | `all_children` |
| `05` | `all_leaves` |
| `06` | `count_descendants` |
| `07` | `count_leaves` |
| `08` | `check_same_subtree` (positive case) |
| `10` | `all_ancestors` |
| `11` | `check_if_ancestor` (positive case) |
| `12` | `check_same_subtree` (negative case) |
| `14` | `check_if_ancestor` (negative case) |

### Comparing Queries across Systems and Schemes

To compare queries that implement the same logical operation, open the three scheme variants side-by-side. For example, for `all_descendants` on Neo4j:

```
queries/neo4j/baseline/01_all_descendants.sql
queries/neo4j/dewey/01_all_descendants.sql
queries/neo4j/prepost/01_all_descendants.sql
```

The numeric prefix is stable across systems, so the same ID in `queries/kuzu/baseline/` and `queries/age/baseline/` implements the same logical operation — making cross-system, same-scheme comparisons straightforward as well.

All queries are parameterised (e.g. `$NODE_TYPE`, `$rootID`); the experiment runners substitute concrete values at runtime.

---

## Generating Reports

### Kuzu and Neo4j

`scripts/kuzu_neo4j_report.py` takes a directory of raw JSON result files and writes report files to an analysis directory.

| File | Description |
|---|---|
| `results.csv` | Median client-side runtime per graph / query / annotation |
| `slowdown.csv` | Every annotated (graph, query, annotation) combination ranked by slowdown relative to baseline |
| `speedup_heatmap.png` | Side-by-side heatmap of dewey and prepost speedup over baseline |

Usage:

```bash
python scripts/kuzu_neo4j_report.py \
    --raw-results <path-to-json-dir> \
    [--output <analysis-dir>] \
    [--gdms <name>]
```

| Argument | Required | Description |
|---|---|---|
| `--raw-results` | Yes | Directory containing the raw `.json` result files (typically `results/<db>/<tag>/raw`) |
| `--output` | No | Analysis output directory (default: sibling `analysis` when `--raw-results` ends with `raw`) |
| `--gdms` | No | Database system name shown in the heatmap title (default: `Kuzu`) |

Reproducing the Paper Results:

Kuzu:
```bash
python scripts/kuzu_neo4j_report.py \
    --raw-results results/kuzu/paper_results/raw
```

Neo4j:
```bash
python scripts/kuzu_neo4j_report.py \
    --raw-results results/neo4j/paper_results/raw \
    --gdms Neo4j
```

#### Heatmap

> **The generated heatmaps (`speedup_heatmap.png`) are written to the `--output` directory you specify.**

For the example commands above, the heatmaps are located at:

| Run | Heatmap path |
|---|---|
| Kuzu | `results/kuzu/paper_results/analysis/speedup_heatmap.png` |
| Neo4j | `results/neo4j/paper_results/analysis/speedup_heatmap.png` |

### Apache AGE

The AGE reference results are generated directly by the AGE experiment script (`experiments/age/run_experiments.sh`) analogously to Kuzu/Neo4j runs.

Reference and run output locations:

| Path | Description |
|---|---|
| `results/age/paper_results/` | AGE paper/reference result folder |
| `results/age/<YYYYMMDD_HHMMSS>/` | AGE timestamped result folder generated by an experiment run |

Typical AGE output files/folders inside these directories:

| File/Folder | Description |
|---|---|
| `runtimes.csv` | AGE runtimes CSV used by cross-system comparison scripts |
| `plans/` | Saved explain plans (`--save-plans`) |
| `results/` | Saved query result payloads (`--save-results`) |
| `queries/` | Saved executed queries (`--save-queries`) |
| `errors/` | Error/timeout logs generated during execution |

This CSV is the AGE input used by the cross-system comparison scripts below.

### Cross-system Comparisons

#### Speedup Plots per Query

`scripts/plot_speedups.py`

Creates per-query speedup PDFs from AGE, Kuzu, and/or Neo4j results.

| Output | Description |
|---|---|
| `speedup_<query>.pdf` | One speedup chart per query found in the provided input CSVs |

Usage:

```bash
python scripts/plot_speedups.py (--age <...> | --kuzu <...> | --neo4j <...>) [OPTION]...
```

At least one of `--age`, `--kuzu`, or `--neo4j` must be provided.

| Argument | Required | Description |
|---|---|---|
| `--age` | No | AGE result input (file, directory, glob, or `paper`/`latest`) |
| `--kuzu` | No | Kuzu result input (file, directory, glob, or `paper`/`latest`) |
| `--neo4j` | No | Neo4j result input (file, directory, glob, or `paper`/`latest`) |
| `--out-dir` | No | Output directory for generated PDFs (default: `results/combined/<db_tag_pairs>/`) |
| `--timeout-ms` | No | Timeout threshold in ms used to derive lower speedup bounds for timeout cases (default: `300000`) |
| `--crop-query-max` | No | Repeatable mapping `QUERY:MAX_Y`, e.g. `01:300` or `01:3e2`, to set the y-axis upper bound for a query |
| `--legend-query-numbers` | No | `all` (default), `none`, or comma-separated query IDs to control where legends are rendered |
| `--legend-placement` | No | Legend placement relative to axes: `inside` (default) or `outside` |
| `--legend-align` | No | Legend horizontal alignment: `left`, `center`, `right`, or numeric (`0..1` / percent `0..100`) |
| `--legend-columns` | No | Maximum number of legend columns (default: `2`) |
| `--legend-order` | No | Legend item order mode: `db-suffix` (default), `suffix-db`, or `reverse` |
| `--baseline-legend-position` | No | Insertion index for baseline in legend (`0`-based, negative values from end) |
| `--baseline-label-placement` | No | Baseline label mode: `none` (default), `line`, or `legend` |
| `--label-shift` | No | Repeatable label offset rule in format `QUERY:BAR:DX:DY[:CURV]` |

Meaning of `--legend-order` values:

- `db-suffix` (default): Legend grouped by database first, then suffix (annotation scheme).
- `suffix-db`: Legend grouped by suffix first, then database.
- `reverse`: Reverse of the default `db-suffix` order.

Meaning of `--label-shift QUERY:BAR:DX:DY[:CURV]`:

- `QUERY`: Numeric query ID (e.g. `01`, `2`, `11`; internally normalized to integer form).
- `BAR`: Bar index to move; multiple bars can be grouped with `/` (e.g. `0/1/2`).
  Grouping is only valid when all selected bars have the same label text (for example all `>10^x` or all `<10^-x`).
  In grouped mode, these bars are rendered with one shared label position instead of one label per bar.
- `DX`: Horizontal label shift (float).
- `DY`: Vertical label shift (float).
- `CURV` (optional): Integer curvature for an arrow/callout.
  If `CURV` is provided, an arrow from label to bar is drawn; if omitted, no arrow is drawn.
  For a straight arrow, use `CURV=0`.

Examples:

- `01:0/1:-9:0` shifts labels of bars `0` and `1` for query `01` by `dx=-9`, `dy=0`.
- `05:4:-15:0` shifts only bar `4` for query `05`.
- `02:0/1:0:8:0` same shift as above, but with explicit curvature value.

Reproducing the Paper Results:

```bash
python scripts/plot_speedups.py \
    --kuzu paper \
    --neo4j paper \
    --legend-query-numbers 01 \
    --legend-placement outside \
    --legend-align 50 \
    --legend-columns 5 \
    --baseline-legend-position 2 \
    --baseline-label-placement legend \
    --label-shift 01:0/1:-9:0 \
    --label-shift 02:0/1:-9:0 \
    --label-shift 05:0/1:-9:0 \
    --label-shift 05:2/3:0:0 \
    --label-shift 05:4:-15:0
```

#### Baseline Runtime Comparison for Kuzu and Apache AGE

`scripts/compare_baseline_kuzu_age.py`

Compares baseline median runtimes from Kuzu and AGE and writes a joined CSV.

Usage:

```bash
python scripts/compare_baseline_kuzu_age.py \
    --kuzu <path|dir|paper|latest> \
    --age <path|dir|paper|latest> \
    [--out <output-csv>]
```

| Argument | Required | Description |
|---|---|---|
| `--kuzu` | Yes | Kuzu CSV, result directory, or shorthand `paper` / `latest` |
| `--age` | Yes | AGE CSV, result directory, or shorthand `paper` / `latest` |
| `--out` | No | Explicit output CSV path (default: auto path under `results/combined/`) |

Examples:

```bash
python scripts/compare_baseline_kuzu_age.py --age paper --kuzu latest
python scripts/compare_baseline_kuzu_age.py --age results/age/20260301_130056 --kuzu results/kuzu/paper_results/analysis
```

`scripts/view_baseline_kuzu_age.py`

Shows filtered rows from a combined baseline comparison CSV in a table.

Usage:

```bash
python scripts/view_baseline_kuzu_age.py [--csv <file>] [--query <id|name[,id|name...]>]
```

| Argument | Required | Description |
|---|---|---|
| `--csv` | No | Explicit comparison CSV (default: latest `results/combined/**/baseline_kuzu_age_compare.csv`) |
| `--query` | No | Query filter by IDs (`01`, `02`, `05`, `11`) or names (`all_descendants`, `all_children`, `all_leaves`, `check_if_ancestor`) |

Examples:

```bash
python scripts/view_baseline_kuzu_age.py
python scripts/view_baseline_kuzu_age.py --query 01,11
python scripts/view_baseline_kuzu_age.py --csv results/combined/age_20260301_130056_kuzu_paper_results/baseline_kuzu_age_compare.csv --query all_children
```

---

## Datasets

### Artificial Trees and Forests
The datasets in `data/prepared/artificial_trees` and `data/prepared/artificial_forests` are synthetic datasets created by us.

### LDBC Social Network Benchmark
The datasets in `data/prepared/snb/sf1` are derived from the LDBC Social Network Benchmark (SNB).

Original datasets: https://ldbcouncil.org/benchmarks/snb/datasets/

The original LDBC SNB CSV files were modified as follows:
- Field delimiter changed from `|` to `,`
- Vertex IDs shifted when necessary to avoid IDs < 1 (required by Apache AGE)

These changes do not modify the graph structure.

These experiments are not official LDBC Benchmark results.
See the LDBC benchmark fair use policy:
https://ldbcouncil.org/benchmarks/fair-use-policies/

---

## Citation

If you use this artifact, please cite:
````bibtex
@misc{treesingraphs_artifact,
  title        = {Trees in Graphs: Benchmarking Tree Queries in Property Graphs},
  author       = {Daniel Aarao Reis Arturi, Christoph Köhnen, George Fletcher, Bettina Kemme, Stefanie Scherzinger},
  year         = {2026},
  howpublished = {\url{https://github.com/sdbs-uni-p/trees-in-graphs}}
}
````

---

## Acknowledgment

This project/research was partly funded by the Passau International Centre for Advanced Interdisciplinary Studies (PICAIS) of the University of Passau, Germany.
