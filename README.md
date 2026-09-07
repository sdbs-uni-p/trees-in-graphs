# Trees in Graphs: Benchmarking Tree Queries in Property Graphs
This repository contains the full experimental setup for the paper "Seeing the Trees for the Forest: Leveraging Tree-Shaped Substructures in Property Graphs". It includes database-specific query suites, execution scripts, generated reference results, and instructions to reproduce the paper setup. An older technical report can be found [here](https://arxiv.org/abs/2603.12476).

The speedups of the scenarios discussed in the paper are presented [here](results/combined/paper_results/runtime_tables_rounded.pdf).

For a quick start, use the container setup below and then run the experiment/report commands in the documented order.

## Contents

- [Setup](#setup)
  - [Kuzu](#kuzu)
    - [Kuzu LDBC](#kuzu-ldbc)
  - [Neo4j](#neo4j)
    - [Neo4j LDBC](#neo4j-ldbc)
  - [Apache AGE](#apache-age)
    - [AGE maintenance](#age-maintenance)
    - [AGE LDBC](#age-ldbc)
- [Running Experiments](#running-experiments)
  - [Kuzu](#kuzu-1)
    - [Kuzu LDBC](#kuzu-ldbc-1)
  - [Neo4j](#neo4j-1)
    - [Neo4j LDBC](#neo4j-ldbc-1)
  - [Apache AGE](#apache-age-1)
    - [AGE maintenance](#age-maintenance-1)
    - [AGE LDBC](#age-ldbc-1)
- [Queries](#queries)
  - [Directory Structure](#directory-structure)
  - [Encoding Schemes](#encoding-schemes)
  - [Query Naming](#query-naming)
  - [Comparing Queries across Systems and Schemes](#comparing-queries-across-systems-and-schemes)
- [Generating Reports](#generating-reports)
  - [Single-system runtime tables](#single-system-runtime-tables)
  - [Maintenance runtime tables](#maintenance-runtime-tables)
  - [Combined runtime tables](#combined-runtime-tables)
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

#### Kuzu LDBC

Start the container from the `docker/kuzu_ldbc/` directory:

```bash
cd docker/kuzu_ldbc
docker compose up -d --build
```

The LDBC container initializes the complete SNB SF1 databases for the
`baseline`, `dewey`, and `prepost` representations. It is ready when the
healthcheck finds `.initialized` and `max_depths.json`.

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

#### Neo4j LDBC

Start the database and initialization containers from `docker/neo4j_ldbc/`:

```bash
cd docker/neo4j_ldbc
docker compose up -d --build
```

The initialization container populates the complete SNB SF1 databases for the
`baseline`, `dewey`, and `prepost` representations.

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

#### AGE maintenance

Start the container from the `docker/age_maintenance/` directory:

```bash
cd docker/age_maintenance
docker compose up -d --build
```

Maintenance uses a separate PostgreSQL volume so that its writes do not affect
the regular AGE benchmark database.

#### AGE LDBC

Start the container from the `docker/age_ldbc/` directory:

```bash
cd docker/age_ldbc
docker compose up -d --build
```

The LDBC container initializes the complete SNB SF1 graphs for the `baseline`,
`dewey`, and `prepost` representations and is ready when `.init_complete` is
present and PostgreSQL is ready.

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

Results are written to `results/kuzu/<YYYYMMDD_HHMMSS>/runtimes.csv`.
Reference results are stored under `results/kuzu/paper_results/runtimes.csv`.

The regular Kuzu tree experiments run four queries: `01_all_descendants`,
`02_all_children`, `05_all_leaves`, and `11_check_if_ancestor`. The separate
LDBC runner below executes the three interactive SNB queries.

To run only the negative ancestor scenarios without warmup, with five measured
runs and all artifacts enabled:

```bash
docker exec -u "$(id -u):$(id -g)" -w /project \
  -e SCENARIO_FILTER=q09,q10 \
  -e EXPERIMENT_HEAT=0 -e EXPERIMENT_N=5 \
  -e SAVE_QUERIES=1 -e SAVE_PLANS=1 -e SAVE_RESULTS=1 \
  kuzu_treebench python -m experiments.kuzu.kuzu_experiment_def
```

#### Kuzu LDBC

The native Kuzu and Neo4j LDBC runners execute the three interactive SNB SF1
queries against complete `snb_sf1_baseline`, `snb_sf1_dewey`, and
`snb_sf1_prepost` databases. All three tree labels (`Comment`, `Place`, and
`TagClass`) are annotated together. Their fixed logical parameters are resolved
before the timed execution; this setup work is not part of `runtime_ms`.

```bash
docker exec -it -w /project kuzu_ldbc_treebench \
  python -m experiments.kuzu_ldbc.kuzu_ldbc_experiment_def
```

The runner defaults to five measured executions per query and writes results
under `results/kuzu_ldbc/`. Use `EXPERIMENT_N`, `EXPERIMENT_HEAT`,
`QUERY_FILTER`, `SAVE_QUERIES`, `SAVE_PLANS`, and `SAVE_RESULTS` to configure
the run. `QUERY_FILTER` accepts comma-separated query names or globs.

### Neo4j

Execute the experiment runner inside the init container from the project root:

```bash
docker exec -it -w /project neo4j_treebench_init python -m experiments.neo4j.neo4j_experiment_def
```

On Linux, prefer using a mapped host user:

```bash
docker exec -it -u "$(id -u):$(id -g)" -w /project neo4j_treebench_init python -m experiments.neo4j.neo4j_experiment_def
```

Results are written to `results/neo4j/<YYYYMMDD_HHMMSS>/runtimes.csv`.
Reference results are stored under `results/neo4j/paper_results/runtimes.csv`.

The regular Neo4j tree experiments run the same four queries as Kuzu. The
separate LDBC runner above executes the three interactive SNB queries.

To run only the negative ancestor scenarios without warmup, with five measured
runs and all artifacts enabled:

```bash
docker exec -u "$(id -u):$(id -g)" -w /project \
  -e SCENARIO_FILTER=q09,q10 \
  -e EXPERIMENT_HEAT=0 -e EXPERIMENT_N=5 \
  -e SAVE_QUERIES=1 -e SAVE_PLANS=1 -e SAVE_RESULTS=1 \
  neo4j_treebench_init python -m experiments.neo4j.neo4j_experiment_def
```

#### Neo4j LDBC

```bash
docker exec -it -w /project neo4j_ldbc_treebench_init \
  python -m experiments.neo4j_ldbc.neo4j_ldbc_experiment_def
```

The runner uses the same LDBC options as Kuzu and writes results under
`results/neo4j_ldbc/`.

### Apache AGE

AGE experiments are started via `run_experiments.sh` in the `age_treebench` container.

Options:

| Option | Description |
|---|---|
| `-q`, `--queries LIST` | Comma-separated query IDs or filenames/globs, e.g. `01,02`, `01_foo.sql`, `0*`. |
| `-d`, `--datasets LIST` | Comma-separated datasets/graph names or globs, e.g. `snb_sf1_comment`, `artificial_trees_truebase_100`, `snb*`. |
| `-s`, `--scenarios LIST` | Comma-separated scenario names or globs, e.g. `q03,q04,q05` or `q0?`. |
| `-n`, `--note TEXT` | Optional run note; appended to `results/age/notes.txt`. |
| `-w`, `--warmup` | Run one warmup execution per query before measurements. |
| `-r`, `--runs N` | Number of measurement runs per query (default: `1`). |
| `-t`, `--timeout-ms N` | Statement timeout in milliseconds (default: `3600000`). |
| `--timing-off` | Use `EXPLAIN (ANALYZE, TIMING OFF)` for plan runtime measurement. |
| `--save-plans` | Save one explain plan per graph/query after measurements. |
| `--save-results` | Save one result output per graph/query after measurements. |
| `--save-queries` | Save rendered query files. |
| `--parameters-file FILE` | Parameter CSV (default: `experiments/query_parameters.csv`). |
| `-h`, `--help` | Show help. |

The parameter CSV uses the long format `graph,query,scenario,parameter,value`,
so it has no empty fields: only parameters used by a query appear in its rows.
A scenario groups the parameters for one execution of a query; every scenario
is measured independently. Scenarios with identical parameter values for the
same graph and query are collapsed to one execution; their IDs are combined
(for example, `q01_q02`). `graph` is the base graph name without the AGE
encoding suffix (`_baseline`, `_dewey`, or `_prepost`), so one parameter group
applies to all three representations. The current queries require `rootid`
(queries 01, 02, and 05) or `id1` and `id2` (query 11).

The fixed parameter scenarios are:

| Scenario | Parameters | Used by |
|---|---|---|
| `q01` | Root of the largest tree | Queries 01 and 05 |
| `q02` | Root of the deepest tree | Queries 01 and 05 |
| `q03` | Parent of leaves with high degree | Queries 01 and 05 |
| `q04` | Parent of leaves with low degree | Queries 01 and 05 |
| `q05` | Node with the highest degree | Query 02 |
| `q06` | Node with the lowest degree | Query 02 |
| `q07` | Root and a farthest leaf | Query 11 (`true`, long positive case) |
| `q08` | Parent and a deep direct child | Query 11 (`true`, short positive case) |
| `q09` | Shallow siblings; roots of different trees are siblings below an imaginary parent | Query 11 (`false`, short negative case, `shallow_siblings`) |
| `q10` | Leaves maximizing the sum of their distances to their lowest common ancestor, which may be the imaginary parent | Query 11 (`false`, long negative case, `distant_leaves`) |

For `11_check_if_ancestor`, the relationship is checked in both directions:
the result is true if either node is an ancestor of the other.

The `q09` and `q10` parameters are selected deterministically from the first
rows of the corresponding Top-20 reports under
`results/age/dewey_top20/<graph>_dewey/`.

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

#### AGE maintenance

Maintenance experiments are started via `run_experiments.sh` in the
`age_treebench_maintenance` container.

Options:

| Option | Description |
|---|---|
| `-q`, `--queries LIST` | Comma-separated query IDs, filenames, or globs. Examples: `01,03` or `01_insert_last_child_under_last_root.sql`. |
| `-d`, `--datasets LIST` | Comma-separated base graph names, graph names, or globs. |
| `-r`, `--runs N` | Number of measured runs per operation (default: `1`). |
| `-w`, `--warmup` | Run one rolled-back warmup execution before measurements. |
| `-t`, `--timeout-ms N` | Statement timeout in milliseconds (default: `3600000`). |
| `-n`, `--note TEXT` | Append a note to the maintenance run notes. |
| `--parameters-file FILE` | Root-parameter CSV (default: `experiments/maintenance_parameters.csv`). |
| `--save-queries` | Save rendered SQL files. |
| `--save-plans` | Save rolled-back `EXPLAIN ANALYZE` plans. |
| `--save-results` | Save rolled-back result snapshots and diffs. |
| `--timing-off` | Use `TIMING OFF` for saved plans. |
| `--compact-before` | Run `VACUUM FULL ANALYZE` once before measurements. |
| `--no-vacuum-between-runs` | Disable cleanup after rolled-back runs. |
| `-h`, `--help` | Show help. |

For `--queries`, use the numeric IDs from the scenario table below, complete
query filenames, or shell-style globs. For example, `--queries 01,02` runs the
two child insertions. For
`--datasets`, use a base name such as `snb_sf1_comment`, a complete graph name
such as `snb_sf1_comment_dewey`, or a glob such as `snb*`. The fixed parent
parameters are loaded from `experiments/maintenance_parameters.csv`; override
the file with `--parameters-file`.

The four scenarios are:

| Query ID | Scenario |
|---|---|
| `01` | Insert the last child under the last root. |
| `02` | Insert the first child under the first root. |
| `03` | Insert the last root. |
| `04` | Insert the first root. |

Reproducing The Paper Setup:

```bash
docker exec -it -w /experiments age_treebench_maintenance bash run_experiments.sh \
  --runs 5 \
  --warmup \
  --save-plans \
  --save-results \
  --save-queries
```

On Linux, prefer using a mapped host user:

```bash
docker exec -it -u "$(id -u):$(id -g)" -w /experiments age_treebench_maintenance bash run_experiments.sh \
  --runs 5 \
  --warmup \
  --save-plans \
  --save-results \
  --save-queries
```

With `--save-results`, the runner stores the post-maintenance snapshots and
unified before/after diffs in the run's `results/` and `diffs/` directories.
Snapshot and diff generation are outside `runtime_ms` and can produce large
artifacts for SNB.

Results are written to `results/age_maintenance/<YYYYMMDD_HHMMSS>/runtimes.csv`.

#### AGE LDBC

Experiments on the official LDBC SNB interactive queries are run via `experiments/age_ldbc/run_experiments.sh` in the `age_ldbc_treebench` container, using the baseline, Dewey, and pre/post query sets under `queries/age_ldbc/`.

The committed interactive queries are:

- `interactive-short-2`
- `interactive-short-6`
- `interactive-complex-12`

Options:

| Option | Description |
|---|---|
| `-t`, `--timeout-ms N` | Statement timeout in milliseconds (default: `3600000`) |
| `-q`, `--queries LIST` | Comma-separated query ids or filenames (globs supported) |
| `-d`, `--datasets LIST` | Comma-separated dataset or graph names (globs supported) |
| `-r`, `--runs N` | Number of measurement runs per query (default: `1`) |
| `--save-plans` | Save an execution plan per graph/query |
| `--save-results` | Save result output per graph/query |
| `--save-queries` | Save the original SQL query files |

Reproducing The Paper Setup:

```bash
docker exec -it -w /experiments/age_ldbc age_ldbc_treebench bash run_experiments.sh \
  --queries interactive-short-2,interactive-short-6,interactive-complex-12 \
  --runs 5 \
  --save-plans \
  --save-results \
  --save-queries
```

On Linux, prefer using a mapped host user:

```bash
docker exec -it -u "$(id -u):$(id -g)" -w /experiments/age_ldbc age_ldbc_treebench bash run_experiments.sh \
  --queries interactive-short-2,interactive-short-6,interactive-complex-12 \
  --runs 5 \
  --save-plans \
  --save-results \
  --save-queries
```

The script writes runtimes and the requested results and plans to a timestamped folder under `results/age_ldbc/`.

Unlike the native Kuzu and Neo4j LDBC runners, the AGE runner passes its query
parameters directly to the rendered SQL files; it does not perform a separate
structural-parameter lookup before timing.

---

## Queries

The `queries/` directory contains all tree-traversal query implementations, organised by database system and tree-encoding scheme.

The three committed official LDBC SNB interactive queries are available as
original Cypher under `queries/age_ldbc/original/` and as adapted queries under
`queries/age_ldbc/{baseline,dewey,prepost}/`,
`queries/kuzu_ldbc/{baseline,dewey,prepost}/`, and
`queries/neo4j_ldbc/{baseline,dewey,prepost}/`. Their benchmark outputs are
stored under the corresponding `results/*_ldbc/` directories.

### Directory Structure

```
queries/
├── age/
│   ├── baseline/       # 4 queries
│   ├── dewey/          # 4 queries
│   ├── prepost/        # 4 queries
├── age_ldbc/
│   ├── baseline/       # 3 official SNB queries adapted for AGE
│   ├── dewey/          # 3 official SNB queries with Dewey annotations
│   ├── original/       # 3 original Cypher queries
│   └── prepost/        # 3 official SNB queries with PrePost annotations
├── age_maintenance/
│   ├── baseline/       # 4 rolled-back insertion operations
│   ├── dewey/          # 4 insertion operations with Dewey annotations
│   └── prepost/        # 4 insertion operations with PrePost annotations
├── kuzu/
│   ├── baseline/       # 10 queries
│   ├── dewey/          # 10 queries
│   └── prepost/        # 10 queries
├── kuzu_ldbc/
│   ├── baseline/       # 3 official SNB queries
│   ├── dewey/          # 3 official SNB queries with Dewey annotations
│   └── prepost/        # 3 official SNB queries with PrePost annotations
├── neo4j/
│   ├── baseline/       # 10 queries
│   ├── dewey/          # 10 queries
│   └── prepost/        # 10 queries
└── neo4j_ldbc/
    ├── baseline/       # 3 official SNB queries
    ├── dewey/          # 3 official SNB queries with Dewey annotations
    └── prepost/        # 3 official SNB queries with PrePost annotations
```

Kuzu and Neo4j each implement 10 queries per encoding. AGE
implements only 4 queries per encoding: `01`, `02`, `05`, and
`11`. LDBC and maintenance are separate workloads with their own query names.

### Encoding Schemes

Each database system implements the same logical operations under three different tree-encoding strategies:

| Scheme | Description |
|---|---|
| `baseline` | Recursive graph traversal following stored edge relationships directly |
| `dewey` | Hierarchical string IDs (e.g. `"1.2.3"`); descendants are found via prefix matching |
| `prepost` | DFS pre-/post-order integers; subtree membership is a range-containment check |

### Query Naming

Tree query files follow the pattern `{NN}_{operation}.sql`, where the numeric
prefix groups equivalent tree operations across the baseline, Dewey, and
PrePost implementations:

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

Official LDBC query files use names such as `interactive-short-2.sql`,
`interactive-short-6.sql`, and `interactive-complex-12.sql` rather than the
tree-operation numbers.

Maintenance query files use the scenario IDs `01` to `04`:

| ID | Maintenance operation |
|---|---|
| `01` | Insert the last child under the last root |
| `02` | Insert the first child under the first root |
| `03` | Insert the last root |
| `04` | Insert the first root |

### Comparing Queries across Systems and Schemes

To compare queries that implement the same logical operation, open the three scheme variants side-by-side. For example, for `all_descendants` on Neo4j:

```
queries/neo4j/baseline/01_all_descendants.sql
queries/neo4j/dewey/01_all_descendants.sql
queries/neo4j/prepost/01_all_descendants.sql
```

The numeric prefix is stable across the regular tree-query implementations.
LDBC and maintenance queries are compared within their respective workloads.

All queries are parameterised (e.g. `$NODE_TYPE`, `$rootID`); the experiment runners substitute concrete values at runtime.

---

## Generating Reports

All current report commands operate on the timestamped `runtimes.csv` files
written by the experiment runners. The scripts require Python 3.10 or newer,
`rsvg-convert`, and `pdfunite`.

### Single-system runtime tables

Create a PDF for regular AGE tree results, LDBC results, or a query-specific
CSV with:

```bash
python scripts/create_runtime_tables.py \
  results/age/<run>/runtimes.csv \
  results/age/<run>/runtime_tables.pdf
```

| Argument | Description |
|---|---|
| `INPUT` | Runtime CSV to render. Supports regular Tree and LDBC result formats. |
| `OUTPUT` | PDF path to create. |
| `--timeout-log-dir CSV=ERRORS_DIR` | Use timeout/error logs from another directory; repeatable for copied or merged CSVs. |

Use the corresponding `results/kuzu/`, `results/neo4j/`, `results/age_ldbc/`,
`results/kuzu_ldbc/`, or `results/neo4j_ldbc/` path for other workloads. The
script renders regular tree scenarios and the three LDBC queries using the
same Baseline, Dewey, and PrePost runtime columns.

### Maintenance runtime tables

```bash
python scripts/create_maintenance_runtime_tables.py \
  results/age_maintenance/<run>/runtimes.csv \
  results/age_maintenance/<run>/runtime_tables.pdf
```

| Argument | Description |
|---|---|
| `INPUT` | Maintenance `runtimes.csv`. |
| `OUTPUT` | Optional PDF path; defaults to `runtime_tables.pdf` next to the input. |
| `--timeout-log-dir CSV=ERRORS_DIR` | Use timeout/error logs from another directory; repeatable. |

The PDF has one page per maintenance scenario and shows Baseline, Dewey, and
PrePost medians plus the corresponding maintenance-cost differences.

### Combined runtime tables

For a combined regular Tree/LDBC overview, provide the six input CSVs and
choose one or more output variants:

```bash
python scripts/create_combined_overview.py \
  --age paper --kuzu paper --neo4j paper \
  --age-ldbc paper --kuzu-ldbc paper --neo4j-ldbc paper \
  --table-output results/combined/paper_results/runtime_table_compact.pdf
```

Input options:

| Option | Description |
|---|---|
| `--age CSV\|paper` | Regular AGE Tree runtime CSV; `paper` selects the frozen paper input. |
| `--kuzu CSV\|paper` | Regular Kuzu Tree runtime CSV. |
| `--neo4j CSV\|paper` | Regular Neo4j Tree runtime CSV. |
| `--age-ldbc CSV\|paper` | AGE LDBC runtime CSV. |
| `--kuzu-ldbc CSV\|paper` | Kuzu LDBC runtime CSV. |
| `--neo4j-ldbc CSV\|paper` | Neo4j LDBC runtime CSV. |

Output options:

| Option | Description |
|---|---|
| `--table-output [PDF]` | Compact overview; optional path. |
| `--detailed-output [PDF]` | Exact-value overview; optional path. |
| `--rounded-output [PDF]` | Rounded-value overview; optional path. |
| `--timeout-log-dir CSV=ERRORS_DIR` | Use timeout/error logs from another directory; repeatable. |

Without an explicit output option, all three overview variants are written to
a new timestamped directory under `results/combined/`.

The overview selects the requested paper inputs, validates their provenance,
and writes compact, exact, and rounded PDFs. The lower-level three-system
table can be generated with:

```bash
python scripts/create_combined_runtime_tables.py \
  --age results/age/<run>/runtimes.csv \
  --kuzu results/kuzu/<run>/runtimes.csv \
  --neo4j results/neo4j/<run>/runtimes.csv \
  --output results/combined/<run>/runtime_tables.pdf
```

| Option | Description |
|---|---|
| `--age CSV` | Regular AGE Tree runtime CSV. Required. |
| `--kuzu CSV` | Regular Kuzu Tree runtime CSV. Required. |
| `--neo4j CSV` | Regular Neo4j Tree runtime CSV. Required. |
| `--output PDF` | Output PDF; defaults to a timestamped `results/combined/` path. |
| `--timeout-log-dir CSV=ERRORS_DIR` | Use timeout/error logs from another directory; repeatable. |

Both scripts write `sources.json` provenance next to generated PDFs. Timeout
logs for copied or merged CSVs can be supplied with the repeatable
`--timeout-log-dir CSV=ERRORS_DIR` option.

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

Official implementations of the LDBC SNB interactive queries are available in the LDBC repository https://github.com/ldbc/ldbc_snb_interactive_v2_impls/tree/main. The versions in this paper artifact are adaptations/translations of these queries.

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
