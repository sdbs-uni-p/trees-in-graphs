# AGE Init Flow (prepared data)

This file describes how the init chain runs during container startup with data from `data/prepared`.

## Execution order

The entry flow is:

1. `docker/age/entrypoint-resumable-init.sh`
2. `docker/age/run-all-init.sh`
3. `docker/age/init/00_init-age.sh`
4. `docker/age/init/10_create_graphs.sh`
5. `docker/age/init/20_load_data.sh`
6. `docker/age/init/30_add_tree_indexes.sh`
7. `docker/age/init/99_init_complete.sh`

`run-all-init.sh` creates done markers per script in `${PGDATA}/.init_state/*.done`, so initialization is resumable.

## 10_create_graphs.sh

- Creates **schema/graphs without data** via SQL: `sql_scripts/10_create_graph_schema.sql`.
- If `trees.csv` exists in a graph directory and contains declarations, graph names are generated as:
  - `<graph_base>_<tree_name_lowercase>_baseline`
  - `<graph_base>_<tree_name_lowercase>_dewey`
  - `<graph_base>_<tree_name_lowercase>_prepost`
- `tree_name` is mandatory in each `trees.csv` row:
  - `tree_name,node_file.csv,edge_file.csv`
- Without `trees.csv` (or without declarations), fallback is:
  - `<graph_base>_baseline`, `<graph_base>_dewey`, `<graph_base>_prepost`

## 20_load_data.sh

- SQL for loading individual CSV files: `sql_scripts/20_load_data.sql`.
- For each `graph_base`, **one source graph** is selected (one of the existing suffix/tree graphs).
- Nodes/edges are loaded from CSV **once** into this source graph.
- Afterwards, all other graphs of the same `graph_base` are **cloned** via SQL:
  - Create label if needed (`create_vlabel`/`create_elabel`)
  - `TRUNCATE` target label
  - `INSERT INTO dst.label SELECT * FROM src.label`
  - `ANALYZE dst.label`
- Result: CSV I/O once per `graph_base`, then only internal DB copying.

## 30_add_tree_indexes.sh

- SQL for index creation: `sql_scripts/30_add_tree_indexes.sql`.
- By default, processes only `*_dewey` and `*_prepost`.
- For tree-specific graphs (e.g. `snb_sf1_comment_dewey`), `tree_name` is derived from the graph name.
- Then only the **matching tree row** from `trees.csv` is selected.
- For that tree, the respective index is created:
  - `dewey`: columns/constraint + populate + `ANALYZE`
  - `prepost`: columns/constraints + populate + `ANALYZE`

## Important log prefixes in container output

- `[entrypoint]` start/retry of the resumable init chain
- `[init-runner]` script order, skip when done markers exist
- `[10-create]` graph creation, `trees.csv` resolution
- `[20-load]` CSV load into source graph + cloning into target graphs
- `[30-index]` tree matching and index creation per graph
- `[init]` completion marker written

## SNB SF1 example

With `data/prepared/snb/sf1/trees.csv` containing `comment`, `place`, `tagclass`, these graphs are created per suffix:

- `snb_sf1_comment_baseline`, `snb_sf1_place_baseline`, `snb_sf1_tagclass_baseline`
- `snb_sf1_comment_dewey`, `snb_sf1_place_dewey`, `snb_sf1_tagclass_dewey`
- `snb_sf1_comment_prepost`, `snb_sf1_place_prepost`, `snb_sf1_tagclass_prepost`

`20_load_data.sh` loads CSVs once into one of these graphs (per base) and clones data to the others.
`30_add_tree_indexes.sh` indexes only the comment tree, for example, in `snb_sf1_comment_dewey`.
