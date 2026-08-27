# AGE Maintenance Flow

This file describes the isolated AGE maintenance environment and the state
cleanup performed around its measurements.

## Setup and initialization

The maintenance container reuses the AGE image and init scripts from
`docker/age`, but has its own PostgreSQL volume:

- Container: `age_treebench_maintenance`
- Volume: `age_treebench_maintenance_data`
- Init scripts: `docker/age/init/`
- Prepared data: mounted read-only from `data/`

The separate volume is important: maintenance statements can modify tables
inside their transactions, while the regular AGE benchmark database remains
untouched. Every measured maintenance statement is rolled back, so the
container provides a repeatable copy of the prepared AGE data.

The init chain is:

1. `docker/age/entrypoint-resumable-init.sh`
2. `docker/age/run-all-init.sh`
3. `docker/age/init/00_init-age.sh`
4. `docker/age/init/10_create_graphs.sh`
5. `docker/age/init/20_load_data.sh`
6. `docker/age/init/25_sync_label_sequences.sh`
7. `docker/age/init/30_add_tree_indexes.sh`
8. `docker/age/init/99_init_complete.sh`

The maintenance compose file mounts `docker/age/init/` as `/opt/age-init`, so
it intentionally shares the regular AGE initialization logic. The resulting
`.init_complete` marker and PostgreSQL readiness determine container health.

## Label sequence synchronization

AGE's CSV loader and graph-cloning step preserve explicit local IDs but do not
advance the PostgreSQL sequence associated with each vertex or edge label. The
`25_sync_label_sequences.sh` step therefore sets every user-label sequence to
the maximum imported local ID after loading and cloning. This ensures that a
later maintenance `CREATE` or `INSERT` receives a new ID instead of colliding
with an imported row. Empty labels retain their initial next value of 1.

## Measurement cleanup

Each measurement runs in a transaction and is rolled back. A rollback restores
the logical data, but PostgreSQL may retain dead row versions created by the
write. Without cleanup, repeated maintenance runs would progressively change
table and index bloat and could make later runtimes incomparable.

By default, the runner therefore performs regular `VACUUM` after each run on
the changed node table and, for the child-insertion queries, the changed edge
table. This cleanup happens outside `runtime_ms`. Regular `VACUUM` marks dead
space reusable and, with `ANALYZE FALSE`, does not refresh planner statistics
for every run.

`--no-vacuum-between-runs` disables this cleanup and is intended only when that
state change is deliberate. `VACUUM` is not the same as `VACUUM FULL`: regular
`VACUUM` reuses space in place, while `VACUUM FULL` rewrites a table, requires
an exclusive lock, and needs additional temporary disk space.

The optional `--compact-before` runs `VACUUM FULL ANALYZE` once on every
selected tree node and edge table before measurements. `ANALYZE` refreshes
PostgreSQL's planner statistics after the table rewrite. This option is useful
for a container that was already bloated by an older runner version, but it is
outside the measured runtime and can take a long time for SNB.

The runner disables parallel query workers and parallel maintenance work for
these cleanup operations. This avoids excessive shared-memory use with the
large SNB indexes in the container.

## Measurement semantics

The runner measures four rolled-back insert operations for each selected graph
and encoding:

- insert the last child under the last root
- insert the first child under the first root
- insert the last root
- insert the first root

Root and parent IDs come from `experiments/maintenance_parameters.csv` so that
root discovery and ordering are not part of `runtime_ms`.
