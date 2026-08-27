#!/usr/bin/env bash
# SPDX-License-Identifier: GPL-3.0-only

set -euo pipefail

# AGE's file loaders and the graph-cloning step insert explicit graphids. They
# do not advance the per-label sequences used by later Cypher CREATE or SQL
# INSERT operations. Synchronize every user label after all loading/cloning is
# complete so the next generated local id is greater than every imported id.

echo "[25-sequences] synchronizing AGE label sequences"

psql -v ON_ERROR_STOP=1 \
  --echo-errors \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" <<'SQL'
DO $$
DECLARE
  item record;
  sequence_name text;
  maximum_local_id bigint;
  labels_synchronized bigint := 0;
BEGIN
  FOR item IN
    SELECT g.name AS graph_name, l.name AS label_name
    FROM ag_catalog.ag_graph g
    JOIN ag_catalog.ag_label l ON l.graph = g.graphid
    WHERE l.name NOT LIKE '\_ag\_%' ESCAPE '\'
      AND l.kind IN ('v', 'e')
    ORDER BY g.name, l.kind, l.name
  LOOP
    sequence_name := pg_get_serial_sequence(
      format('%I.%I', item.graph_name, item.label_name),
      'id'
    );
    IF sequence_name IS NULL THEN
      RAISE EXCEPTION 'No id sequence for %.%', item.graph_name, item.label_name;
    END IF;

    EXECUTE format(
      'SELECT max((id::text::numeric %% 281474976710656)::bigint) FROM ONLY %I.%I',
      item.graph_name,
      item.label_name
    ) INTO maximum_local_id;

    IF maximum_local_id IS NULL THEN
      PERFORM pg_catalog.setval(sequence_name::regclass, 1, false);
    ELSE
      PERFORM pg_catalog.setval(sequence_name::regclass, maximum_local_id, true);
    END IF;
    labels_synchronized := labels_synchronized + 1;
  END LOOP;

  RAISE NOTICE 'synchronized % AGE label sequences', labels_synchronized;
END $$;
SQL

echo "[25-sequences] finished"
