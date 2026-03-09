-- SPDX-License-Identifier: GPL-3.0-only

-- Load a single node or edge CSV into an existing graph.
-- Parameters (psql variables):
--   graph_name: Required. Graph schema name.
--   label_kind: Required. 'v' for node, 'e' for edge.
--   label_name: Required. Label name (table name).
--   file_path: Required. Absolute CSV file path.

\if :{?graph_name}
\else
\set graph_name ''
\endif
\if :{?label_kind}
\else
\set label_kind ''
\endif
\if :{?label_name}
\else
\set label_name ''
\endif
\if :{?file_path}
\else
\set file_path ''
\endif

CREATE TEMP TABLE IF NOT EXISTS _load_vars (
  graph_name text,
  label_kind text,
  label_name text,
  file_path text
);

TRUNCATE _load_vars;
INSERT INTO _load_vars (graph_name, label_kind, label_name, file_path)
VALUES (
  NULLIF(:'graph_name', ''),
  NULLIF(:'label_kind', ''),
  NULLIF(:'label_name', ''),
  NULLIF(:'file_path', '')
);

DO $$
DECLARE
  gname text;
  kind text;
  lname text;
  fpath text;
BEGIN
  SELECT v.graph_name, v.label_kind, v.label_name, v.file_path
  INTO gname, kind, lname, fpath
  FROM _load_vars v
  LIMIT 1;

  IF gname IS NULL OR gname = '' THEN
    RAISE EXCEPTION 'graph_name is required';
  END IF;
  IF kind IS NULL OR kind = '' THEN
    RAISE EXCEPTION 'label_kind is required';
  END IF;
  IF lname IS NULL OR lname = '' THEN
    RAISE EXCEPTION 'label_name is required';
  END IF;
  IF fpath IS NULL OR fpath = '' THEN
    RAISE EXCEPTION 'file_path is required';
  END IF;

  kind := lower(kind);
  IF kind NOT IN ('v', 'e') THEN
    RAISE EXCEPTION 'label_kind must be v or e (got: %)', kind;
  END IF;

  IF NOT EXISTS (SELECT 1 FROM ag_catalog.ag_graph WHERE name = gname) THEN
    RAISE NOTICE 'graph % not found, skipping', gname;
    RETURN;
  END IF;

  IF kind = 'v' THEN
    IF NOT EXISTS (
      SELECT 1
      FROM ag_catalog.ag_label l
      WHERE l.name = lname
        AND l.graph = (SELECT graphid FROM ag_catalog.ag_graph WHERE name = gname)
    ) THEN
      EXECUTE format('SELECT ag_catalog.create_vlabel(%L, %L)', gname, lname);
    END IF;

    EXECUTE format(
      'ALTER TABLE %I.%I SET (autovacuum_enabled = off, toast.autovacuum_enabled = off)',
      gname, lname
    );

    EXECUTE format(
      'SELECT ag_catalog.load_labels_from_file(%L, %L, %L)',
      gname, lname, fpath
    );
  ELSE
    IF NOT EXISTS (
      SELECT 1
      FROM ag_catalog.ag_label l
      WHERE l.name = lname
        AND l.graph = (SELECT graphid FROM ag_catalog.ag_graph WHERE name = gname)
    ) THEN
      EXECUTE format('SELECT ag_catalog.create_elabel(%L, %L)', gname, lname);
    END IF;

    EXECUTE format(
      'ALTER TABLE %I.%I SET (autovacuum_enabled = off, toast.autovacuum_enabled = off)',
      gname, lname
    );

    EXECUTE format(
      'SELECT ag_catalog.load_edges_from_file(%L, %L, %L)',
      gname, lname, fpath
    );
  END IF;

  EXECUTE format('ANALYZE %I.%I', gname, lname);
END $$;
