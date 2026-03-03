-- Generic baseline/Dewey/Prepost index builder for Apache AGE
-- Parameters (psql variables):
--   graph_name: Required. Graph schema name.
--   node_table: Required. Node table name (label table) within the graph schema.
--   edge_table: Required. Edge table name (label table) within the graph schema.
--   index_kind: Required. 'baseline', 'dewey' or 'prepost'.
--
-- Assumption: The graph is a forest (no cycles). If cycles exist, recursion would loop.
--             We assume a forest for now; a pre-check can be added later.

\if :{?graph_name}
\else
\set graph_name ''
\endif
\if :{?index_kind}
\else
\set index_kind ''
\endif
\if :{?node_table}
\else
\set node_table ''
\endif
\if :{?edge_table}
\else
\set edge_table ''
\endif
CREATE TEMP TABLE IF NOT EXISTS _tree_index_vars (
  graph_name text,
  node_table text,
  edge_table text,
  index_kind text
);

TRUNCATE _tree_index_vars;
INSERT INTO _tree_index_vars (graph_name, node_table, edge_table, index_kind)
VALUES (
  NULLIF(:'graph_name', ''),
  NULLIF(:'node_table', ''),
  NULLIF(:'edge_table', ''),
  NULLIF(:'index_kind', '')
);

DO $$
DECLARE
  gname text;
  node_tbl text;
  edge_tbl text;
  kind text;
  comment_rel regclass;
  con_dewey text;
  con_pre text;
  con_post text;
  idx_edge_start text;
  idx_edge_end text;
  idx_depth_dewey text;
  idx_depth_dewey_pattern text;
  idx_depth_pre text;
BEGIN
  SELECT v.graph_name, v.node_table, v.edge_table, v.index_kind
  INTO gname, node_tbl, edge_tbl, kind
  FROM _tree_index_vars v
  LIMIT 1;

  IF gname IS NULL OR gname = '' THEN
    RAISE EXCEPTION 'graph_name is required';
  END IF;

  IF node_tbl IS NULL OR node_tbl = '' THEN
    RAISE EXCEPTION 'node_table is required';
  END IF;

  IF edge_tbl IS NULL OR edge_tbl = '' THEN
    RAISE EXCEPTION 'edge_table is required';
  END IF;

  kind := lower(coalesce(kind, ''));
  IF kind NOT IN ('baseline', 'dewey', 'prepost') THEN
    RAISE EXCEPTION 'index_kind must be baseline, dewey or prepost (got: %)', kind;
  END IF;

  IF NOT EXISTS (SELECT 1 FROM ag_catalog.ag_graph WHERE name = gname) THEN
    RAISE NOTICE 'graph % not found, skipping', gname;
    RETURN;
  END IF;

  comment_rel := format('%I.%I', gname, node_tbl)::regclass;
  con_dewey := format('%s_dewey_unique', node_tbl);
  con_pre := format('%s_pre_unique', node_tbl);
  con_post := format('%s_post_unique', node_tbl);
  idx_edge_start := format('%s_start_idx', edge_tbl);
  idx_edge_end := format('%s_end_idx', edge_tbl);
  idx_depth_dewey := format('%s_depth_dewey_idx', node_tbl);
  idx_depth_dewey_pattern := format('%s_depth_dewey_pattern_idx', node_tbl);
  idx_depth_pre := format('%s_depth_pre_idx', node_tbl);

  IF kind = 'baseline' THEN
    -- EXECUTE format(
    --   'ALTER TABLE %I.%I SET (autovacuum_enabled = off, toast.autovacuum_enabled = off)',
    --   gname, edge_tbl
    -- );
    -- EXECUTE format(
    --   'CREATE INDEX IF NOT EXISTS %I ON %I.%I (start_id)',
    --   idx_edge_start, gname, edge_tbl
    -- );
    -- EXECUTE format(
    --   'CREATE INDEX IF NOT EXISTS %I ON %I.%I (end_id)',
    --   idx_edge_end, gname, edge_tbl
    -- );
    -- EXECUTE format('ANALYZE %I.%I', gname, edge_tbl);
    RETURN;
  END IF;

  EXECUTE format(
    'ALTER TABLE %I.%I SET (autovacuum_enabled = off, toast.autovacuum_enabled = off)',
    gname, node_tbl
  );

  EXECUTE format('ALTER TABLE %I.%I ADD COLUMN IF NOT EXISTS depth integer', gname, node_tbl);

  IF kind = 'dewey' THEN
    EXECUTE format('ALTER TABLE %I.%I ADD COLUMN IF NOT EXISTS dewey text', gname, node_tbl);

    IF NOT EXISTS (
      SELECT 1
      FROM pg_constraint c
      WHERE c.conname = con_dewey
        AND c.conrelid = comment_rel
    ) THEN
      EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I UNIQUE (dewey)', gname, node_tbl, con_dewey);
    END IF;

    EXECUTE format($sql$
      WITH RECURSIVE
      root_order AS (
        SELECT c.id AS node_id,
               ROW_NUMBER() OVER (ORDER BY c.id) AS ord
         FROM %I.%I c
         LEFT JOIN %I.%I e
               ON e.start_id = c.id
        WHERE e.start_id IS NULL
      ),
      child_order AS (
         SELECT e.end_id AS parent_id,
           e.start_id AS child_id,
           ROW_NUMBER() OVER (PARTITION BY e.end_id ORDER BY e.start_id) AS ord
         FROM %I.%I e
      ),
      dewey AS (
        SELECT r.node_id AS id,
           r.ord::text AS dewey,
           0::integer AS depth
        FROM root_order r

        UNION ALL

        SELECT co.child_id AS id,
           d.dewey || '.' || co.ord::text AS dewey,
           d.depth + 1 AS depth
        FROM dewey d
        JOIN child_order co
          ON co.parent_id = d.id
      )
      UPDATE %I.%I c
            SET dewey = d.dewey,
           depth = d.depth
      FROM dewey d
      WHERE d.id = c.id;
    $sql$, gname, node_tbl, gname, edge_tbl, gname, edge_tbl, gname, node_tbl);

    EXECUTE format(
      'CREATE INDEX IF NOT EXISTS %I ON %I.%I (depth, dewey)',
      idx_depth_dewey, gname, node_tbl
    );
    EXECUTE format(
      'CREATE INDEX IF NOT EXISTS %I ON %I.%I (depth, dewey text_pattern_ops)',
      idx_depth_dewey_pattern, gname, node_tbl
    );
    EXECUTE format('ANALYZE %I.%I', gname, node_tbl);
  ELSE
    EXECUTE format('ALTER TABLE %I.%I ADD COLUMN IF NOT EXISTS pre integer', gname, node_tbl);
    EXECUTE format('ALTER TABLE %I.%I ADD COLUMN IF NOT EXISTS post integer', gname, node_tbl);

    IF NOT EXISTS (
      SELECT 1
      FROM pg_constraint c
      WHERE c.conname = con_pre
        AND c.conrelid = comment_rel
    ) THEN
      EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I UNIQUE (pre)', gname, node_tbl, con_pre);
    END IF;

    IF NOT EXISTS (
      SELECT 1
      FROM pg_constraint c
      WHERE c.conname = con_post
        AND c.conrelid = comment_rel
    ) THEN
      EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I UNIQUE (post)', gname, node_tbl, con_post);
    END IF;

    EXECUTE format($sql$
      WITH RECURSIVE
      root_order AS (
        SELECT c.id AS node_id,
               ROW_NUMBER() OVER (ORDER BY c.id)::bigint AS ord
         FROM %I.%I c
         LEFT JOIN %I.%I e
               ON e.start_id = c.id
        WHERE e.start_id IS NULL
      ),
      child_order AS (
         SELECT e.end_id AS parent_id,
           e.start_id AS child_id,
           ROW_NUMBER() OVER (PARTITION BY e.end_id ORDER BY e.start_id)::bigint AS ord
         FROM %I.%I e
      ),
      walk AS (
        SELECT r.node_id AS id,
           ARRAY[r.ord]::bigint[] AS path,
           0::integer AS depth
        FROM root_order r

        UNION ALL

        SELECT co.child_id AS id,
           w.path || co.ord,
           w.depth + 1 AS depth
        FROM walk w
        JOIN child_order co
          ON co.parent_id = w.id
      ),
      events AS (
        SELECT id, path AS ord_path, 0 AS is_exit
        FROM walk
        UNION ALL
        SELECT id, path || ARRAY[9223372036854775807::bigint] AS ord_path, 1 AS is_exit
        FROM walk
      ),
      numbered AS (
        SELECT id,
               is_exit,
               ROW_NUMBER() OVER (ORDER BY ord_path) AS idx
        FROM events
      ),
      prepost AS (
        SELECT id,
               MIN(depth) AS depth,
               MIN(idx) FILTER (WHERE is_exit = 0) AS pre,
               MIN(idx) FILTER (WHERE is_exit = 1) AS post
        FROM numbered
        JOIN walk USING (id)
        GROUP BY id
      )
      UPDATE %I.%I c
      SET pre = p.pre,
          post = p.post,
          depth = p.depth
      FROM prepost p
      WHERE p.id = c.id;
    $sql$, gname, node_tbl, gname, edge_tbl, gname, edge_tbl, gname, node_tbl);

    EXECUTE format(
      'CREATE INDEX IF NOT EXISTS %I ON %I.%I (depth, pre)',
      idx_depth_pre, gname, node_tbl
    );
    EXECUTE format('ANALYZE %I.%I', gname, node_tbl);
  END IF;
END $$;
