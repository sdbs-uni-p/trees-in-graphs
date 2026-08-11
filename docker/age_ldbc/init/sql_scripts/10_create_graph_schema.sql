-- SPDX-License-Identifier: GPL-3.0-only

-- Create a graph schema without loading any data.
-- Parameters (psql variables):
--   graph_name: Optional. Graph schema name (overrides derived name)
--   graph_path: Optional. Used only to derive graph_name if graph_name is empty

\if :{?graph_path}
\else
\set graph_path ''
\endif
\if :{?graph_name}
\else
\set graph_name ''
\endif

CREATE TEMP TABLE IF NOT EXISTS _graph_schema_vars (
  graph_path text,
  graph_name text
);

TRUNCATE _graph_schema_vars;
INSERT INTO _graph_schema_vars (graph_path, graph_name)
VALUES (NULLIF(:'graph_path', ''), NULLIF(:'graph_name', ''));

DO $$
DECLARE
  graph_path text;
  graph_name text;
  relpath text;
BEGIN
  SELECT v.graph_path, v.graph_name
  INTO graph_path, graph_name
  FROM _graph_schema_vars v
  LIMIT 1;

  IF graph_name IS NULL OR graph_name = '' THEN
    IF graph_path IS NULL OR graph_path = '' THEN
      RAISE EXCEPTION 'graph_name or graph_path is required';
    END IF;

    relpath := regexp_replace(graph_path, '^/data/prepared/?', '');
    graph_name := replace(relpath, '/', '_');

    IF graph_name IS NULL OR graph_name = '' THEN
      graph_name := replace(regexp_replace(graph_path, '^/+', ''), '/', '_');
    END IF;
  END IF;

  IF NOT EXISTS (SELECT 1 FROM ag_catalog.ag_graph WHERE name = graph_name) THEN
    EXECUTE format('SELECT ag_catalog.create_graph(%L)', graph_name);
  END IF;

  EXECUTE format(
    'ALTER TABLE %I._ag_label_vertex SET (autovacuum_enabled = off, toast.autovacuum_enabled = off)',
    graph_name
  );
  EXECUTE format(
    'ALTER TABLE %I._ag_label_edge SET (autovacuum_enabled = off, toast.autovacuum_enabled = off)',
    graph_name
  );
END $$;
