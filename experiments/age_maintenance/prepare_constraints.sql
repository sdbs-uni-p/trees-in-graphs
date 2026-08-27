-- SPDX-License-Identifier: GPL-3.0-only
-- Make dense structural keys updateable in one statement. PostgreSQL checks a
-- deferred unique constraint at transaction end instead of after every row.

DO $$
DECLARE
  item record;
BEGIN
  FOR item IN
    SELECT g.name AS graph_name, l.name AS label_name, c.conname,
           a.attname AS column_name
    FROM ag_catalog.ag_graph g
    JOIN ag_catalog.ag_label l ON l.graph = g.graphid AND l.kind = 'v'
    JOIN pg_namespace ns ON ns.nspname = g.name
    JOIN pg_class tbl ON tbl.relnamespace = ns.oid AND tbl.relname = l.name
    JOIN pg_constraint c ON c.conrelid = tbl.oid AND c.contype = 'u'
    JOIN unnest(c.conkey) AS key(attnum) ON true
    JOIN pg_attribute a ON a.attrelid = tbl.oid AND a.attnum = key.attnum
    WHERE l.name NOT LIKE '\_ag\_%' ESCAPE '\'
      AND a.attname IN ('dewey', 'pre', 'post')
      AND NOT c.condeferrable
  LOOP
    EXECUTE format('ALTER TABLE %I.%I DROP CONSTRAINT %I',
                   item.graph_name, item.label_name, item.conname);
    EXECUTE format(
      'ALTER TABLE %I.%I ADD CONSTRAINT %I UNIQUE (%I) DEFERRABLE INITIALLY IMMEDIATE',
      item.graph_name, item.label_name, item.conname, item.column_name);
  END LOOP;
END $$;

