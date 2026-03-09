-- SPDX-License-Identifier: GPL-3.0-only

WITH root AS (
  SELECT dewey
  FROM :"graphname".:"nodetype"
  WHERE properties @> format('{"__id__": %s}', :rootid)::agtype
)
SELECT (c.properties ->> '"__id__"'::agtype)::bigint AS id, c.dewey, c.depth
FROM :"graphname".:"nodetype" c, root r
WHERE c.dewey LIKE r.dewey || '.%';
