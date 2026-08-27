-- SPDX-License-Identifier: GPL-3.0-only
WITH parent AS MATERIALIZED (
  SELECT id, dewey
  FROM :"graphname".:"nodetype"
  WHERE properties @> format('{"__id__": %s}', :rootid)::agtype
), next_child AS (
  SELECT p.id AS parent_id, p.dewey AS parent_dewey,
         COALESCE(MAX(split_part(n.dewey, '.', 2)::bigint), 0) + 1 AS child_no
  FROM parent p
  LEFT JOIN :"graphname".:"nodetype" n
    ON n.depth = 1 AND split_part(n.dewey, '.', 1) = p.dewey
  GROUP BY p.id, p.dewey
), new_node AS (
INSERT INTO :"graphname".:"nodetype" (properties, depth, dewey)
SELECT format('{"id":"%s","type":"%s","__id__":%s}',
              :newid, :'nodetype', :newid)::agtype,
       1, parent_dewey || '.' || child_no
FROM next_child
RETURNING id
)
INSERT INTO :"graphname".:"reltype" (start_id, end_id)
SELECT n.id, c.parent_id FROM new_node n CROSS JOIN next_child c;
