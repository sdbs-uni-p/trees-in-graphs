-- SPDX-License-Identifier: GPL-3.0-only
WITH first_root AS MATERIALIZED (
  SELECT id, dewey
  FROM :"graphname".:"nodetype"
  WHERE properties @> format('{"__id__": %s}', :rootid)::agtype
), shifted AS (
  UPDATE :"graphname".:"nodetype" n
  SET dewey = r.dewey || '.' || (split_part(n.dewey, '.', 2)::bigint + 1) ||
              substring(n.dewey FROM '^[^.]+\.[^.]+(.*)$')
  FROM first_root r
  WHERE n.depth > 0
    AND n.dewey LIKE r.dewey || '.%'
  RETURNING n.id
), new_node AS (
INSERT INTO :"graphname".:"nodetype" (properties, depth, dewey)
SELECT format('{"id":"%s","type":"%s","__id__":%s}',
              :newid, :'nodetype', :newid)::agtype,
       1, r.dewey || '.1'
FROM first_root r
LEFT JOIN (SELECT count(*) FROM shifted) force_shift ON true
RETURNING id
)
INSERT INTO :"graphname".:"reltype" (start_id, end_id)
SELECT n.id, r.id FROM new_node n CROSS JOIN first_root r;
