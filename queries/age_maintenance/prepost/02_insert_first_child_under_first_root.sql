-- SPDX-License-Identifier: GPL-3.0-only
WITH insertion AS MATERIALIZED (
  SELECT id, pre + 1 AS position
  FROM :"graphname".:"nodetype"
  WHERE properties @> format('{"__id__": %s}', :rootid)::agtype
), shifted AS (
  UPDATE :"graphname".:"nodetype" n
  SET pre = CASE WHEN n.pre >= p.position THEN n.pre + 2 ELSE n.pre END,
      post = CASE WHEN n.post >= p.position THEN n.post + 2 ELSE n.post END
  FROM insertion p
  WHERE n.pre >= p.position OR n.post >= p.position
  RETURNING n.id
), new_node AS (
INSERT INTO :"graphname".:"nodetype" (properties, depth, pre, post)
SELECT format('{"id":"%s","type":"%s","__id__":%s}',
              :newid, :'nodetype', :newid)::agtype,
       1, p.position, p.position + 1
FROM insertion p
LEFT JOIN (SELECT count(*) FROM shifted) force_shift ON true
RETURNING id
)
INSERT INTO :"graphname".:"reltype" (start_id, end_id)
SELECT n.id, p.id FROM new_node n CROSS JOIN insertion p;
