-- SPDX-License-Identifier: GPL-3.0-only
WITH parent AS MATERIALIZED (
  SELECT id, post
  FROM :"graphname".:"nodetype"
  WHERE properties @> format('{"__id__": %s}', :rootid)::agtype
), shifted AS (
  UPDATE :"graphname".:"nodetype" n
  SET pre = CASE WHEN n.pre >= p.post THEN n.pre + 2 ELSE n.pre END,
      post = CASE WHEN n.post >= p.post THEN n.post + 2 ELSE n.post END
  FROM parent p
  WHERE n.pre >= p.post OR n.post >= p.post
  RETURNING n.id
), new_node AS (
INSERT INTO :"graphname".:"nodetype" (properties, depth, pre, post)
SELECT format('{"id":"%s","type":"%s","__id__":%s}',
              :newid, :'nodetype', :newid)::agtype,
       1, p.post, p.post + 1
FROM parent p
LEFT JOIN (SELECT count(*) FROM shifted) force_shift ON true
RETURNING id
)
INSERT INTO :"graphname".:"reltype" (start_id, end_id)
SELECT n.id, p.id FROM new_node n CROSS JOIN parent p;
