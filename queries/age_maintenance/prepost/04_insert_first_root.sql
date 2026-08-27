-- SPDX-License-Identifier: GPL-3.0-only
WITH shifted AS (
  UPDATE :"graphname".:"nodetype"
  SET pre = pre + 2, post = post + 2
  RETURNING id
)
INSERT INTO :"graphname".:"nodetype" (properties, depth, pre, post)
SELECT format('{"id":"%s","type":"%s","__id__":%s}',
              :newid, :'nodetype', :newid)::agtype,
       0, 1, 2
FROM (SELECT count(*) FROM shifted) force_shift;
