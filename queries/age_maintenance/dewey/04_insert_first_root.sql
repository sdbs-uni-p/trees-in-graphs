-- SPDX-License-Identifier: GPL-3.0-only
WITH shifted AS (
  UPDATE :"graphname".:"nodetype"
  SET dewey = (split_part(dewey, '.', 1)::bigint + 1) ||
              substring(dewey FROM '^[^.]+(.*)$')
  RETURNING id
)
INSERT INTO :"graphname".:"nodetype" (properties, depth, dewey)
SELECT format('{"id":"%s","type":"%s","__id__":%s}',
              :newid, :'nodetype', :newid)::agtype,
       0, '1'
FROM (SELECT count(*) FROM shifted) force_shift;
