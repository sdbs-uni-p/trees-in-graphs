-- SPDX-License-Identifier: GPL-3.0-only
INSERT INTO :"graphname".:"nodetype" (properties, depth, dewey)
SELECT format('{"id":"%s","type":"%s","__id__":%s}',
              :newid, :'nodetype', :newid)::agtype,
       0, (MAX(dewey::bigint) + 1)::text
FROM :"graphname".:"nodetype"
WHERE depth = 0;
