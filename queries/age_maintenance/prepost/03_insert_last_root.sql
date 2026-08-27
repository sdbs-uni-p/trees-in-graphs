-- SPDX-License-Identifier: GPL-3.0-only
INSERT INTO :"graphname".:"nodetype" (properties, depth, pre, post)
SELECT format('{"id":"%s","type":"%s","__id__":%s}',
              :newid, :'nodetype', :newid)::agtype,
       0, MAX(post) + 1, MAX(post) + 2
FROM :"graphname".:"nodetype";
