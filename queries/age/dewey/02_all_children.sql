WITH root AS (
  SELECT dewey, depth
  FROM :"graphname".:"nodetype"
  WHERE properties @> format('{"__id__": %s}', :rootid)::agtype
)
SELECT (c.properties ->> '"__id__"'::agtype)::bigint AS id, c.dewey, c.depth
FROM :"graphname".:"nodetype" c, root r
WHERE c.dewey LIKE r.dewey || '.%'
  AND c.depth = r.depth + 1;
