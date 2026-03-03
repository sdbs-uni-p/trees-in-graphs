WITH root AS (
  SELECT dewey
  FROM :"graphname".:"nodetype"
  WHERE properties @> format('{"__id__": %s}', :rootid)::agtype
), descendants AS (
  SELECT (c.properties ->> '"__id__"'::agtype)::bigint AS id, c.dewey, c.depth
  FROM :"graphname".:"nodetype" c, root r
  WHERE c.dewey LIKE r.dewey || '.%'
)
SELECT *
FROM descendants d
WHERE NOT EXISTS (
  SELECT 1
  FROM descendants e
  WHERE e.dewey LIKE d.dewey || '.%'
);
