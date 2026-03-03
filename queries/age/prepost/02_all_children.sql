WITH root AS (
  SELECT pre, post, depth
  FROM :"graphname".:"nodetype"
  WHERE properties @> format('{"__id__": %s}', :rootid)::agtype
)
SELECT (c.properties ->> '"__id__"'::agtype)::bigint AS id, c.pre, c.post, c.depth
FROM :"graphname".:"nodetype" c, root r
WHERE c.pre  >  r.pre
  AND c.post < r.post
  AND c.depth = r.depth + 1;
