WITH node1 AS (
  SELECT pre, post
  FROM :"graphname".:"nodetype"
  WHERE properties @> format('{"__id__": %s}', :id1)::agtype
),
node2 AS (
  SELECT pre, post
  FROM :"graphname".:"nodetype"
  WHERE properties @> format('{"__id__": %s}', :id2)::agtype
)
SELECT n1.pre < n2.pre AND n1.post > n2.post
    OR n2.pre < n1.pre AND n2.post > n1.post
FROM node1 n1, node2 n2;
