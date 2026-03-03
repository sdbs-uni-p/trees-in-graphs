WITH node1 AS (
  SELECT dewey
  FROM :"graphname".:"nodetype"
  WHERE properties @> format('{"__id__": %s}', :id1)::agtype
),
node2 AS (
  SELECT dewey
  FROM :"graphname".:"nodetype"
  WHERE properties @> format('{"__id__": %s}', :id2)::agtype
)
SELECT n1.dewey LIKE n2.dewey || '.%'
    OR n2.dewey LIKE n1.dewey || '.%'
FROM node1 n1, node2 n2;
