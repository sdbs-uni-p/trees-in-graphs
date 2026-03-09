/*+ SeqScan(n1) SeqScan(n2) Parallel(n1 0 hard) Parallel(n2 0 hard) */
WITH node1 AS (
  SELECT dewey
  FROM :"graphname".:"nodetype" n1
  WHERE properties @> format('{"__id__": %s}', 1455)::agtype
),
node2 AS (
  SELECT dewey
  FROM :"graphname".:"nodetype" n2
  WHERE properties @> format('{"__id__": %s}', 548)::agtype
)
SELECT n1.dewey LIKE n2.dewey || '.%'
    OR n2.dewey LIKE n1.dewey || '.%'
FROM node1 n1, node2 n2;
