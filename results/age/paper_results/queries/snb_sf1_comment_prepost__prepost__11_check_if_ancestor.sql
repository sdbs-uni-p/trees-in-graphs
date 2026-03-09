/*+ SeqScan(n1) SeqScan(n2) Parallel(n1 0 hard) Parallel(n2 0 hard) */
WITH node1 AS (
  SELECT pre, post
  FROM :"graphname".:"nodetype" n1
  WHERE properties @> format('{"__id__": %s}', 549757114012)::agtype
),
node2 AS (
  SELECT pre, post
  FROM :"graphname".:"nodetype" n2
  WHERE properties @> format('{"__id__": %s}', 549757114029)::agtype
)
SELECT n1.pre < n2.pre AND n1.post > n2.post
    OR n2.pre < n1.pre AND n2.post > n1.post
FROM node1 n1, node2 n2;
