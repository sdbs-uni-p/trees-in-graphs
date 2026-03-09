BEGIN;

SET LOCAL max_parallel_workers_per_gather = 0;

SELECT *
FROM cypher(:'graphname', $$
  MATCH (root:TreeNode {__id__: 1})
  MATCH (root)<-[:TreeEdge*1..]-(d:TreeNode)
  RETURN d
$$) AS (node agtype);

ROLLBACK;
