BEGIN;

SET LOCAL max_parallel_workers_per_gather = 0;

SELECT *
FROM cypher(:'graphname', $$
  MATCH (root:TreeNode {__id__: 2})
  MATCH (root)<-[:TreeEdge]-(child:TreeNode)
  RETURN child
$$) AS (node agtype);

ROLLBACK;
