BEGIN;

SET LOCAL max_parallel_workers_per_gather = 0;

SELECT *
FROM cypher(:'graphname', $$
  MATCH (root:TreeNode {__id__: 1})
  MATCH (root)<-[:TreeEdge*1..]-(leaf:TreeNode)
  WHERE NOT EXISTS((leaf)<-[:TreeEdge]-(:TreeNode))
  RETURN leaf
$$) AS (leaf agtype);

ROLLBACK;
