BEGIN;

SET LOCAL max_parallel_workers_per_gather = 0;

/*+ SeqScan(node1) SeqScan(node2) Parallel(node1 0 hard) Parallel(node2 0 hard) */
SELECT *
FROM cypher(:'graphname', $$
  MATCH (node1:TreeNode {__id__: 1})
  MATCH (node2:TreeNode {__id__: 2})
  RETURN EXISTS((node1)-[:TreeEdge*1..]->(node2))
  OR EXISTS((node2)-[:TreeEdge*1..]->(node1))
$$) AS (isAncestorRelationship agtype);

ROLLBACK;
