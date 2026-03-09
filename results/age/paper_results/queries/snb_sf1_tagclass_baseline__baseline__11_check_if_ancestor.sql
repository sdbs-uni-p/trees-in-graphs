BEGIN;

SET LOCAL max_parallel_workers_per_gather = 0;

/*+ SeqScan(node1) SeqScan(node2) Parallel(node1 0 hard) Parallel(node2 0 hard) */
SELECT *
FROM cypher(:'graphname', $$
  MATCH (node1:Tagclass {__id__: 240})
  MATCH (node2:Tagclass {__id__: 47})
  RETURN EXISTS((node1)-[:tagclass_isSubclassOf_tagclass*1..]->(node2))
  OR EXISTS((node2)-[:tagclass_isSubclassOf_tagclass*1..]->(node1))
$$) AS (isAncestorRelationship agtype);

ROLLBACK;
