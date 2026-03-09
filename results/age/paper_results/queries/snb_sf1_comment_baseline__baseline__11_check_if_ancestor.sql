BEGIN;

SET LOCAL max_parallel_workers_per_gather = 0;

/*+ SeqScan(node1) SeqScan(node2) Parallel(node1 0 hard) Parallel(node2 0 hard) */
SELECT *
FROM cypher(:'graphname', $$
  MATCH (node1:Comment {__id__: 549757114012})
  MATCH (node2:Comment {__id__: 549757114029})
  RETURN EXISTS((node1)-[:comment_replyOf_comment*1..]->(node2))
  OR EXISTS((node2)-[:comment_replyOf_comment*1..]->(node1))
$$) AS (isAncestorRelationship agtype);

ROLLBACK;
