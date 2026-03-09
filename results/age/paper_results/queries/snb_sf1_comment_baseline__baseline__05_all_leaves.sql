BEGIN;

SET LOCAL max_parallel_workers_per_gather = 0;

SELECT *
FROM cypher(:'graphname', $$
  MATCH (root:Comment {__id__: 1374390095024})
  MATCH (root)<-[:comment_replyOf_comment*1..]-(leaf:Comment)
  WHERE NOT EXISTS((leaf)<-[:comment_replyOf_comment]-(:Comment))
  RETURN leaf
$$) AS (leaf agtype);

ROLLBACK;
