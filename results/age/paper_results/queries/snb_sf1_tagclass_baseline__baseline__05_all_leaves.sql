BEGIN;

SET LOCAL max_parallel_workers_per_gather = 0;

SELECT *
FROM cypher(:'graphname', $$
  MATCH (root:Tagclass {__id__: 1})
  MATCH (root)<-[:tagclass_isSubclassOf_tagclass*1..]-(leaf:Tagclass)
  WHERE NOT EXISTS((leaf)<-[:tagclass_isSubclassOf_tagclass]-(:Tagclass))
  RETURN leaf
$$) AS (leaf agtype);

ROLLBACK;
