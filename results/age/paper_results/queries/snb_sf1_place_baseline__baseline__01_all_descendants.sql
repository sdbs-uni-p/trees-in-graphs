BEGIN;

SET LOCAL max_parallel_workers_per_gather = 0;

SELECT *
FROM cypher(:'graphname', $$
  MATCH (root:Place {__id__: 1455})
  MATCH (root)<-[:place_isPartOf_place*1..]-(d:Place)
  RETURN d
$$) AS (node agtype);

ROLLBACK;
