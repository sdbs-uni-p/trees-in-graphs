SELECT *
FROM cypher(:'graphname', $$
  MATCH (root:$NODE_TYPE {__id__: $rootID})
  MATCH (root)<-[:$REL_TYPE*1..]-(leaf:$NODE_TYPE)
  WHERE NOT EXISTS((leaf)<-[:$REL_TYPE]-(:$NODE_TYPE))
  RETURN leaf
$$) AS (leaf agtype);
