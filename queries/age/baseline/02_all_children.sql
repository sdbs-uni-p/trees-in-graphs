SELECT *
FROM cypher(:'graphname', $$
  MATCH (root:$NODE_TYPE {__id__: $rootID})
  MATCH (root)<-[:$REL_TYPE]-(child:$NODE_TYPE)
  RETURN child
$$) AS (node agtype);
