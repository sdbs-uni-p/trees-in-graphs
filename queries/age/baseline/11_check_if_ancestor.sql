-- SPDX-License-Identifier: GPL-3.0-only

SELECT *
FROM cypher(:'graphname', $$
  MATCH (node1:$NODE_TYPE {__id__: $id1})
  MATCH (node2:$NODE_TYPE {__id__: $id2})
  RETURN EXISTS((node1)-[:$REL_TYPE*1..]->(node2))
  OR EXISTS((node2)-[:$REL_TYPE*1..]->(node1))
$$) AS (isAncestorRelationship agtype);
