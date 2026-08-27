-- SPDX-License-Identifier: GPL-3.0-only

SELECT *
FROM cypher(:'graphname', $$
  MATCH (parent:$NODE_TYPE {__id__: $ROOT_ID})
  CREATE (node:$NODE_TYPE {id: '$NEW_ID', type: '$NODE_TYPE', __id__: $NEW_ID})
         -[:$REL_TYPE]->(parent)
  RETURN node
$$) AS (node agtype);
