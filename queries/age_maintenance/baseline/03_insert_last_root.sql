-- SPDX-License-Identifier: GPL-3.0-only

SELECT *
FROM cypher(:'graphname', $$
  CREATE (node:$NODE_TYPE {id: '$NEW_ID', type: '$NODE_TYPE', __id__: $NEW_ID})
  RETURN node
$$) AS (node agtype);
