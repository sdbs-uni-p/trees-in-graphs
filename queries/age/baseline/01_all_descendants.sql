-- SPDX-License-Identifier: GPL-3.0-only

SELECT *
FROM cypher(:'graphname', $$
  MATCH (root:$NODE_TYPE {__id__: $rootID})
  MATCH (root)<-[:$REL_TYPE*1..]-(d:$NODE_TYPE)
  RETURN d
$$) AS (node agtype);
