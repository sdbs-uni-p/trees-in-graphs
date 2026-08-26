-- SPDX-License-Identifier: GPL-3.0-only

MATCH (node1:$NODE_TYPE {dewey: "$deweyId1_f"})
MATCH (node2:$NODE_TYPE {dewey: "$deweyId2_f"})
WITH node1.dewey AS n1s, node2.dewey AS n2s
MATCH (r:$NODE_TYPE)
WHERE r.depth = 0
AND (n1s = r.dewey OR n1s STARTS WITH (r.dewey + '.'))
AND (n2s = r.dewey OR n2s STARTS WITH (r.dewey + '.'))
RETURN COUNT(r) > 0 AS sameSubtree
