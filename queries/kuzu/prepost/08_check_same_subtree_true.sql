-- SPDX-License-Identifier: GPL-3.0-only

MATCH (node1:$NODE_TYPE {integer_id: $prepostId1_t})
MATCH (node2:$NODE_TYPE {integer_id: $prepostId2_t})
WITH node1.integer_id AS n1i, node2.integer_id AS n2i
MATCH (ancestor:$NODE_TYPE)
WHERE ancestor.integer_id < n1i
AND ancestor.integer_id < n2i
AND ancestor.upper_bound >= n1i
AND ancestor.upper_bound >= n2i
WITH ancestor LIMIT 1
RETURN ancestor IS NOT NULL
AS sameSubtree
