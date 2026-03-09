-- SPDX-License-Identifier: GPL-3.0-only

MATCH (node1:$NODE_TYPE {id: $id1_t})
MATCH (node2:$NODE_TYPE {id: $id2_t})
MATCH (ancestor:$NODE_TYPE)
WHERE ancestor.integer_id < node1.integer_id
AND ancestor.integer_id < node2.integer_id
AND node1.integer_id <= ancestor.upper_bound
AND node2.integer_id <= ancestor.upper_bound
RETURN COUNT(ancestor) > 0 AS sameSubtree
