-- SPDX-License-Identifier: GPL-3.0-only

MATCH (node1:$NODE_TYPE {id: $id1_t})
MATCH (node2:$NODE_TYPE {id: $id2_t})
MATCH (ancestor:$NODE_TYPE)
WHERE ancestor.pre < node1.pre
AND ancestor.pre < node2.pre
AND node1.pre < ancestor.post
AND node2.pre < ancestor.post
RETURN COUNT(ancestor) > 0 AS sameSubtree
