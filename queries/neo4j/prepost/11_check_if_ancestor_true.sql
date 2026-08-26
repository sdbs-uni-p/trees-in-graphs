-- SPDX-License-Identifier: GPL-3.0-only

MATCH (node1:$NODE_TYPE {id: $id1_t})
MATCH (node2:$NODE_TYPE {id: $id2_t})
RETURN
(node2.pre > node1.pre AND
node2.pre < node1.post)
OR
(node1.pre > node2.pre AND
node1.pre < node2.post)
AS isAncestorRelationship
