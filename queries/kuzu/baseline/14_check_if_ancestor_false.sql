-- SPDX-License-Identifier: GPL-3.0-only

MATCH (node1:$NODE_TYPE {id: $id1_f})
MATCH (node2:$NODE_TYPE {id: $id2_f})
RETURN
EXISTS { MATCH (node1)-[:$REL_TYPE*1..]->(node2) }
OR
EXISTS { MATCH (node2)-[:$REL_TYPE*1..]->(node1) }
AS isAncestorRelationship
