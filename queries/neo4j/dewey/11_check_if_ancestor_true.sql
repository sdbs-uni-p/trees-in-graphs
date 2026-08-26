-- SPDX-License-Identifier: GPL-3.0-only

MATCH (n1:$NODE_TYPE {id: $id1_t})
MATCH (n2:$NODE_TYPE {id: $id2_t})
RETURN
(n2.dewey STARTS WITH (n1.dewey + '.')) OR (n1.dewey STARTS WITH (n2.dewey + '.')) AS isAncestorRelationship;
