-- SPDX-License-Identifier: GPL-3.0-only

MATCH (node1:$NODE_TYPE {dewey: "$deweyId1_t"})
MATCH (node2:$NODE_TYPE {dewey: "$deweyId2_t"})
WITH node1.dewey AS n1s, node2.dewey AS n2s
RETURN
(n1s STARTS WITH (n2s + '.')) OR (n2s STARTS WITH (n1s + '.')) AS isAncestorRelationship;
