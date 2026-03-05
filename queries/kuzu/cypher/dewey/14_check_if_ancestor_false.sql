MATCH (node1:$NODE_TYPE {string_id: "$deweyId1_f"})
MATCH (node2:$NODE_TYPE {string_id: "$deweyId2_f"})
WITH node1.string_id AS n1s, node2.string_id AS n2s
RETURN
(n1s STARTS WITH (n2s + '.')) OR (n2s STARTS WITH (n1s + '.')) AS isAncestorRelationship;