MATCH (node1:$NODE_TYPE {string_id: "$deweyId1_t"})
MATCH (node2:$NODE_TYPE {string_id: "$deweyId2_t"})
WITH node1.string_id AS n1s, node2.string_id AS n2s
MATCH (r:$NODE_TYPE)
WHERE r.depth = 0
AND (n1s = r.string_id OR n1s STARTS WITH (r.string_id + '.'))
AND (n2s = r.string_id OR n2s STARTS WITH (r.string_id + '.'))
RETURN COUNT(r) > 0 AS sameSubtree