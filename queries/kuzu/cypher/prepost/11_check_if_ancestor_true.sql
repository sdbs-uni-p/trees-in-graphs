MATCH (node1:$NODE_TYPE {integer_id: $prepostId1_t})
MATCH (node2:$NODE_TYPE {integer_id: $prepostId2_t})
RETURN
(node2.integer_id > node1.integer_id AND
node2.integer_id <= node1.upper_bound)
OR
(node1.integer_id > node2.integer_id AND
node1.integer_id <= node2.upper_bound)
AS isAncestorRelationship