MATCH (n1:$NODE_TYPE {id: $id1_t})
MATCH (n2:$NODE_TYPE {id: $id2_t})
RETURN split(n1.string_id, '.')[0] = split(n2.string_id, '.')[0] AS same_subtree