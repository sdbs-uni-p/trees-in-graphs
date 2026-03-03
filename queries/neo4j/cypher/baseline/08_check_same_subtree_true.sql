MATCH (node1:$NODE_TYPE {id: $id1_t})
MATCH (node2:$NODE_TYPE {id: $id2_t})
MATCH (node1)-[:$REL_TYPE*0..]->(ancestor:$NODE_TYPE)<-[:$REL_TYPE*0..]-(node2)
RETURN COUNT(ancestor) > 0
AS sameSubtree