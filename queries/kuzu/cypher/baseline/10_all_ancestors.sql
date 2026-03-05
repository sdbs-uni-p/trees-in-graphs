MATCH (node:$NODE_TYPE {id: $nodeID})
MATCH (node)-[:$REL_TYPE*1..]->(ancestor:$NODE_TYPE)
RETURN ancestor