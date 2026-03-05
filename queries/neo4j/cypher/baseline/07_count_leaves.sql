MATCH (leaf:$NODE_TYPE)-[:$REL_TYPE*1..]->(root:$NODE_TYPE {id: $rootID})
WHERE NOT (leaf)<-[:$REL_TYPE]-(:$NODE_TYPE)
RETURN count(leaf)