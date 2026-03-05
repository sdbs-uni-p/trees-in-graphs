MATCH (descendant:$NODE_TYPE)-[:$REL_TYPE*1..]->(ancestor:$NODE_TYPE)
WHERE ancestor.id = $rootID
RETURN count(descendant);