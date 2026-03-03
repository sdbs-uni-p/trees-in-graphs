MATCH (root:$NODE_TYPE {id: $rootID})
MATCH (n:$NODE_TYPE)
WHERE n.integer_id > root.integer_id
AND n.integer_id <= root.upper_bound
AND n.depth = root.depth + 1
RETURN n;