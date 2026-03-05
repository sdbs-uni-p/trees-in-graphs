MATCH (root:$NODE_TYPE {integer_id: $prepostRoot})
MATCH (n:$NODE_TYPE)
WHERE n.integer_id > $prepostRoot
AND n.integer_id <= root.upper_bound
AND n.depth = root.depth + 1
RETURN n;