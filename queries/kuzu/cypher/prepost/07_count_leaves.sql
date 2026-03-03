MATCH (root:$NODE_TYPE {integer_id: $prepostRoot})
MATCH (n:$NODE_TYPE)
WHERE n.integer_id > root.integer_id
AND n.integer_id <= root.upper_bound
AND n.upper_bound - n.integer_id = 1
RETURN count(n);