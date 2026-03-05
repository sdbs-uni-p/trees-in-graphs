MATCH (root:$NODE_TYPE {integer_id: $prepostRoot})
RETURN ((root.upper_bound - root.integer_id + 1) / 2) - 1;