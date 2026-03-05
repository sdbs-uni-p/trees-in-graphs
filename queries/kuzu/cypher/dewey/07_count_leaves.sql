MATCH (root:$NODE_TYPE {string_id: "$deweyRoot"})
WITH root, root.string_id AS root_string_id
MATCH (n:$NODE_TYPE)
WHERE n.string_id STARTS WITH root_string_id
AND NOT (n)<-[:$REL_TYPE]-(:$NODE_TYPE)
RETURN count(n);