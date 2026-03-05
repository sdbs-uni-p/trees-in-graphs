MATCH (target:$NODE_TYPE {id: $nodeID})
MATCH (ancestor:$NODE_TYPE)
WHERE ancestor.string_id < target.string_id
AND target.string_id STARTS WITH ancestor.string_id + '.'
RETURN ancestor