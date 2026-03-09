-- SPDX-License-Identifier: GPL-3.0-only

MATCH (node:$NODE_TYPE {id: $nodeID})
MATCH (ancestor:$NODE_TYPE)
WHERE ancestor.integer_id < node.integer_id
AND node.integer_id <= ancestor.upper_bound
RETURN ancestor
