-- SPDX-License-Identifier: GPL-3.0-only

MATCH (node:$NODE_TYPE {integer_id: $prepostId})
WITH node.integer_id AS targetId
MATCH (ancestor:$NODE_TYPE)
WHERE ancestor.integer_id < targetId
AND ancestor.upper_bound >= targetId
RETURN ancestor
