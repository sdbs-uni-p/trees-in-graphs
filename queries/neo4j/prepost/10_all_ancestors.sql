-- SPDX-License-Identifier: GPL-3.0-only

MATCH (node:$NODE_TYPE {id: $nodeID})
MATCH (ancestor:$NODE_TYPE)
WHERE ancestor.pre < node.pre
AND node.pre < ancestor.post
RETURN ancestor
