-- SPDX-License-Identifier: GPL-3.0-only

MATCH (target:$NODE_TYPE {id: $nodeID})
MATCH (ancestor:$NODE_TYPE)
WHERE ancestor.dewey < target.dewey
AND target.dewey STARTS WITH ancestor.dewey + '.'
RETURN ancestor
