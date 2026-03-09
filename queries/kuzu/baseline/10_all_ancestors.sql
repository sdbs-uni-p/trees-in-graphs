-- SPDX-License-Identifier: GPL-3.0-only

MATCH (node:$NODE_TYPE {id: $nodeID})
MATCH (node)-[:$REL_TYPE*1..]->(ancestor:$NODE_TYPE)
RETURN ancestor
