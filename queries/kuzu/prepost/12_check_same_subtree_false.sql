-- SPDX-License-Identifier: GPL-3.0-only

MATCH (node1:$NODE_TYPE {pre: $prepostId1_f})
MATCH (node2:$NODE_TYPE {pre: $prepostId2_f})
MATCH (ancestor:$NODE_TYPE)
WHERE ancestor.pre < node1.pre
AND ancestor.pre < node2.pre
AND node1.pre < ancestor.post
AND node2.pre < ancestor.post
WITH ancestor LIMIT 1
RETURN ancestor IS NOT NULL
AS sameSubtree
