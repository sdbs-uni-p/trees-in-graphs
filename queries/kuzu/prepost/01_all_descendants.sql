-- SPDX-License-Identifier: GPL-3.0-only

MATCH (root:$NODE_TYPE {pre: $prepostRoot})
MATCH (n:$NODE_TYPE)
WHERE n.pre > root.pre
AND n.pre < root.post
RETURN n;
