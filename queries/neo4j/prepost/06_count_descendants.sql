-- SPDX-License-Identifier: GPL-3.0-only

MATCH (root:$NODE_TYPE {id: $rootID})
MATCH (n:$NODE_TYPE)
WHERE n.pre > root.pre
AND n.pre < root.post
RETURN count(n);
