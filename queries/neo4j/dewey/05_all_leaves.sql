-- SPDX-License-Identifier: GPL-3.0-only

MATCH (root:$NODE_TYPE {id: $rootID})
WITH root.dewey AS root_dewey
MATCH (n:$NODE_TYPE)
WHERE n.dewey STARTS WITH (root_dewey + '.')
AND NOT (n)<-[:$REL_TYPE]-(:$NODE_TYPE)
RETURN n;
