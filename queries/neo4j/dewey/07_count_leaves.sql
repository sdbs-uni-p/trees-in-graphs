-- SPDX-License-Identifier: GPL-3.0-only

MATCH (root:$NODE_TYPE {id: $rootID})
WITH root.string_id AS root_string_id
MATCH (n:$NODE_TYPE)
WHERE n.string_id STARTS WITH root_string_id
AND NOT (n)<-[:$REL_TYPE]-(:$NODE_TYPE)
RETURN count(n);
