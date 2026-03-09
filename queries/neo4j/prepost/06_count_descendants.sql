-- SPDX-License-Identifier: GPL-3.0-only

MATCH (root:$NODE_TYPE {id: $rootID})
WITH root.upper_bound AS upperBound, root.integer_id AS prepostRoot
MATCH (n:$NODE_TYPE)
WHERE n.integer_id > prepostRoot
AND n.integer_id <= upperBound
RETURN count(n);
