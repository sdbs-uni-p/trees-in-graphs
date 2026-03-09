-- SPDX-License-Identifier: GPL-3.0-only

MATCH (root:$NODE_TYPE {integer_id: $prepostRoot})
WITH root.upper_bound AS upperBound
MATCH (n:$NODE_TYPE)
WHERE n.integer_id > $prepostRoot
AND n.integer_id <= upperBound
RETURN n;
