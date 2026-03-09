-- SPDX-License-Identifier: GPL-3.0-only

MATCH (root:$NODE_TYPE {integer_id: $prepostRoot})
RETURN ((root.upper_bound - root.integer_id + 1) / 2) - 1;
