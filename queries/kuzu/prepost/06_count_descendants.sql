-- SPDX-License-Identifier: GPL-3.0-only

MATCH (root:$NODE_TYPE {pre: $prepostRoot})
RETURN ((root.post - root.pre + 1) / 2) - 1;
