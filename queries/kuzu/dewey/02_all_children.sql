-- SPDX-License-Identifier: GPL-3.0-only

MATCH (root:$NODE_TYPE {dewey: "$deweyRoot"})
WITH root.dewey AS root_dewey, root.depth AS root_depth
MATCH (n:$NODE_TYPE)
WHERE n.dewey STARTS WITH (root_dewey + '.')
AND n.depth = root_depth + 1
RETURN n;
