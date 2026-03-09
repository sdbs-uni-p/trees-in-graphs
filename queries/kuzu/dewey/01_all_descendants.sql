-- SPDX-License-Identifier: GPL-3.0-only

MATCH (root:$NODE_TYPE {string_id: "$deweyRoot"})
WITH root.string_id AS root_string_id
MATCH (n:$NODE_TYPE)
WHERE n.string_id STARTS WITH (root_string_id + '.')
RETURN n;
