-- SPDX-License-Identifier: GPL-3.0-only

MATCH (n1:$NODE_TYPE {id: $id1_f})
MATCH (n2:$NODE_TYPE {id: $id2_f})
RETURN split(n1.string_id, '.')[0] = split(n2.string_id, '.')[0] AS same_subtree
