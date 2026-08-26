-- SPDX-License-Identifier: GPL-3.0-only

MATCH (n1:$NODE_TYPE {id: $id1_f})
MATCH (n2:$NODE_TYPE {id: $id2_f})
RETURN split(n1.dewey, '.')[0] = split(n2.dewey, '.')[0] AS same_subtree
