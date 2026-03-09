-- SPDX-License-Identifier: GPL-3.0-only

MATCH (n1:$NODE_TYPE {id: $id1_f})
MATCH (n2:$NODE_TYPE {id: $id2_f})
RETURN
(n2.string_id STARTS WITH (n1.string_id + '.')) OR (n1.string_id STARTS WITH (n2.string_id + '.')) AS isAncestorRelationship;
