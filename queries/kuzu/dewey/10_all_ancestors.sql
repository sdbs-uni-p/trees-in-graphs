-- SPDX-License-Identifier: GPL-3.0-only

MATCH (target:$NODE_TYPE {string_id: "$deweyId"})
MATCH (ancestor:$NODE_TYPE)
WHERE ancestor.string_id < target.string_id
AND target.string_id STARTS WITH (ancestor.string_id + '.')
RETURN ancestor
