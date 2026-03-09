-- SPDX-License-Identifier: GPL-3.0-only

MATCH (root:$NODE_TYPE {id: $rootID})
MATCH (root)<-[:$REL_TYPE*1..]-(d:$NODE_TYPE)
RETURN d;
