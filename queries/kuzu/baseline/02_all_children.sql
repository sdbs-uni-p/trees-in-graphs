-- SPDX-License-Identifier: GPL-3.0-only

MATCH (root:$NODE_TYPE {id: $rootID})
MATCH p = (root)<-[:$REL_TYPE]-(child:$NODE_TYPE)
RETURN child;
