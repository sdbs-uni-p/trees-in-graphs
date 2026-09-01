-- SPDX-License-Identifier: GPL-3.0-only
MATCH (message:Comment {pre: $messagePre})
MATCH (root:Comment {pre: $rootPre})
OPTIONAL MATCH (root)-[:REPLY_OF]->(post:Post)
OPTIONAL MATCH (post)<-[:CONTAINER_OF]-(forum:Forum)
OPTIONAL MATCH (forum)-[:HAS_MODERATOR]->(moderator:Person)
RETURN forum.id AS forumId, forum.title AS forumTitle,
       moderator.id AS moderatorId, moderator.firstName AS moderatorFirstName,
       moderator.lastName AS moderatorLastName;
