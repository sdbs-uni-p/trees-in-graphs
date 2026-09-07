-- SPDX-License-Identifier: GPL-3.0-only
MATCH (message:Comment {pre: $messagePre})
MATCH (root:Comment)
WHERE root.depth = 0
  AND root.pre <= $messagePre
  AND root.post >= $messagePre
WITH root
MATCH (root)-[:REPLY_OF]->(post:Post)
      <-[:CONTAINER_OF]-(forum:Forum)-[:HAS_MODERATOR]->(moderator:Person)
RETURN forum.id AS forumId, forum.title AS forumTitle,
       moderator.id AS moderatorId, moderator.firstName AS moderatorFirstName,
       moderator.lastName AS moderatorLastName;
