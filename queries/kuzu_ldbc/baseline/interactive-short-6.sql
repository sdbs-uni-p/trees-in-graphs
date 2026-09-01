-- SPDX-License-Identifier: GPL-3.0-only
MATCH (message:Comment {id: 1236950581249})-[:REPLY_OF*0..]->(post:Post)
      <-[:CONTAINER_OF]-(forum:Forum)-[:HAS_MODERATOR]->(moderator:Person)
RETURN forum.id AS forumId, forum.title AS forumTitle,
       moderator.id AS moderatorId, moderator.firstName AS moderatorFirstName,
       moderator.lastName AS moderatorLastName;
