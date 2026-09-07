-- SPDX-License-Identifier: GPL-3.0-only
MATCH (message:Comment {dewey: $messageDewey})
MATCH (root:Comment {dewey: string_split($messageDewey, '.')[1]})
      -[:REPLY_OF]->(post:Post)
      <-[:CONTAINER_OF]-(forum:Forum)-[:HAS_MODERATOR]->(moderator:Person)
RETURN forum.id AS forumId, forum.title AS forumTitle,
       moderator.id AS moderatorId, moderator.firstName AS moderatorFirstName,
       moderator.lastName AS moderatorLastName;
