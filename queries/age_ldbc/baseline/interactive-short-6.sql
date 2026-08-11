-- SPDX-License-Identifier: GPL-3.0-only

-- Chosen parameter is a comment with id '1236950581249'.
-- If parameter is a post, change type of m to Post.
-- This distinction is necessary because Apache AGE does not support polymorphic relationships, such as Post|Comment.
SELECT *
FROM cypher('snb_sf1_baseline', $$
  MATCH (m:Comment {id: '1236950581249'})-[:REPLY_OF*0..]->(p:Post)
        <-[:CONTAINER_OF]-(f:Forum)
        -[:HAS_MODERATOR]->(mod:Person)
  RETURN
    f.id AS forumId,
    f.title AS forumTitle,
    mod.id AS moderatorId,
    mod.firstName AS moderatorFirstName,
    mod.lastName AS moderatorLastName
$$) AS (
  forumId agtype,
  forumTitle agtype,
  moderatorId agtype,
  moderatorFirstName agtype,
  moderatorLastName agtype
);
