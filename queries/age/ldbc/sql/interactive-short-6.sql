-- SPDX-License-Identifier: GPL-3.0-only

-- Source: queries/age/ldbc/cypher/interactive-short-6.cypher
-- Params: graphname, messageId
SELECT *
FROM cypher('snb_sf1_comment_baseline', $$
  MATCH (m {__id__: 1236950581249 })
  OPTIONAL MATCH (m)-[:comment_replyOf_comment*0..]->(c2:Comment)-[:comment_replyOf_post]->(postC:Post)
  WITH m, postC, m AS postP
  WITH coalesce(postC, postP) AS post
  WHERE post IS NOT NULL
  MATCH (f:Forum)-[:forum_containerOf_post]->(postMatch:Post)
  WHERE postMatch = post
  MATCH (f)-[:forum_hasModerator_person]->(mod:Person)
  RETURN
      f.__id__ AS forumId,
      f.title AS forumTitle,
      mod.__id__ AS moderatorId,
      mod.firstName AS moderatorFirstName,
      mod.lastName AS moderatorLastName
$$) AS (
  forumId agtype,
  forumTitle agtype,
  moderatorId agtype,
  moderatorFirstName agtype,
  moderatorLastName agtype
);

