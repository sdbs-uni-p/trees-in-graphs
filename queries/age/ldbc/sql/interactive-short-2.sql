-- SPDX-License-Identifier: GPL-3.0-only

-- Source: queries/age/ldbc/cypher/interactive-short-2.cypher
-- Params: graphname, personId
SELECT *
FROM cypher('snb_sf1_comment_baseline', $$
  MATCH (person:Person {__id__: 933})<-[:comment_hasCreator_person]-(message:Comment)
  MATCH (message)-[:comment_replyOf_comment*0..]->(c2:Comment)-[:comment_replyOf_post]->(post:Post)
  MATCH (post)-[:post_hasCreator_person]->(creator:Person)
  RETURN
    message.__id__ AS messageId,
    coalesce(message.imageFile,message.content) AS messageContent,
    message.creationDate AS messageCreationDate,
    post.__id__ AS postId,
    creator.__id__ AS personId,
    creator.firstName AS personFirstName,
    creator.lastName AS personLastName
  ORDER BY message.creationDate DESC, message.__id__ ASC
  LIMIT 10
$$) AS (
  messageId agtype,
  messageContent agtype,
  messageCreationDate agtype,
  postId agtype,
  personId agtype,
  personFirstName agtype,
  personLastName agtype
);

