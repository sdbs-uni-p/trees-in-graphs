-- SPDX-License-Identifier: GPL-3.0-only

SELECT *
FROM cypher('snb_sf1_baseline', $$
  MATCH (:Person {id: '19791209302645'})<-[:HAS_CREATOR]-(message)
  WITH message
  ORDER BY message.creationDate DESC, message.id ASC
  LIMIT 10
  MATCH
    (message)-[:REPLY_OF*0..]->(post:Post),
    (post)-[:HAS_CREATOR]->(person)
  RETURN
    message.id AS messageId,
    coalesce(message.imageFile,message.content) AS messageContent,
    message.creationDate AS messageCreationDate,
    post.id AS postId,
    person.id AS personId,
    person.firstName AS personFirstName,
    person.lastName AS personLastName
  ORDER BY message.creationDate DESC, message.id ASC
$$) AS (
  messageId agtype,
  messageContent agtype,
  messageCreationDate agtype,
  postId agtype,
  personId agtype,
  personFirstName agtype,
  personLastName agtype
);
