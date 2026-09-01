-- SPDX-License-Identifier: GPL-3.0-only
MATCH (:Person {id: 19791209302645})<-[:HAS_CREATOR]-(message)
WITH message ORDER BY message.creationDate DESC, message.id ASC LIMIT 10
OPTIONAL MATCH (root:Comment)
WHERE message.pre IS NOT NULL AND root.depth = 0 AND
      root.pre <= message.pre AND root.post >= message.post
OPTIONAL MATCH (root)-[:REPLY_OF]->(commentPost:Post)
OPTIONAL MATCH (message:Post)-[:HAS_CREATOR]->(directPerson:Person)
OPTIONAL MATCH (commentPost)-[:HAS_CREATOR]->(commentPerson:Person)
RETURN message.id AS messageId, coalesce(message.imageFile, message.content) AS messageContent,
       message.creationDate AS messageCreationDate,
       CASE WHEN message.pre IS NULL THEN message.id ELSE commentPost.id END AS postId,
       CASE WHEN message.pre IS NULL THEN directPerson.id ELSE commentPerson.id END AS personId,
       CASE WHEN message.pre IS NULL THEN directPerson.firstName ELSE commentPerson.firstName END AS personFirstName,
       CASE WHEN message.pre IS NULL THEN directPerson.lastName ELSE commentPerson.lastName END AS personLastName
ORDER BY messageCreationDate DESC, messageId ASC;
