-- SPDX-License-Identifier: GPL-3.0-only
MATCH (:Person {id: 19791209302645})<-[:HAS_CREATOR]-(message)
WITH message ORDER BY message.creationDate DESC, message.id ASC LIMIT 10
CALL (message) {
  OPTIONAL MATCH (root:Comment)
  WHERE message:Comment AND root.depth = 0 AND root.pre <= message.pre
  WITH root ORDER BY root.pre DESC LIMIT 1
  RETURN root
}
WITH message, root
WHERE root IS NULL OR root.post >= message.post
OPTIONAL MATCH (root)-[:REPLY_OF]->(commentPost:Post)
WITH message, CASE WHEN message:Post THEN message ELSE commentPost END AS post
MATCH (post)-[:HAS_CREATOR]->(person:Person)
RETURN message.id AS messageId, coalesce(message.imageFile, message.content) AS messageContent,
       message.creationDate AS messageCreationDate, post.id AS postId,
       person.id AS personId, person.firstName AS personFirstName,
       person.lastName AS personLastName
ORDER BY messageCreationDate DESC, messageId ASC;
