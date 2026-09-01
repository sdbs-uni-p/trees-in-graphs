-- SPDX-License-Identifier: GPL-3.0-only
MATCH (:Person {id: 19791209302645})<-[:HAS_CREATOR]-(message)
WITH message ORDER BY message.creationDate DESC, message.id ASC LIMIT 10
OPTIONAL MATCH (root:Comment {dewey: split(message.dewey, '.')[0]})
OPTIONAL MATCH (root)-[:REPLY_OF]->(commentPost:Post)
WITH message, CASE WHEN message:Post THEN message ELSE commentPost END AS post
MATCH (post)-[:HAS_CREATOR]->(person:Person)
RETURN message.id AS messageId, coalesce(message.imageFile, message.content) AS messageContent,
       message.creationDate AS messageCreationDate, post.id AS postId,
       person.id AS personId, person.firstName AS personFirstName,
       person.lastName AS personLastName
ORDER BY messageCreationDate DESC, messageId ASC;
