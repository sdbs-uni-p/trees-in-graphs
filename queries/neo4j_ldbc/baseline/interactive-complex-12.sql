-- SPDX-License-Identifier: GPL-3.0-only
MATCH (tag:Tag)-[:HAS_TYPE]->(:TagClass)-[:IS_SUBCLASS_OF*0..]->(base:TagClass)
WHERE tag.name = 'Monarch' OR base.name = 'Monarch'
WITH collect(tag.id) AS tags
MATCH (:Person {id: 933})-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(:Post)-[:HAS_TAG]->(tag:Tag)
WHERE tag.id IN tags
RETURN friend.id AS personId, friend.firstName AS personFirstName,
       friend.lastName AS personLastName, collect(DISTINCT tag.name) AS tagNames,
       count(DISTINCT comment) AS replyCount
ORDER BY replyCount DESC, personId ASC LIMIT 20;
