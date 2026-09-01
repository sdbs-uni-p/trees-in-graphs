-- SPDX-License-Identifier: GPL-3.0-only
MATCH (base:TagClass {dewey: $baseDewey}), (tagClass:TagClass)<-[:HAS_TYPE]-(tag:Tag)
WHERE tag.name = 'Monarch' OR tagClass.dewey = base.dewey
      OR tagClass.dewey STARTS WITH base.dewey + '.'
WITH collect(DISTINCT tag.id) AS tags
MATCH (:Person {id: 933})-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(:Post)-[:HAS_TAG]->(tag:Tag)
WHERE tag.id IN tags OR tag.name = 'Monarch'
RETURN friend.id AS personId, friend.firstName AS personFirstName,
       friend.lastName AS personLastName, collect(DISTINCT tag.name) AS tagNames,
       count(DISTINCT comment) AS replyCount
ORDER BY replyCount DESC, personId ASC LIMIT 20;
