-- SPDX-License-Identifier: GPL-3.0-only

SELECT *
FROM cypher('snb_sf1_baseline', $$
  MATCH (tag:Tag)-[:HAS_TYPE]->(:TagClass)-[IS_SUBCLASS_OF*0..]->(baseTagClass:TagClass)
  WHERE tag.name = 'Monarch' OR baseTagClass.name = 'Monarch'
  WITH collect(tag.id) as tags
  MATCH (:Person {id: '933'})-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(:Post)-[:HAS_TAG]->(tag:Tag)
  WHERE tag.id in tags
  RETURN
    friend.id AS personId,
    friend.firstName AS personFirstName,
    friend.lastName AS personLastName,
    collect(DISTINCT tag.name) AS tagNames,
    count(DISTINCT comment) AS replyCount
  ORDER BY
    count(DISTINCT comment) DESC,
    toInteger(friend.id) ASC
  LIMIT 20
$$) AS (
  personId agtype,
  personFirstName agtype,
  personLastName agtype,
  tagNames agtype,
  replyCount agtype
);
