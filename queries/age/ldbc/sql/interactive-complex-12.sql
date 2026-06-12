-- SPDX-License-Identifier: GPL-3.0-only

-- Source: queries/age/ldbc/cypher/interactive-complex-12.cypher
SELECT *
FROM cypher('snb_sf1_comment_baseline', $$
  MATCH (tag:Tag)-[:tag_hasType_tagclass]->(tagClass:Tagclass)
  MATCH (tagClass)-[:tagclass_isSubclassOf_tagclass*0..]->(baseTagClass:Tagclass)
  WHERE tag.name = 'Monarch' OR baseTagClass.name = 'Monarch'
  WITH collect(tag.__id__) as tags
  MATCH (:Person {__id__: 933 })-[:person_knows_person]-(friend:Person)<-[:comment_hasCreator_person]-(comment:Comment)-[:comment_replyOf_post]->(:Post)-[:post_hasTag_tag]->(tag:Tag)
  WHERE tag.__id__ in tags
  WITH
    friend,
    collect(DISTINCT tag.name) AS tagNames,
    count(DISTINCT comment) AS replyCount
  RETURN
    friend.__id__ AS personId,
    friend.firstName AS personFirstName,
    friend.lastName AS personLastName,
    tagNames,
    replyCount
  ORDER BY
    replyCount DESC,
    toInteger(friend.__id__) ASC
  LIMIT 20
$$) AS (
  personId agtype,
  personFirstName agtype,
  personLastName agtype,
  tagNames agtype,
  replyCount agtype
);

