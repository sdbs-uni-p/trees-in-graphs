// Q12. Expert search
/*
:params { personId: 10995116278009, tagClassName: "Monarch" }
*/
MATCH (tag:Tag)-[:tag_hasType_tagclass|tagclass_isSubclassOf_tagclass*0..]->(baseTagClass:Tagclass)
WHERE tag.name = $tagClassName OR baseTagClass.name = $tagClassName
WITH collect(tag.id) as tags
MATCH (:Person {id: $personId })-[:person_knows_person]-(friend:Person)<-[:comment_hasCreator_person]-(comment:Comment)-[:comment_replyOf_post]->(:Post)-[:post_hasTag_tag]->(tag:Tag)
WHERE tag.id in tags
RETURN
    friend.id AS personId,
    friend.firstName AS personFirstName,
    friend.lastName AS personLastName,
    collect(DISTINCT tag.name) AS tagNames,
    count(DISTINCT comment) AS replyCount
ORDER BY
    replyCount DESC,
    toInteger(personId) ASC
LIMIT 20
