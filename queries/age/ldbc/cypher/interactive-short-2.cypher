// IS2. Recent messages of a person
/*
:params { personId: 10995116277795 }
*/
MATCH (:Person {id: $personId})<-[:post_hasCreator_person|comment_hasCreator_person]-(message)
WITH
 message,
 message.id AS messageId,
 message.creationDate AS messageCreationDate
ORDER BY messageCreationDate DESC, messageId ASC
LIMIT 10
MATCH (message)-[:comment_replyOf_comment|comment_replyOf_post*0..]->(post:Post),
      (post)-[:post_hasCreator_person]->(person)
RETURN
 messageId,
 coalesce(message.imageFile,message.content) AS messageContent,
 messageCreationDate,
 post.id AS postId,
 person.id AS personId,
 person.firstName AS personFirstName,
 person.lastName AS personLastName
ORDER BY messageCreationDate DESC, messageId ASC
