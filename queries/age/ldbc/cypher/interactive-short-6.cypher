// IS6. Forum of a message
/*
:params { messageId: 206158431836 }
*/
MATCH (m {id: $messageId })-[:comment_replyOf_comment|comment_replyOf_post*0..]->(p:Post)<-[:forum_containerOf_post]-(f:Forum)-[:forum_hasModerator_person]->(mod:Person)
RETURN
    f.id AS forumId,
    f.title AS forumTitle,
    mod.id AS moderatorId,
    mod.firstName AS moderatorFirstName,
    mod.lastName AS moderatorLastName
