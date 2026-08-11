-- SPDX-License-Identifier: GPL-3.0-only

-- Chosen parameter is a comment with id '1236950581249'.
-- If parameter is a post, change type of m to Post.
-- This distinction is necessary because Apache AGE does not support polymorphic relationships, such as Post|Comment.
WITH message AS (
    SELECT *
    FROM cypher('snb_sf1_prepost', $$
        MATCH (m:Comment {id: '1236950581249'})
        RETURN m
    $$) AS (
        m agtype
    )
),

post AS (
    SELECT
        r.end_id AS post_graph_id

    FROM message m

    JOIN snb_sf1_prepost."Comment" c
        ON c.id = age_id(m.m)::graphid

    JOIN snb_sf1_prepost."Comment" root
        ON root.depth = 0
       AND root.pre <= c.pre
       AND root.post >= c.post

    JOIN snb_sf1_prepost."REPLY_OF" r
        ON r.start_id = root.id
),

rest AS (
    SELECT *
    FROM cypher('snb_sf1_prepost', $$
        MATCH (p:Post)<-[:CONTAINER_OF]-(f:Forum)
              -[:HAS_MODERATOR]->(mod:Person)
        RETURN p, f, mod
    $$) AS (
        p agtype,
        f agtype,
        mod agtype
    )
)

SELECT
    r.f -> '"id"'::agtype AS forumId,
    r.f -> '"title"'::agtype AS forumTitle,
    r.mod -> '"id"'::agtype AS moderatorId,
    r.mod -> '"firstName"'::agtype AS moderatorFirstName,
    r.mod -> '"lastName"'::agtype AS moderatorLastName

FROM post p
JOIN rest r
    ON age_id(r.p)::graphid = p.post_graph_id;
