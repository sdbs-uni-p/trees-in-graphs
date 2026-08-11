-- SPDX-License-Identifier: GPL-3.0-only

WITH top10 AS (
    SELECT *
    FROM cypher('snb_sf1_dewey', $$
        MATCH (:Person {id: '19791209302645'})<-[:HAS_CREATOR]-(message)
        RETURN message
        ORDER BY message.creationDate DESC, message.id ASC
        LIMIT 10
    $$) AS (
        message agtype
    )
),

posts AS (
    SELECT
        t.message,
        CASE
            WHEN age_label(t.message) = '"Post"'::agtype
                THEN age_id(t.message)::graphid
            ELSE r.end_id
        END AS post_graph_id

    FROM top10 t

    LEFT JOIN snb_sf1_dewey."Comment" c
        ON c.id = age_id(t.message)::graphid

    LEFT JOIN snb_sf1_dewey."Comment" root
        ON root.depth = 0
       AND root.dewey = split_part(c.dewey, '.', 1)

    LEFT JOIN snb_sf1_dewey."REPLY_OF" r
        ON r.start_id = root.id
),

rest AS (
    SELECT *
    FROM cypher('snb_sf1_dewey', $$
        MATCH (post:Post)-[:HAS_CREATOR]->(person:Person)
        RETURN post, person
    $$) AS (
        post agtype,
        person agtype
    )
)

SELECT
    p.message -> '"id"'::agtype AS messageId,
    coalesce(
        p.message -> '"imageFile"'::agtype,
        p.message -> '"content"'::agtype
    ) AS messageContent,
    p.message -> '"creationDate"'::agtype AS messageCreationDate,
    r.post -> '"id"'::agtype AS postId,
    r.person -> '"id"'::agtype AS personId,
    r.person -> '"firstName"'::agtype AS personFirstName,
    r.person -> '"lastName"'::agtype AS personLastName

FROM posts p
JOIN rest r
    ON age_id(r.post)::graphid = p.post_graph_id

ORDER BY
    messageCreationDate DESC,
    messageId ASC;
