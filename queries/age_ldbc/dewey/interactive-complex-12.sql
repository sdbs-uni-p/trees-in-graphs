-- SPDX-License-Identifier: GPL-3.0-only

WITH base AS (
    SELECT *
    FROM cypher('snb_sf1_dewey', $$
        MATCH (baseTagClass:TagClass {name: 'Monarch'})
        RETURN baseTagClass
    $$) AS (
        baseTagClass agtype
    )
),

descendants AS (
    SELECT
        tc.id AS tagclass_graph_id

    FROM base b

    JOIN snb_sf1_dewey."TagClass" base_tc
        ON base_tc.id = age_id(b.baseTagClass)::graphid

    JOIN snb_sf1_dewey."TagClass" tc
        ON tc.dewey = base_tc.dewey
        OR tc.dewey LIKE base_tc.dewey || '.%'
),

tag_types AS (
    SELECT *
    FROM cypher('snb_sf1_dewey', $$
        MATCH (tag:Tag)-[:HAS_TYPE]->(tagClass:TagClass)
        RETURN tag, tagClass
    $$) AS (
        tag agtype,
        tagClass agtype
    )
),

tags AS (
    SELECT DISTINCT
        age_id(tt.tag)::graphid AS tag_graph_id

    FROM tag_types tt

    LEFT JOIN descendants d
        ON d.tagclass_graph_id = age_id(tt.tagClass)::graphid

    WHERE
        tt.tag -> '"name"'::agtype = '"Monarch"'::agtype
        OR d.tagclass_graph_id IS NOT NULL
),

rest AS (
    SELECT *
    FROM cypher('snb_sf1_dewey', $$
        MATCH (:Person {id: '933'})
              -[:KNOWS]-(friend:Person)
              <-[:HAS_CREATOR]-(comment:Comment)
              -[:REPLY_OF]->(:Post)
              -[:HAS_TAG]->(tag:Tag)
        RETURN friend, comment, tag
    $$) AS (
        friend agtype,
        comment agtype,
        tag agtype
    )
)

SELECT
    r.friend -> '"id"'::agtype AS personId,
    r.friend -> '"firstName"'::agtype AS personFirstName,
    r.friend -> '"lastName"'::agtype AS personLastName,

    jsonb_agg(
        DISTINCT to_jsonb(r.tag ->> '"name"'::agtype)
    )::text::agtype AS tagNames,

    count(
        DISTINCT age_id(r.comment)::graphid
    )::text::agtype AS replyCount

FROM rest r

JOIN tags t
    ON t.tag_graph_id = age_id(r.tag)::graphid

GROUP BY r.friend

ORDER BY
    count(DISTINCT age_id(r.comment)::graphid) DESC,
    (r.friend ->> '"id"'::agtype)::bigint ASC

LIMIT 20;
