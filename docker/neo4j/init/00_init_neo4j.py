#!/usr/bin/env python3
"""
Initialize Neo4j databases for the tree benchmark.

Creates one Neo4j database per graph variant (e.g. truebase.10.plain),
mirroring the Apache AGE and Kuzu setups. Each database contains nodes
and relationships loaded from the same CSV files used by the other backends.

Since the prepared CSVs contain extra columns (type, start_vertex_type, etc.)
that don't map to Neo4j's schema, this script preprocesses CSVs into
the shared /import volume before loading.
"""

import csv
import os
import time

from neo4j import GraphDatabase

DATA_DIR = "/project/data/prepared"
IMPORT_DIR = "/import"

NEO4J_URI = f"bolt://{os.getenv('NEO4J_HOST', 'neo4j_treebench_db')}:{os.getenv('NEO4J_PORT', '7687')}"
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "treebenchPW")

BATCH_SIZE = 10000

# Node properties per annotation type (column_name, neo4j_cast_fn)
NODE_PROPS = {
    "plain": [
        ("id", "toInteger"),
    ],
    "dewey": [
        ("id", "toInteger"),
        ("string_id", None),  # already a string, no cast needed
        ("height", "toInteger"),
        ("depth", "toInteger"),
    ],
    "prepost": [
        ("id", "toInteger"),
        ("integer_id", "toInteger"),
        ("upper_bound", "toInteger"),
        ("height", "toInteger"),
        ("depth", "toInteger"),
    ],
}

# CSV columns to extract from the prepared node CSVs (skip 'type')
NODE_CSV_COLUMNS = {
    "plain": ["id"],
    "dewey": ["id", "string_id", "height", "depth"],
    "prepost": ["id", "integer_id", "upper_bound", "height", "depth"],
}

# All datasets to load
ARTIFICIAL_TREE_TYPES = ["truebase", "ultratall", "ultrawide"]
ARTIFICIAL_TREE_SIZES = [10, 100, 1000, 10000, 100000]
ANNOTATION_TYPES = ["plain", "dewey", "prepost"]


def tree_nodes_filename(annotation: str) -> str:
    """Return the tree node CSV filename for a given annotation."""
    return "TreeNode.csv" if annotation == "plain" else f"TreeNode_{annotation}.csv"

# ─── sf1 (full LDBC SNB) constants ──────────────────────────────────────────

# Labels that carry tree-annotation columns (dewey/prepost)
TREE_ANNOTATED_LABELS = {"Comment", "Place", "Tagclass"}

# Non-tree node types: (column_name, cast_fn) — same across all annotation variants
NON_TREE_NODE_PROPS = {
    "Forum":        [("id","toInteger"),("title",None),("creationDate",None)],
    "Organisation": [("id","toInteger"),("type",None),("name",None),("url",None)],
    "Person":       [("id","toInteger"),("firstName",None),("lastName",None),("gender",None),
                     ("birthday",None),("creationDate",None),("locationIP",None),("browserUsed",None)],
    "Post":         [("id","toInteger"),("imageFile",None),("creationDate",None),("locationIP",None),
                     ("browserUsed",None),("language",None),("content",None),("length","toInteger")],
    "Tag":          [("id","toInteger"),("name",None),("url",None)],
}

NON_TREE_NODE_CSV_COLUMNS = {
    "Forum":        ["id","title","creationDate"],
    "Organisation": ["id","type","name","url"],
    "Person":       ["id","firstName","lastName","gender","birthday","creationDate","locationIP","browserUsed"],
    "Post":         ["id","imageFile","creationDate","locationIP","browserUsed","language","content","length"],
    "Tag":          ["id","name","url"],
}

NON_TREE_NODE_FILES = {
    "Forum":        "forum_0_0.csv",
    "Organisation": "organisation_0_0.csv",
    "Person":       "person_0_0.csv",
    "Post":         "post_0_0.csv",
    "Tag":          "tag_0_0.csv",
}

# Tree-annotated node props for sf1, keyed by (label, annotation)
S_ALL_TREE_NODE_PROPS = {
    ("Comment","plain"):   [("id","toInteger"),("creationDate",None),("locationIP",None),
                             ("browserUsed",None),("content",None),("length","toInteger")],
    ("Comment","dewey"):   [("id","toInteger"),("creationDate",None),("locationIP",None),
                             ("browserUsed",None),("content",None),("length","toInteger"),
                             ("height","toInteger"),("depth","toInteger"),("string_id",None)],
    ("Comment","prepost"): [("id","toInteger"),("creationDate",None),("locationIP",None),
                             ("browserUsed",None),("content",None),("length","toInteger"),
                             ("height","toInteger"),("depth","toInteger"),
                             ("integer_id","toInteger"),("upper_bound","toInteger")],
    ("Place","plain"):     [("id","toInteger"),("name",None),("url",None),("type",None)],
    ("Place","dewey"):     [("id","toInteger"),("name",None),("url",None),("type",None),
                             ("height","toInteger"),("depth","toInteger"),("string_id",None)],
    ("Place","prepost"):   [("id","toInteger"),("name",None),("url",None),("type",None),
                             ("height","toInteger"),("depth","toInteger"),
                             ("integer_id","toInteger"),("upper_bound","toInteger")],
    ("Tagclass","plain"):  [("id","toInteger"),("name",None),("url",None)],
    ("Tagclass","dewey"):  [("id","toInteger"),("name",None),("url",None),
                             ("height","toInteger"),("depth","toInteger"),("string_id",None)],
    ("Tagclass","prepost"):[("id","toInteger"),("name",None),("url",None),
                             ("height","toInteger"),("depth","toInteger"),
                             ("integer_id","toInteger"),("upper_bound","toInteger")],
}

S_ALL_TREE_NODE_CSV_COLUMNS = {
    ("Comment","plain"):   ["id","creationDate","locationIP","browserUsed","content","length"],
    ("Comment","dewey"):   ["id","creationDate","locationIP","browserUsed","content","length",
                            "height","depth","string_id"],
    ("Comment","prepost"): ["id","creationDate","locationIP","browserUsed","content","length",
                            "height","depth","integer_id","upper_bound"],
    ("Place","plain"):     ["id","name","url","type"],
    ("Place","dewey"):     ["id","name","url","type","height","depth","string_id"],
    ("Place","prepost"):   ["id","name","url","type","height","depth","integer_id","upper_bound"],
    ("Tagclass","plain"):  ["id","name","url"],
    ("Tagclass","dewey"):  ["id","name","url","height","depth","string_id"],
    ("Tagclass","prepost"):["id","name","url","height","depth","integer_id","upper_bound"],
}

S_ALL_TREE_NODE_FILES = {
    ("Comment","plain"):   "comment_0_0.csv",
    ("Comment","dewey"):   "comment_0_0_dewey.csv",
    ("Comment","prepost"): "comment_0_0_prepost.csv",
    ("Place","plain"):     "place_0_0.csv",
    ("Place","dewey"):     "place_0_0_dewey.csv",
    ("Place","prepost"):   "place_0_0_prepost.csv",
    ("Tagclass","plain"):  "tagclass_0_0.csv",
    ("Tagclass","dewey"):  "tagclass_0_0_dewey.csv",
    ("Tagclass","prepost"):"tagclass_0_0_prepost.csv",
}

# All 23 edge types: (rel_label, from_label, to_label, csv_filename)
S_ALL_EDGES = [
    ("comment_hasCreator_person_0_0",       "Comment",      "Person",       "comment_hasCreator_person_0_0.csv"),
    ("comment_hasTag_tag_0_0",              "Comment",      "Tag",          "comment_hasTag_tag_0_0.csv"),
    ("comment_isLocatedIn_place_0_0",       "Comment",      "Place",        "comment_isLocatedIn_place_0_0.csv"),
    ("comment_replyOf_comment_0_0",         "Comment",      "Comment",      "comment_replyOf_comment_0_0.csv"),
    ("comment_replyOf_post_0_0",            "Comment",      "Post",         "comment_replyOf_post_0_0.csv"),
    ("forum_containerOf_post_0_0",          "Forum",        "Post",         "forum_containerOf_post_0_0.csv"),
    ("forum_hasMember_person_0_0",          "Forum",        "Person",       "forum_hasMember_person_0_0.csv"),
    ("forum_hasModerator_person_0_0",       "Forum",        "Person",       "forum_hasModerator_person_0_0.csv"),
    ("forum_hasTag_tag_0_0",                "Forum",        "Tag",          "forum_hasTag_tag_0_0.csv"),
    ("organisation_isLocatedIn_place_0_0",  "Organisation", "Place",        "organisation_isLocatedIn_place_0_0.csv"),
    ("person_hasInterest_tag_0_0",          "Person",       "Tag",          "person_hasInterest_tag_0_0.csv"),
    ("person_isLocatedIn_place_0_0",        "Person",       "Place",        "person_isLocatedIn_place_0_0.csv"),
    ("person_knows_person_0_0",             "Person",       "Person",       "person_knows_person_0_0.csv"),
    ("person_likes_comment_0_0",            "Person",       "Comment",      "person_likes_comment_0_0.csv"),
    ("person_likes_post_0_0",               "Person",       "Post",         "person_likes_post_0_0.csv"),
    ("person_studyAt_organisation_0_0",     "Person",       "Organisation", "person_studyAt_organisation_0_0.csv"),
    ("person_workAt_organisation_0_0",      "Person",       "Organisation", "person_workAt_organisation_0_0.csv"),
    ("place_isPartOf_place_0_0",            "Place",        "Place",        "place_isPartOf_place_0_0.csv"),
    ("post_hasCreator_person_0_0",          "Post",         "Person",       "post_hasCreator_person_0_0.csv"),
    ("post_hasTag_tag_0_0",                 "Post",         "Tag",          "post_hasTag_tag_0_0.csv"),
    ("post_isLocatedIn_place_0_0",          "Post",         "Place",        "post_isLocatedIn_place_0_0.csv"),
    ("tag_hasType_tagclass_0_0",            "Tag",          "Tagclass",     "tag_hasType_tagclass_0_0.csv"),
    ("tagclass_isSubclassOf_tagclass_0_0",  "Tagclass",     "Tagclass",     "tagclass_isSubclassOf_tagclass_0_0.csv"),
]


def to_neo4j_db_name(graph_name: str) -> str:
    """Convert graph name to valid Neo4j database name (underscores -> dots)."""
    return graph_name.replace("_", ".")


def build_dataset_list():
    """Build the full list of datasets to create."""
    datasets = []

    # Artificial trees: truebase, ultratall, ultrawide
    for tree_type in ARTIFICIAL_TREE_TYPES:
        for size in ARTIFICIAL_TREE_SIZES:
            for annotation in ANNOTATION_TYPES:
                graph_name = f"{tree_type}_{size}_{annotation}"
                node_csv = os.path.join(
                    DATA_DIR,
                    "artificial_trees",
                    tree_type,
                    str(size),
                    "nodes",
                    tree_nodes_filename(annotation),
                )
                edge_csv = os.path.join(
                    DATA_DIR,
                    "artificial_trees",
                    tree_type,
                    str(size),
                    "edges",
                    "TreeEdge.csv",
                )
                datasets.append({
                    "graph_name": graph_name,
                    "node_label": "TreeNode",
                    "edge_label": "HAS_CHILD",
                    "node_csv": node_csv,
                    "edge_csv": edge_csv,
                    "annotation": annotation,
                })

    # Artificial forest
    for forest_size in [40, 1000]:
        for annotation in ANNOTATION_TYPES:
            graph_name = f"artificial_forest_{forest_size}_{annotation}"
            node_csv = os.path.join(
                DATA_DIR,
                "artificial_forests",
                f"{forest_size}",
                "nodes",
                tree_nodes_filename(annotation),
            )
            edge_csv = os.path.join(
                DATA_DIR,
                "artificial_forests",
                f"{forest_size}",
                "edges",
                "TreeEdge.csv",
            )
            datasets.append({
                "graph_name": graph_name,
                "node_label": "TreeNode",
                "edge_label": "HAS_CHILD",
                "node_csv": node_csv,
                "edge_csv": edge_csv,
                "annotation": annotation,
            })

    # SNB s1
    for annotation in ANNOTATION_TYPES:
        graph_name = f"s1_{annotation}"
        node_file = "comment_0_0.csv" if annotation == "plain" else f"comment_0_0_{annotation}.csv"
        node_csv = os.path.join(DATA_DIR, "snb", "sf1", "nodes", node_file)
        edge_csv = os.path.join(DATA_DIR, "snb", "sf1", "edges", "comment_replyOf_comment_0_0.csv")
        datasets.append({
            "graph_name": graph_name,
            "node_label": "Comment",
            "edge_label": "comment_replyOf_comment_0_0",
            "node_csv": node_csv,
            "edge_csv": edge_csv,
            "annotation": annotation,
        })

    # SNB s2 — Place nodes
    for annotation in ANNOTATION_TYPES:
        graph_name = f"s2_{annotation}"
        node_file = "place_0_0.csv" if annotation == "plain" else f"place_0_0_{annotation}.csv"
        node_csv = os.path.join(DATA_DIR, "snb", "sf1", "nodes", node_file)
        edge_csv = os.path.join(DATA_DIR, "snb", "sf1", "edges", "place_isPartOf_place_0_0.csv")
        datasets.append({
            "graph_name": graph_name,
            "node_label": "Place",
            "edge_label": "place_isPartOf_place_0_0",
            "node_csv": node_csv,
            "edge_csv": edge_csv,
            "annotation": annotation,
        })

    # SNB s3 — Tagclass nodes
    for annotation in ANNOTATION_TYPES:
        graph_name = f"s3_{annotation}"
        node_file = "tagclass_0_0.csv" if annotation == "plain" else f"tagclass_0_0_{annotation}.csv"
        node_csv = os.path.join(DATA_DIR, "snb", "sf1", "nodes", node_file)
        edge_csv = os.path.join(DATA_DIR, "snb", "sf1", "edges", "tagclass_isSubclassOf_tagclass_0_0.csv")
        datasets.append({
            "graph_name": graph_name,
            "node_label": "Tagclass",
            "edge_label": "tagclass_isSubclassOf_tagclass_0_0",
            "node_csv": node_csv,
            "edge_csv": edge_csv,
            "annotation": annotation,
        })

    return datasets


def preprocess_node_csv(src_path, dst_path, annotation):
    """Extract only the columns Neo4j needs from a prepared node CSV."""
    columns = NODE_CSV_COLUMNS[annotation]
    with open(src_path, "r", newline="") as fin, \
         open(dst_path, "w", newline="") as fout:
        reader = csv.DictReader(fin)
        writer = csv.DictWriter(fout, fieldnames=columns)
        writer.writeheader()
        for row in reader:
            writer.writerow({col: row[col] for col in columns})


def preprocess_edge_csv(src_path, dst_path):
    """Extract start_id and end_id from a prepared edge CSV."""
    with open(src_path, "r", newline="") as fin, \
         open(dst_path, "w", newline="") as fout:
        reader = csv.DictReader(fin)
        writer = csv.DictWriter(fout, fieldnames=["start_id", "end_id"])
        writer.writeheader()
        for row in reader:
            writer.writerow({
                "start_id": row["start_id"],
                "end_id": row["end_id"],
            })


def build_create_node_query(node_label, annotation):
    """Build the LOAD CSV + CREATE query for nodes."""
    props = NODE_PROPS[annotation]
    prop_assignments = []
    for col, cast_fn in props:
        if cast_fn:
            prop_assignments.append(f"{col}: {cast_fn}(row.{col})")
        else:
            prop_assignments.append(f"{col}: row.{col}")
    props_str = ", ".join(prop_assignments)

    return (
        f"LOAD CSV WITH HEADERS FROM $csv_uri AS row "
        f"CALL {{ "
        f"WITH row "
        f"CREATE (n:{node_label} {{{props_str}}}) "
        f"}} IN TRANSACTIONS OF {BATCH_SIZE} ROWS"
    )


def build_create_edge_query(node_label, edge_label):
    """Build the LOAD CSV + MATCH/CREATE query for edges."""
    return (
        f"LOAD CSV WITH HEADERS FROM $csv_uri AS row "
        f"CALL {{ "
        f"WITH row "
        f"MATCH (child:{node_label} {{id: toInteger(row.start_id)}}) "
        f"MATCH (parent:{node_label} {{id: toInteger(row.end_id)}}) "
        f"CREATE (child)-[:{edge_label}]->(parent) "
        f"}} IN TRANSACTIONS OF {BATCH_SIZE} ROWS"
    )


def wait_for_database(driver, db_name, timeout=120):
    """Wait until a database is online and ready."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            with driver.session(database=db_name) as session:
                session.run("RETURN 1").consume()
            return True
        except Exception:
            time.sleep(1)
    raise TimeoutError(f"Database {db_name} did not come online within {timeout}s")


def create_neo4j_database(driver, dataset):
    """Create and populate a single Neo4j database."""
    graph_name = dataset["graph_name"]
    node_label = dataset["node_label"]
    edge_label = dataset["edge_label"]
    annotation = dataset["annotation"]
    node_csv = dataset["node_csv"]
    edge_csv = dataset["edge_csv"]

    db_name = to_neo4j_db_name(graph_name)

    # Skip if source CSVs don't exist
    if not os.path.isfile(node_csv):
        print(f"  SKIP {graph_name}: node CSV not found ({node_csv})")
        return False
    if not os.path.isfile(edge_csv):
        print(f"  SKIP {graph_name}: edge CSV not found ({edge_csv})")
        return False

    # Preprocess CSVs into the shared import volume
    import_node_csv = os.path.join(IMPORT_DIR, f"{graph_name}_nodes.csv")
    import_edge_csv = os.path.join(IMPORT_DIR, f"{graph_name}_edges.csv")

    print(f"  Preprocessing node CSV...")
    preprocess_node_csv(node_csv, import_node_csv, annotation)

    print(f"  Preprocessing edge CSV...")
    preprocess_edge_csv(edge_csv, import_edge_csv)

    # Create database
    print(f"  Creating database `{db_name}`...")
    with driver.session(database="system") as session:
        session.run(f"CREATE DATABASE `{db_name}` IF NOT EXISTS")

    wait_for_database(driver, db_name)

    # Skip if the database already contains nodes
    with driver.session(database=db_name) as session:
        result = session.run(f"MATCH (n:{node_label}) RETURN count(n) AS cnt")
        if result.single()["cnt"] > 0:
            print(f"  SKIP {graph_name}: database already populated")
            return True

    with driver.session(database=db_name) as session:
        # Create uniqueness constraint on id (needed for edge loading performance)
        print(f"  Creating constraint on {node_label}.id...")
        session.run(
            f"CREATE CONSTRAINT IF NOT EXISTS "
            f"FOR (n:{node_label}) REQUIRE n.id IS UNIQUE"
        )

        # Load nodes
        node_csv_uri = f"file:///{graph_name}_nodes.csv"
        node_query = build_create_node_query(node_label, annotation)
        print(f"  Loading nodes...")
        session.run(node_query, csv_uri=node_csv_uri)

        # Load edges
        edge_csv_uri = f"file:///{graph_name}_edges.csv"
        edge_query = build_create_edge_query(node_label, edge_label)
        print(f"  Loading edges...")
        session.run(edge_query, csv_uri=edge_csv_uri)

        # Create additional indexes for annotated properties
        if annotation == "dewey":
            print(f"  Creating index on {node_label}.string_id...")
            session.run(
                f"CREATE INDEX IF NOT EXISTS "
                f"FOR (n:{node_label}) ON (n.string_id)"
            )
        elif annotation == "prepost":
            print(f"  Creating index on {node_label}.integer_id...")
            session.run(
                f"CREATE INDEX IF NOT EXISTS "
                f"FOR (n:{node_label}) ON (n.integer_id)"
            )

    # Clean up preprocessed CSVs
    os.remove(import_node_csv)
    os.remove(import_edge_csv)

    print(f"  OK: {graph_name} -> `{db_name}`")
    return True


def _build_node_query(node_label, props):
    """Build a LOAD CSV CREATE query for a node label given a props list."""
    prop_assignments = []
    for col, cast_fn in props:
        if cast_fn:
            prop_assignments.append(f"{col}: {cast_fn}(row.{col})")
        else:
            prop_assignments.append(f"{col}: row.{col}")
    props_str = ", ".join(prop_assignments)
    return (
        f"LOAD CSV WITH HEADERS FROM $csv_uri AS row "
        f"CALL {{ "
        f"WITH row "
        f"CREATE (n:{node_label} {{{props_str}}}) "
        f"}} IN TRANSACTIONS OF {BATCH_SIZE} ROWS"
    )


def create_neo4j_s_all_database(driver, annotation):
    """Create and populate the s_all_{annotation} Neo4j database."""
    graph_name = f"s_all_{annotation}"
    db_name = to_neo4j_db_name(graph_name)
    nodes_dir = os.path.join(DATA_DIR, "snb", "sf1", "nodes")
    edges_dir = os.path.join(DATA_DIR, "snb", "sf1", "edges")

    print(f"  Creating database `{db_name}`...")
    with driver.session(database="system") as session:
        session.run(f"CREATE DATABASE `{db_name}` IF NOT EXISTS")
    wait_for_database(driver, db_name)

    # Skip if already populated (same guard as create_neo4j_database)
    with driver.session(database=db_name) as session:
        result = session.run("MATCH (n:Forum) RETURN count(n) AS cnt")
        if result.single()["cnt"] > 0:
            print(f"  SKIP {graph_name}: database already populated")
            return True

    loaded_labels = set()

    with driver.session(database=db_name) as session:
        # Load non-tree node types
        for label, props in NON_TREE_NODE_PROPS.items():
            node_csv_src = os.path.join(nodes_dir, NON_TREE_NODE_FILES[label])
            if not os.path.isfile(node_csv_src):
                print(f"  SKIP {label}: CSV not found")
                continue

            columns = NON_TREE_NODE_CSV_COLUMNS[label]
            import_path = os.path.join(IMPORT_DIR, f"{graph_name}_{label}_nodes.csv")
            with open(node_csv_src, "r", newline="") as fin, \
                 open(import_path, "w", newline="") as fout:
                reader = csv.DictReader(fin)
                writer = csv.DictWriter(fout, fieldnames=columns)
                writer.writeheader()
                for row in reader:
                    writer.writerow({col: row[col] for col in columns})

            session.run(
                f"CREATE CONSTRAINT IF NOT EXISTS "
                f"FOR (n:{label}) REQUIRE n.id IS UNIQUE"
            )
            node_query = _build_node_query(label, props)
            session.run(node_query, csv_uri=f"file:///{graph_name}_{label}_nodes.csv")
            os.remove(import_path)
            loaded_labels.add(label)
            print(f"    Loaded {label}")

        # Load tree-annotated node types
        for label in ["Comment", "Place", "Tagclass"]:
            node_csv_src = os.path.join(
                nodes_dir, S_ALL_TREE_NODE_FILES[(label, annotation)]
            )
            if not os.path.isfile(node_csv_src):
                print(f"  SKIP {label}: CSV not found")
                continue

            columns = S_ALL_TREE_NODE_CSV_COLUMNS[(label, annotation)]
            import_path = os.path.join(IMPORT_DIR, f"{graph_name}_{label}_nodes.csv")
            with open(node_csv_src, "r", newline="") as fin, \
                 open(import_path, "w", newline="") as fout:
                reader = csv.DictReader(fin)
                writer = csv.DictWriter(fout, fieldnames=columns)
                writer.writeheader()
                for row in reader:
                    writer.writerow({col: row[col] for col in columns})

            session.run(
                f"CREATE CONSTRAINT IF NOT EXISTS "
                f"FOR (n:{label}) REQUIRE n.id IS UNIQUE"
            )
            props = S_ALL_TREE_NODE_PROPS[(label, annotation)]
            node_query = _build_node_query(label, props)
            session.run(node_query, csv_uri=f"file:///{graph_name}_{label}_nodes.csv")
            os.remove(import_path)
            loaded_labels.add(label)

            # Create annotation index on tree-annotated labels
            if annotation == "dewey":
                session.run(
                    f"CREATE INDEX IF NOT EXISTS "
                    f"FOR (n:{label}) ON (n.string_id)"
                )
            elif annotation == "prepost":
                session.run(
                    f"CREATE INDEX IF NOT EXISTS "
                    f"FOR (n:{label}) ON (n.integer_id)"
                )
            print(f"    Loaded {label}")

        # Load all 23 edge types
        for edge_label, from_label, to_label, edge_file in S_ALL_EDGES:
            if from_label not in loaded_labels or to_label not in loaded_labels:
                print(f"  SKIP edge {edge_label}: {from_label} or {to_label} not loaded")
                continue

            edge_csv_src = os.path.join(edges_dir, edge_file)
            if not os.path.isfile(edge_csv_src):
                print(f"  SKIP edge {edge_label}: CSV not found")
                continue

            import_path = os.path.join(IMPORT_DIR, f"{graph_name}_{edge_label}.csv")
            preprocess_edge_csv(edge_csv_src, import_path)

            edge_query = (
                f"LOAD CSV WITH HEADERS FROM $csv_uri AS row "
                f"CALL {{ "
                f"WITH row "
                f"MATCH (src:{from_label} {{id: toInteger(row.start_id)}}) "
                f"MATCH (dst:{to_label} {{id: toInteger(row.end_id)}}) "
                f"CREATE (src)-[:{edge_label}]->(dst) "
                f"}} IN TRANSACTIONS OF {BATCH_SIZE} ROWS"
            )
            session.run(edge_query, csv_uri=f"file:///{graph_name}_{edge_label}.csv")
            os.remove(import_path)
            print(f"    Loaded edge {edge_label}")

    print(f"  OK: {graph_name} -> `{db_name}`")
    return True


def main():
    datasets = build_dataset_list()
    print(f"Found {len(datasets)} graph variants to create.\n")

    os.makedirs(IMPORT_DIR, exist_ok=True)

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    # Verify connectivity
    print(f"Connecting to {NEO4J_URI}...")
    driver.verify_connectivity()
    print("Connected.\n")

    try:
        created = 0
        skipped = 0
        for i, dataset in enumerate(datasets, 1):
            print(f"[{i}/{len(datasets)}] {dataset['graph_name']}")
            if create_neo4j_database(driver, dataset):
                created += 1
            else:
                skipped += 1
            print()

        # sf1 multi-type graphs
        for annotation in ANNOTATION_TYPES:
            print(f"[s_all_{annotation}]")
            create_neo4j_s_all_database(driver, annotation)
            print()
    finally:
        driver.close()

    print(f"Done. Created: {created}, Skipped: {skipped}")


if __name__ == "__main__":
    main()
