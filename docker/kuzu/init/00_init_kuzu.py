#!/usr/bin/env python3
"""
Initialize Kuzu databases for the tree benchmark.

Creates one Kuzu database per graph variant (e.g. truebase_10_plain),
mirroring the Apache AGE setup. Each database contains a node table
and a relationship table loaded from the same CSV files used by AGE.

Since AGE CSVs contain extra columns (type, start_vertex_type, etc.)
that don't map to Kuzu's schema, this script preprocesses CSVs into
a temporary directory before loading.
"""

import csv
import os
import shutil
import tempfile

import kuzu

DATA_DIR = "/project/data/prepared"
KUZU_DIR = "/kuzu_data"

# Node table schemas per annotation type (column_name, kuzu_type)
NODE_SCHEMAS = {
    "plain": [
        ("id", "INT64"),
    ],
    "dewey": [
        ("id", "INT64"),
        ("string_id", "STRING"),
        ("height", "INT64"),
        ("depth", "INT64"),
    ],
    "prepost": [
        ("id", "INT64"),
        ("integer_id", "INT64"),
        ("upper_bound", "INT64"),
        ("height", "INT64"),
        ("depth", "INT64"),
    ],
}

# Primary key column per annotation type
PRIMARY_KEYS = {
    "plain": "id",
    "dewey": "string_id",
    "prepost": "integer_id",
}

# CSV columns to extract from AGE node CSVs (skip 'type' and AGE-specific columns)
NODE_CSV_COLUMNS = {
    "plain": ["id"],
    "dewey": ["id", "string_id", "height", "depth"],
    "prepost": ["id", "integer_id", "upper_bound", "height", "depth"],
}

# All datasets to load
ARTIFICIAL_TREE_TYPES = ["truebase", "ultratall", "ultrawide"]
ARTIFICIAL_TREE_SIZES = [10, 100, 1000, 10000, 100000]
ANNOTATION_TYPES = ["plain", "dewey", "prepost"]

# ─── s_all (full LDBC SNB) constants ──────────────────────────────────────────

# Labels that carry tree-annotation columns (dewey/prepost)
TREE_ANNOTATED_LABELS = {"Comment", "Place", "Tagclass"}

# Non-tree node types: same schema across all annotation variants
NON_TREE_NODE_SCHEMAS = {
    "Forum":        [("id","INT64"),("title","STRING"),("creationDate","STRING")],
    "Organisation": [("id","INT64"),("type","STRING"),("name","STRING"),("url","STRING")],
    "Person":       [("id","INT64"),("firstName","STRING"),("lastName","STRING"),("gender","STRING"),
                     ("birthday","STRING"),("creationDate","STRING"),("locationIP","STRING"),("browserUsed","STRING")],
    "Post":         [("id","INT64"),("imageFile","STRING"),("creationDate","STRING"),("locationIP","STRING"),
                     ("browserUsed","STRING"),("language","STRING"),("content","STRING"),("length","INT64")],
    "Tag":          [("id","INT64"),("name","STRING"),("url","STRING")],
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

# Tree-annotated node schemas for s_all, keyed by (label, annotation)
S_ALL_TREE_NODE_SCHEMAS = {
    ("Comment","plain"):   [("id","INT64"),("creationDate","STRING"),("locationIP","STRING"),
                             ("browserUsed","STRING"),("content","STRING"),("length","INT64")],
    ("Comment","dewey"):   [("id","INT64"),("creationDate","STRING"),("locationIP","STRING"),
                             ("browserUsed","STRING"),("content","STRING"),("length","INT64"),
                             ("height","INT64"),("depth","INT64"),("string_id","STRING")],
    ("Comment","prepost"): [("id","INT64"),("creationDate","STRING"),("locationIP","STRING"),
                             ("browserUsed","STRING"),("content","STRING"),("length","INT64"),
                             ("height","INT64"),("depth","INT64"),("integer_id","INT64"),("upper_bound","INT64")],
    ("Place","plain"):     [("id","INT64"),("name","STRING"),("url","STRING"),("type","STRING")],
    ("Place","dewey"):     [("id","INT64"),("name","STRING"),("url","STRING"),("type","STRING"),
                             ("height","INT64"),("depth","INT64"),("string_id","STRING")],
    ("Place","prepost"):   [("id","INT64"),("name","STRING"),("url","STRING"),("type","STRING"),
                             ("height","INT64"),("depth","INT64"),("integer_id","INT64"),("upper_bound","INT64")],
    ("Tagclass","plain"):  [("id","INT64"),("name","STRING"),("url","STRING")],
    ("Tagclass","dewey"):  [("id","INT64"),("name","STRING"),("url","STRING"),
                             ("height","INT64"),("depth","INT64"),("string_id","STRING")],
    ("Tagclass","prepost"):[("id","INT64"),("name","STRING"),("url","STRING"),
                             ("height","INT64"),("depth","INT64"),("integer_id","INT64"),("upper_bound","INT64")],
}

# Source CSV filename for each (tree-label, annotation) combination
S_ALL_TREE_NODE_FILES = {
    ("Comment","plain"):   "comment_0_0_plain.csv",
    ("Comment","dewey"):   "comment_0_0_dewey.csv",
    ("Comment","prepost"): "comment_0_0_prepost.csv",
    ("Place","plain"):     "place_0_0_plain.csv",
    ("Place","dewey"):     "place_0_0_dewey.csv",
    ("Place","prepost"):   "place_0_0_prepost.csv",
    ("Tagclass","plain"):  "tagclass_0_0_plain.csv",
    ("Tagclass","dewey"):  "tagclass_0_0_dewey.csv",
    ("Tagclass","prepost"):"tagclass_0_0_prepost.csv",
}

# Primary key per annotation type for tree-annotated nodes in s_all
S_ALL_TREE_PKS = {
    "plain":   "id",
    "dewey":   "string_id",
    "prepost": "integer_id",
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
                    f"TreeNodes_{annotation}.csv",
                )
                edge_csv = os.path.join(
                    DATA_DIR,
                    "artificial_trees",
                    tree_type,
                    str(size),
                    "edges",
                    "TreeEdges.csv",
                )
                datasets.append(
                    {
                        "graph_name": graph_name,
                        "node_label": "TreeNode",
                        "edge_label": "HAS_CHILD",
                        "node_csv": node_csv,
                        "edge_csv": edge_csv,
                        "annotation": annotation,
                    }
                )

    # Artificial forest
    for forest_size in [40, 1000]:
        for annotation in ANNOTATION_TYPES:
            graph_name = f"artificial_forest_{forest_size}_{annotation}"
            node_csv = os.path.join(
                DATA_DIR,
                "artificial_forests",
                f"{forest_size}",
                "nodes",
                f"TreeNodes_{annotation}.csv",
            )
            edge_csv = os.path.join(
                DATA_DIR,
                "artificial_forests",
                f"{forest_size}",
                "edges",
                "TreeEdges.csv",
            )
            datasets.append(
                {
                    "graph_name": graph_name,
                    "node_label": "TreeNode",
                    "edge_label": "HAS_CHILD",
                    "node_csv": node_csv,
                    "edge_csv": edge_csv,
                    "annotation": annotation,
                }
            )

    # SNB sf1
    for annotation in ANNOTATION_TYPES:
        graph_name = f"sf1_{annotation}"
        node_csv = os.path.join(DATA_DIR, "snb", "s_all", "nodes", f"comment_0_0_{annotation}.csv")
        edge_csv = os.path.join(DATA_DIR, "snb", "s_all", "edges", "comment_replyOf_comment_0_0.csv")
        datasets.append(
            {
                "graph_name": graph_name,
                "node_label": "Comment",
                "edge_label": "comment_replyOf_comment_0_0",
                "node_csv": node_csv,
                "edge_csv": edge_csv,
                "annotation": annotation,
            }
        )

    # SNB sf2 — Place nodes
    for annotation in ANNOTATION_TYPES:
        graph_name = f"sf2_{annotation}"
        node_csv = os.path.join(DATA_DIR, "snb", "s_all", "nodes", f"place_0_0_{annotation}.csv")
        edge_csv = os.path.join(DATA_DIR, "snb", "s_all", "edges", "place_isPartOf_place_0_0.csv")
        datasets.append(
            {
                "graph_name": graph_name,
                "node_label": "Place",
                "edge_label": "place_isPartOf_place_0_0",
                "node_csv": node_csv,
                "edge_csv": edge_csv,
                "annotation": annotation,
            }
        )

    # SNB sf3 — Tagclass nodes
    for annotation in ANNOTATION_TYPES:
        graph_name = f"sf3_{annotation}"
        node_csv = os.path.join(DATA_DIR, "snb", "s_all", "nodes", f"tagclass_0_0_{annotation}.csv")
        edge_csv = os.path.join(DATA_DIR, "snb", "s_all", "edges", "tagclass_isSubclassOf_tagclass_0_0.csv")
        datasets.append(
            {
                "graph_name": graph_name,
                "node_label": "Tagclass",
                "edge_label": "tagclass_isSubclassOf_tagclass_0_0",
                "node_csv": node_csv,
                "edge_csv": edge_csv,
                "annotation": annotation,
            }
        )

    return datasets


def preprocess_node_csv(src_path, dst_path, annotation):
    """Extract only the columns Kuzu needs from an AGE node CSV."""
    columns = NODE_CSV_COLUMNS[annotation]
    with open(src_path, "r", newline="") as fin, open(
        dst_path, "w", newline=""
    ) as fout:
        reader = csv.DictReader(fin)
        writer = csv.DictWriter(fout, fieldnames=columns)
        writer.writeheader()
        for row in reader:
            writer.writerow({col: row[col] for col in columns})


def build_id_mapping(node_csv_path, annotation):
    """Build a mapping from AGE id to the Kuzu primary key value."""
    pk_col = PRIMARY_KEYS[annotation]
    if pk_col == "id":
        return None  # No remapping needed
    mapping = {}
    with open(node_csv_path, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            mapping[row["id"]] = row[pk_col]
    return mapping


def preprocess_edge_csv(src_path, dst_path, id_mapping=None):
    """Extract start/end IDs from an AGE edge CSV, remapping to PKs if needed."""
    with open(src_path, "r", newline="") as fin, open(
        dst_path, "w", newline=""
    ) as fout:
        reader = csv.DictReader(fin)
        writer = csv.DictWriter(fout, fieldnames=["start_id", "end_id"])
        writer.writeheader()
        for row in reader:
            start = row["start_id"]
            end = row["end_id"]
            if id_mapping is not None:
                start = id_mapping[start]
                end = id_mapping[end]
            writer.writerow({"start_id": start, "end_id": end})


def preprocess_edge_csv_multi(src_path, dst_path, id_mappings):
    """Preprocess a multi-type edge CSV, remapping IDs for tree-annotated labels.

    id_mappings: {label -> {str_id -> pk_value}} for each tree-annotated label
    that needs remapping; absent labels keep their original id unchanged.
    Writes with '|' delimiter to avoid quoting issues with string fields.
    """
    with open(src_path, "r", newline="") as fin, open(
        dst_path, "w", newline=""
    ) as fout:
        reader = csv.DictReader(fin)
        writer = csv.DictWriter(fout, fieldnames=["start_id", "end_id"], delimiter="|")
        writer.writeheader()
        for row in reader:
            start = row["start_id"]
            end = row["end_id"]
            s_type = row["start_vertex_type"]
            e_type = row["end_vertex_type"]
            if s_type in id_mappings:
                start = id_mappings[s_type].get(start, start)
            if e_type in id_mappings:
                end = id_mappings[e_type].get(end, end)
            writer.writerow({"start_id": start, "end_id": end})


def create_kuzu_database(dataset, tmp_dir):
    """Create and populate a single Kuzu database."""
    graph_name = dataset["graph_name"]
    node_label = dataset["node_label"]
    edge_label = dataset["edge_label"]
    annotation = dataset["annotation"]
    node_csv = dataset["node_csv"]
    edge_csv = dataset["edge_csv"]

    db_path = os.path.join(KUZU_DIR, graph_name)

    # Skip if source CSVs don't exist
    if not os.path.isfile(node_csv):
        print(f"  SKIP {graph_name}: node CSV not found ({node_csv})")
        return False
    if not os.path.isfile(edge_csv):
        print(f"  SKIP {graph_name}: edge CSV not found ({edge_csv})")
        return False

    # Remove existing database directory (or stale file) if present
    if os.path.exists(db_path):
        if os.path.isfile(db_path):
            os.remove(db_path)
        else:
            shutil.rmtree(db_path)

    # Preprocess CSVs
    tmp_node_csv = os.path.join(tmp_dir, f"{graph_name}_nodes.csv")
    tmp_edge_csv = os.path.join(tmp_dir, f"{graph_name}_edges.csv")

    print(f"  Preprocessing node CSV...")
    preprocess_node_csv(node_csv, tmp_node_csv, annotation)

    print(f"  Building ID mapping...")
    id_mapping = build_id_mapping(node_csv, annotation)

    print(f"  Preprocessing edge CSV...")
    preprocess_edge_csv(edge_csv, tmp_edge_csv, id_mapping)

    # Create Kuzu database
    db = kuzu.Database(db_path)
    conn = kuzu.Connection(db)

    # Build node table schema
    schema = NODE_SCHEMAS[annotation]
    columns_sql = ", ".join(
        f"{col} {dtype}" for col, dtype in schema
    )
    pk_col = PRIMARY_KEYS[annotation]
    create_node_sql = (
        f"CREATE NODE TABLE {node_label}({columns_sql}, PRIMARY KEY({pk_col}))"
    )
    print(f"  Creating node table: {create_node_sql}")
    conn.execute(create_node_sql)

    # Create relationship table
    create_rel_sql = (
        f"CREATE REL TABLE {edge_label}"
        f"(FROM {node_label} TO {node_label})"
    )
    print(f"  Creating rel table: {create_rel_sql}")
    conn.execute(create_rel_sql)

    # Load node data
    print(f"  Loading nodes from {tmp_node_csv}...")
    conn.execute(
        f"COPY {node_label} FROM '{tmp_node_csv}' (HEADER=true)"
    )

    # Load edge data
    print(f"  Loading edges from {tmp_edge_csv}...")
    conn.execute(
        f"COPY {edge_label} FROM '{tmp_edge_csv}' (HEADER=true)"
    )

    # Clean up temp files
    os.remove(tmp_node_csv)
    os.remove(tmp_edge_csv)

    print(f"  OK: {graph_name}")
    return True


def create_s_all_kuzu_database(annotation, tmp_dir):
    """Create and populate the s_all_{annotation} Kuzu database."""
    graph_name = f"s_all_{annotation}"
    db_path = os.path.join(KUZU_DIR, graph_name)
    nodes_dir = os.path.join(DATA_DIR, "snb", "s_all", "nodes")
    edges_dir = os.path.join(DATA_DIR, "snb", "s_all", "edges")

    print(f"  Creating {graph_name}...")

    # Remove existing database directory (or stale file) if present
    if os.path.exists(db_path):
        if os.path.isfile(db_path):
            os.remove(db_path)
        else:
            shutil.rmtree(db_path)

    # Build id_mappings for tree-annotated labels (dewey/prepost only)
    id_mappings = {}
    pk_col = S_ALL_TREE_PKS[annotation]
    if pk_col != "id":
        for label in TREE_ANNOTATED_LABELS:
            node_file = S_ALL_TREE_NODE_FILES[(label, annotation)]
            node_csv_path = os.path.join(nodes_dir, node_file)
            if not os.path.isfile(node_csv_path):
                continue
            mapping = {}
            with open(node_csv_path, "r", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    mapping[row["id"]] = row[pk_col]
            id_mappings[label] = mapping

    db = kuzu.Database(db_path)
    conn = kuzu.Connection(db)

    loaded_labels = set()

    # Load non-tree node types (same schema for all annotation variants)
    for label, schema in NON_TREE_NODE_SCHEMAS.items():
        node_file = NON_TREE_NODE_FILES[label]
        node_csv_path = os.path.join(nodes_dir, node_file)
        if not os.path.isfile(node_csv_path):
            print(f"  SKIP {label}: node CSV not found ({node_csv_path})")
            continue

        columns_to_extract = NON_TREE_NODE_CSV_COLUMNS[label]
        tmp_node_csv = os.path.join(tmp_dir, f"{graph_name}_{label}_nodes.csv")
        with open(node_csv_path, "r", newline="") as fin, open(
            tmp_node_csv, "w", newline=""
        ) as fout:
            reader = csv.DictReader(fin)
            writer = csv.DictWriter(fout, fieldnames=columns_to_extract, delimiter="|")
            writer.writeheader()
            for row in reader:
                writer.writerow({col: row[col].replace("|", "") for col in columns_to_extract})

        columns_sql = ", ".join(f"{col} {dtype}" for col, dtype in schema)
        conn.execute(f"CREATE NODE TABLE {label}({columns_sql}, PRIMARY KEY(id))")
        conn.execute(f"COPY {label} FROM '{tmp_node_csv}' (HEADER=true, DELIM='|')")
        os.remove(tmp_node_csv)
        loaded_labels.add(label)
        print(f"    Loaded {label}")

    # Load tree-annotated node types
    for label in ["Comment", "Place", "Tagclass"]:
        node_file = S_ALL_TREE_NODE_FILES[(label, annotation)]
        node_csv_path = os.path.join(nodes_dir, node_file)
        if not os.path.isfile(node_csv_path):
            print(f"  SKIP {label}: node CSV not found ({node_csv_path})")
            continue

        schema = S_ALL_TREE_NODE_SCHEMAS[(label, annotation)]
        columns_to_extract = [col for col, _ in schema]
        tmp_node_csv = os.path.join(tmp_dir, f"{graph_name}_{label}_nodes.csv")
        with open(node_csv_path, "r", newline="") as fin, open(
            tmp_node_csv, "w", newline=""
        ) as fout:
            reader = csv.DictReader(fin)
            writer = csv.DictWriter(fout, fieldnames=columns_to_extract, delimiter="|")
            writer.writeheader()
            for row in reader:
                writer.writerow({col: row[col].replace("|", "") for col in columns_to_extract})

        columns_sql = ", ".join(f"{col} {dtype}" for col, dtype in schema)
        conn.execute(
            f"CREATE NODE TABLE {label}({columns_sql}, PRIMARY KEY({pk_col}))"
        )
        conn.execute(f"COPY {label} FROM '{tmp_node_csv}' (HEADER=true, DELIM='|')")
        os.remove(tmp_node_csv)
        loaded_labels.add(label)
        print(f"    Loaded {label}")

    # Load all 23 edge types
    for edge_label, from_label, to_label, edge_file in S_ALL_EDGES:
        if from_label not in loaded_labels or to_label not in loaded_labels:
            print(f"  SKIP edge {edge_label}: {from_label} or {to_label} not loaded")
            continue

        edge_csv_path = os.path.join(edges_dir, edge_file)
        if not os.path.isfile(edge_csv_path):
            print(f"  SKIP edge {edge_label}: CSV not found ({edge_csv_path})")
            continue

        tmp_edge_csv = os.path.join(tmp_dir, f"{graph_name}_{edge_label}.csv")
        preprocess_edge_csv_multi(edge_csv_path, tmp_edge_csv, id_mappings)

        conn.execute(
            f"CREATE REL TABLE {edge_label}(FROM {from_label} TO {to_label})"
        )
        conn.execute(f"COPY {edge_label} FROM '{tmp_edge_csv}' (HEADER=true, DELIM='|')")
        os.remove(tmp_edge_csv)
        print(f"    Loaded edge {edge_label}")

    print(f"  OK: {graph_name}")
    return True


def main():
    datasets = build_dataset_list()
    print(f"Found {len(datasets)} graph variants to create.\n")

    os.makedirs(KUZU_DIR, exist_ok=True)

    tmp_dir = tempfile.mkdtemp(prefix="kuzu_init_")
    try:
        created = 0
        skipped = 0
        for i, dataset in enumerate(datasets, 1):
            print(f"[{i}/{len(datasets)}] {dataset['graph_name']}")
            if create_kuzu_database(dataset, tmp_dir):
                created += 1
            else:
                skipped += 1
            print()
        # s_all multi-type graphs
        for annotation in ANNOTATION_TYPES:
            print(f"[s_all_{annotation}]")
            create_s_all_kuzu_database(annotation, tmp_dir)
            print()
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    print(f"Done. Created: {created}, Skipped: {skipped}")


if __name__ == "__main__":
    main()
