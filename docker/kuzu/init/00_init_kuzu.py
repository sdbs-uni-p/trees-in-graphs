#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-only

"""
Initialize Kuzu databases for the tree benchmark.

Creates one Kuzu database per AGE-compatible graph variant
(e.g. artificial_trees_truebase_10_baseline),
mirroring the Apache AGE setup. Each database contains a node table
and a relationship table loaded from the same CSV files used by AGE.

Since AGE CSVs contain extra columns (type, start_vertex_type, etc.)
that don't map to Kuzu's schema, this script preprocesses CSVs into
a temporary directory before loading.
"""

import csv
import json
import os
import shutil
import tempfile

import kuzu

DATA_DIR = "/project/data/prepared"
KUZU_DIR = "/kuzu_data"
DEPTH_METADATA_FILE = os.path.join(KUZU_DIR, "max_depths.json")

# Node table schemas per annotation type (column_name, kuzu_type)
NODE_SCHEMAS = {
    "plain": [
        ("id", "INT64"),
    ],
    "dewey": [
        ("id", "INT64"),
        ("dewey", "STRING"),
        ("height", "INT64"),
        ("depth", "INT64"),
    ],
    "prepost": [
        ("id", "INT64"),
        ("pre", "INT64"),
        ("post", "INT64"),
        ("height", "INT64"),
        ("depth", "INT64"),
    ],
}

# Primary key column per annotation type
PRIMARY_KEYS = {
    "plain": "id",
    "dewey": "dewey",
    "prepost": "pre",
}

# CSV columns to extract from AGE node CSVs (skip 'type' and AGE-specific columns)
NODE_CSV_COLUMNS = {
    "plain": ["id"],
    "dewey": ["id", "dewey", "height", "depth"],
    "prepost": ["id", "pre", "post", "height", "depth"],
}

# All datasets to load
ARTIFICIAL_TREE_TYPES = ["truebase", "ultratall", "ultrawide"]
ARTIFICIAL_TREE_SIZES = [10, 100, 1000, 10000, 100000]
ANNOTATION_TYPES = ["plain", "dewey", "prepost"]


def graph_variant(annotation: str) -> str:
    """Return the AGE-compatible graph suffix for an annotation source."""
    return "baseline" if annotation == "plain" else annotation


def tree_nodes_filename(annotation: str) -> str:
    """Return the tree node CSV filename for a given annotation."""
    return "TreeNode.csv" if annotation == "plain" else f"TreeNode_{annotation}.csv"

# â”€â”€â”€ sf1 (full LDBC SNB) constants â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

# Labels that carry tree-annotation columns (dewey/prepost)
TREE_ANNOTATED_LABELS = {"Comment", "Place", "TagClass"}

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

# Tree-annotated node schemas for sf1, keyed by (label, annotation)
S_ALL_TREE_NODE_SCHEMAS = {
    ("Comment","plain"):   [("id","INT64"),("creationDate","STRING"),("locationIP","STRING"),
                             ("browserUsed","STRING"),("content","STRING"),("length","INT64")],
    ("Comment","dewey"):   [("id","INT64"),("creationDate","STRING"),("locationIP","STRING"),
                             ("browserUsed","STRING"),("content","STRING"),("length","INT64"),
                             ("height","INT64"),("depth","INT64"),("dewey","STRING")],
    ("Comment","prepost"): [("id","INT64"),("creationDate","STRING"),("locationIP","STRING"),
                             ("browserUsed","STRING"),("content","STRING"),("length","INT64"),
                             ("height","INT64"),("depth","INT64"),("pre","INT64"),("post","INT64")],
    ("Place","plain"):     [("id","INT64"),("name","STRING"),("url","STRING"),("type","STRING")],
    ("Place","dewey"):     [("id","INT64"),("name","STRING"),("url","STRING"),("type","STRING"),
                             ("height","INT64"),("depth","INT64"),("dewey","STRING")],
    ("Place","prepost"):   [("id","INT64"),("name","STRING"),("url","STRING"),("type","STRING"),
                             ("height","INT64"),("depth","INT64"),("pre","INT64"),("post","INT64")],
    ("TagClass","plain"):  [("id","INT64"),("name","STRING"),("url","STRING")],
    ("TagClass","dewey"):  [("id","INT64"),("name","STRING"),("url","STRING"),
                             ("height","INT64"),("depth","INT64"),("dewey","STRING")],
    ("TagClass","prepost"):[("id","INT64"),("name","STRING"),("url","STRING"),
                             ("height","INT64"),("depth","INT64"),("pre","INT64"),("post","INT64")],
}

# Source CSV filename for each (tree-label, annotation) combination
S_ALL_TREE_NODE_FILES = {
    ("Comment","plain"):   "comment_0_0.csv",
    ("Comment","dewey"):   "comment_0_0_dewey.csv",
    ("Comment","prepost"): "comment_0_0_prepost.csv",
    ("Place","plain"):     "place_0_0.csv",
    ("Place","dewey"):     "place_0_0_dewey.csv",
    ("Place","prepost"):   "place_0_0_prepost.csv",
    ("TagClass","plain"):  "tagclass_0_0.csv",
    ("TagClass","dewey"):  "tagclass_0_0_dewey.csv",
    ("TagClass","prepost"):"tagclass_0_0_prepost.csv",
}

# Primary key per annotation type for tree-annotated nodes in sf1
S_ALL_TREE_PKS = {
    "plain":   "id",
    "dewey":   "dewey",
    "prepost": "pre",
}

# All 23 edge sources mapped to the same 15 logical labels used by AGE.
# Kuzu relationship groups combine sources with different endpoint pairs.
S_ALL_EDGES = [
    ("HAS_CREATOR",    "Comment",      "Person",       "comment_hasCreator_person_0_0.csv"),
    ("HAS_TAG",        "Comment",      "Tag",          "comment_hasTag_tag_0_0.csv"),
    ("IS_LOCATED_IN",  "Comment",      "Place",        "comment_isLocatedIn_place_0_0.csv"),
    ("REPLY_OF",       "Comment",      "Comment",      "comment_replyOf_comment_0_0.csv"),
    ("REPLY_OF",       "Comment",      "Post",         "comment_replyOf_post_0_0.csv"),
    ("CONTAINER_OF",   "Forum",        "Post",         "forum_containerOf_post_0_0.csv"),
    ("HAS_MEMBER",     "Forum",        "Person",       "forum_hasMember_person_0_0.csv"),
    ("HAS_MODERATOR",  "Forum",        "Person",       "forum_hasModerator_person_0_0.csv"),
    ("HAS_TAG",        "Forum",        "Tag",          "forum_hasTag_tag_0_0.csv"),
    ("IS_LOCATED_IN",  "Organisation", "Place",        "organisation_isLocatedIn_place_0_0.csv"),
    ("HAS_INTEREST",   "Person",       "Tag",          "person_hasInterest_tag_0_0.csv"),
    ("IS_LOCATED_IN",  "Person",       "Place",        "person_isLocatedIn_place_0_0.csv"),
    ("KNOWS",          "Person",       "Person",       "person_knows_person_0_0.csv"),
    ("LIKES",          "Person",       "Comment",      "person_likes_comment_0_0.csv"),
    ("LIKES",          "Person",       "Post",         "person_likes_post_0_0.csv"),
    ("STUDY_AT",       "Person",       "Organisation", "person_studyAt_organisation_0_0.csv"),
    ("WORK_AT",        "Person",       "Organisation", "person_workAt_organisation_0_0.csv"),
    ("IS_PART_OF",     "Place",        "Place",        "place_isPartOf_place_0_0.csv"),
    ("HAS_CREATOR",    "Post",         "Person",       "post_hasCreator_person_0_0.csv"),
    ("HAS_TAG",        "Post",         "Tag",          "post_hasTag_tag_0_0.csv"),
    ("IS_LOCATED_IN",  "Post",         "Place",        "post_isLocatedIn_place_0_0.csv"),
    ("HAS_TYPE",       "Tag",          "TagClass",     "tag_hasType_tagclass_0_0.csv"),
    ("IS_SUBCLASS_OF", "TagClass",     "TagClass",     "tagclass_isSubclassOf_tagclass_0_0.csv"),
]


def build_dataset_list():
    """Build the full list of datasets to create."""
    datasets = []

    # Artificial trees: truebase, ultratall, ultrawide
    for tree_type in ARTIFICIAL_TREE_TYPES:
        for size in ARTIFICIAL_TREE_SIZES:
            for annotation in ANNOTATION_TYPES:
                graph_name = (
                    f"artificial_trees_{tree_type}_{size}_{graph_variant(annotation)}"
                )
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
            graph_name = f"artificial_forests_{forest_size}_{graph_variant(annotation)}"
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

    return datasets


def read_max_depth(node_csv):
    """Return the maximum non-negative depth in an annotated node CSV."""
    maximum = None
    with open(node_csv, "r", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames or "depth" not in reader.fieldnames:
            raise ValueError(f"Missing depth column in {node_csv}")
        for line_number, row in enumerate(reader, start=2):
            try:
                depth = int(row["depth"])
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"Invalid depth in {node_csv}:{line_number}: {row['depth']!r}"
                ) from exc
            if depth < 0:
                raise ValueError(
                    f"Negative depth in {node_csv}:{line_number}: {depth}"
                )
            maximum = depth if maximum is None else max(maximum, depth)
    if maximum is None:
        raise ValueError(f"Annotated node CSV is empty: {node_csv}")
    return maximum


def determine_depth_metadata(datasets):
    """Determine per-graph and global depths from configured Dewey CSVs."""
    sources = {}
    for dataset in datasets:
        if dataset["annotation"] != "dewey":
            continue
        graph_name = dataset["graph_name"].removesuffix("_dewey")
        sources[graph_name] = dataset["node_csv"]

    snb_nodes_dir = os.path.join(DATA_DIR, "snb", "sf1", "nodes")
    for tree_label in sorted(TREE_ANNOTATED_LABELS):
        graph_name = f"snb_sf1_{tree_label.lower()}"
        sources[graph_name] = os.path.join(
            snb_nodes_dir, S_ALL_TREE_NODE_FILES[(tree_label, "dewey")]
        )

    graph_depths = {}
    for graph_name, node_csv in sorted(sources.items()):
        if not os.path.isfile(node_csv):
            raise FileNotFoundError(
                f"Configured Dewey CSV not found for {graph_name}: {node_csv}"
            )
        graph_depths[graph_name] = read_max_depth(node_csv)

    if not graph_depths:
        raise ValueError("No configured Dewey datasets found")
    return {
        "global_max_depth": max(graph_depths.values()),
        "graphs": graph_depths,
    }


def write_depth_metadata(metadata):
    """Atomically publish generated depth metadata for experiment connections."""
    temporary_path = DEPTH_METADATA_FILE + ".tmp"
    with open(temporary_path, "w", encoding="utf-8") as handle:
        json.dump(metadata, handle, indent=2, sort_keys=True)
        handle.write("\n")
    os.replace(temporary_path, DEPTH_METADATA_FILE)


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
            # Prepared SNB CSVs still spell this source type "Tagclass";
            # normalize it to the AGE-compatible node label used in Kuzu.
            s_type = row["start_vertex_type"].replace("Tagclass", "TagClass")
            e_type = row["end_vertex_type"].replace("Tagclass", "TagClass")
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


def create_snb_kuzu_database(
    tree_label, annotation, tmp_dir, *, graph_name=None, annotated_labels=None
):
    """Create a full SNB graph with selected tree labels annotated.

    The regular tree benchmark passes one ``tree_label``.  The LDBC setup
    passes all three labels and an explicit graph name so that its queries see
    the same complete annotated graph as ``age_ldbc``.
    """
    if annotated_labels is None:
        annotated_labels = {tree_label}
    else:
        annotated_labels = set(annotated_labels)
    if graph_name is None:
        graph_name = f"snb_sf1_{tree_label.lower()}_{graph_variant(annotation)}"
    db_path = os.path.join(KUZU_DIR, graph_name)
    nodes_dir = os.path.join(DATA_DIR, "snb", "sf1", "nodes")
    edges_dir = os.path.join(DATA_DIR, "snb", "sf1", "edges")

    print(f"  Creating {graph_name}...")

    # Remove existing database directory (or stale file) if present
    if os.path.exists(db_path):
        if os.path.isfile(db_path):
            os.remove(db_path)
        else:
            shutil.rmtree(db_path)

    # Only the selected tree uses its structural key; all other labels use id.
    id_mappings = {}
    pk_col = S_ALL_TREE_PKS[annotation]
    if pk_col != "id":
        for annotated_label in annotated_labels:
            node_file = S_ALL_TREE_NODE_FILES[(annotated_label, annotation)]
            node_csv_path = os.path.join(nodes_dir, node_file)
            if os.path.isfile(node_csv_path):
                mapping = {}
                with open(node_csv_path, "r", newline="") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        mapping[row["id"]] = row[pk_col]
                id_mappings[annotated_label] = mapping

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
    for label in ["Comment", "Place", "TagClass"]:
        label_annotation = annotation if label in annotated_labels else "plain"
        node_file = S_ALL_TREE_NODE_FILES[(label, label_annotation)]
        node_csv_path = os.path.join(nodes_dir, node_file)
        if not os.path.isfile(node_csv_path):
            print(f"  SKIP {label}: node CSV not found ({node_csv_path})")
            continue

        schema = S_ALL_TREE_NODE_SCHEMAS[(label, label_annotation)]
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
        label_pk = S_ALL_TREE_PKS[label_annotation]
        conn.execute(
            f"CREATE NODE TABLE {label}({columns_sql}, PRIMARY KEY({label_pk}))"
        )
        conn.execute(f"COPY {label} FROM '{tmp_node_csv}' (HEADER=true, DELIM='|')")
        os.remove(tmp_node_csv)
        loaded_labels.add(label)
        print(f"    Loaded {label}")

    # Collect available edge sources by their AGE-compatible logical label.
    edge_groups = {}
    for edge_label, from_label, to_label, edge_file in S_ALL_EDGES:
        if from_label not in loaded_labels or to_label not in loaded_labels:
            print(f"  SKIP edge {edge_label}: {from_label} or {to_label} not loaded")
            continue

        edge_csv_path = os.path.join(edges_dir, edge_file)
        if not os.path.isfile(edge_csv_path):
            print(f"  SKIP edge {edge_label}: CSV not found ({edge_csv_path})")
            continue

        edge_groups.setdefault(edge_label, []).append(
            (from_label, to_label, edge_file, edge_csv_path)
        )

    # A Kuzu relationship group represents one logical AGE edge label across
    # all of its valid source/target node-table combinations.
    for edge_label, sources in edge_groups.items():
        endpoint_pairs = [(source[0], source[1]) for source in sources]
        endpoints_sql = ", ".join(
            f"FROM {from_label} TO {to_label}"
            for from_label, to_label in endpoint_pairs
        )
        conn.execute(f"CREATE REL TABLE {edge_label}({endpoints_sql})")

        for from_label, to_label, edge_file, edge_csv_path in sources:
            source_name = os.path.splitext(edge_file)[0]
            tmp_edge_csv = os.path.join(
                tmp_dir, f"{graph_name}_{source_name}.csv"
            )
            preprocess_edge_csv_multi(edge_csv_path, tmp_edge_csv, id_mappings)

            copy_options = ["HEADER=true", "DELIM='|'"]
            if len(endpoint_pairs) > 1:
                copy_options.extend(
                    [f"FROM='{from_label}'", f"TO='{to_label}'"]
                )
            conn.execute(
                f"COPY {edge_label} FROM '{tmp_edge_csv}' "
                f"({', '.join(copy_options)})"
            )
            os.remove(tmp_edge_csv)
            print(f"    Loaded {edge_file} into {edge_label}")

    print(f"  OK: {graph_name}")
    return True


def main():
    datasets = build_dataset_list()
    print(f"Found {len(datasets)} graph variants to create.\n")

    os.makedirs(KUZU_DIR, exist_ok=True)
    print("Determining maximum tree depths...")
    depth_metadata = determine_depth_metadata(datasets)
    print(f"Global maximum tree depth: {depth_metadata['global_max_depth']}\n")

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
        # Full SNB graphs, matching AGE's tree-specific graph names.
        for tree_label in sorted(TREE_ANNOTATED_LABELS):
            for annotation in ANNOTATION_TYPES:
                graph_name = (
                    f"snb_sf1_{tree_label.lower()}_{graph_variant(annotation)}"
                )
                print(f"[{graph_name}]")
                create_snb_kuzu_database(tree_label, annotation, tmp_dir)
                print()
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    write_depth_metadata(depth_metadata)
    print(f"Depth metadata: {DEPTH_METADATA_FILE}")
    print(f"Done. Created: {created}, Skipped: {skipped}")


if __name__ == "__main__":
    main()
