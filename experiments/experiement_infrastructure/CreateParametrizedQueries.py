# SPDX-License-Identifier: GPL-3.0-only

import json
from pathlib import Path
import random
from experiments.experiement_infrastructure import Executor

class Parametrizer:
    """
    Generates query parameters by sampling from graph metadata.

    The metadata files should contain:
    - "roots": list of root node IDs
    - "id_list": list of all node IDs in the graph
    """

    def __init__(self, base_meta_path: Path, ex : Executor, db_name: str = None):
        """
        Initialize the Parametrizer.

        Args:
            base_meta_path: Path to directory containing metadata JSON files
            db_name: Actual database name for queries (e.g. "s_all_dewey"); defaults
                     to graph_name if not provided. Used by AGE parametrizers whose
                     Cypher queries must reference the real graph name.
        """
        self.base_meta_path = Path(base_meta_path)
        self.current_meta = None
        self.ex = ex
        self.db_name = db_name
        self.graph_name = None
        self.node_name = None
        self.relation_name = None

    def set_metadata(self, metadata_name: str):
        """
        Load metadata for a specific graph.

        Args:
            metadata_name: Name of the graph (without .json extension)
        """
        full_path = self.base_meta_path / f"{metadata_name}.json"

        if not full_path.exists():
            print(f"Warning: Metadata file not found: {full_path}")
            self.current_meta = None
            return

        with open(full_path, "r") as f:
            self.current_meta = json.load(f)

        self.graph_name = metadata_name
        if "s1" in self.graph_name:
            self.node_name = "Comment"
            self.relation_name = "comment_replyOf_comment_0_0"
        elif "s2" in self.graph_name:
            self.node_name = "Place"
            self.relation_name = "place_isPartOf_place_0_0"
        elif "s3" in self.graph_name:
            self.node_name = "Tagclass"
            self.relation_name = "tagclass_isSubclassOf_tagclass_0_0"
        else:
            self.node_name = "TreeNode"
            self.relation_name = "HAS_CHILD"

    def sample_n(self, n: int):
        """
        Generate n random parameter sets.

        Args:
            n: Number of parameter sets to generate

        Returns:
            List of dictionaries containing parameter substitutions
        """
        if self.current_meta is None:
            return []

        parameter_sample = []

        for _ in range(n):
            root_annotation_value = random.choice(self.current_meta["roots"])
            root_id_from_annotation = self.ex.collect_id(root_annotation_value, self.node_name, self.graph_name)

            id1_annotation_value = random.choice(self.current_meta["id_list"])
            id1_id_from_annotation = self.ex.collect_id(id1_annotation_value, self.node_name, self.graph_name)

            id2_annotation_value = random.choice(self.current_meta["id_list"])
            id2_id_from_annotation = self.ex.collect_id(id2_annotation_value, self.node_name, self.graph_name)

            nodeID_annotation_value = random.choice(self.current_meta["id_list"])
            nodeID_id_from_annotation = self.ex.collect_id(nodeID_annotation_value, self.node_name, self.graph_name)

            parameter_sample.append(
                {
                    "$rootID": root_id_from_annotation,
                    "$id1": id1_id_from_annotation,
                    "$id2": id2_id_from_annotation,
                    "$nodeID": nodeID_id_from_annotation,
                    "$REL_TYPE": self.relation_name,
                    "$NODE_TYPE": self.node_name,
                }
            )
        return parameter_sample


class KuzuParametrizer(Parametrizer):
    """
    Parametrizer for Kuzu that provides plain, dewey, and prepost node
    identifiers, since each Kuzu graph variant uses a different primary key
    (id for plain, string_id for dewey, integer_id for prepost).

    Loads both dewey and prepost metadata and uses index-based correspondence
    to ensure all three variants query the same node. On first use for each
    graph, builds a string_idâ†’id mapping by querying the dewey Kuzu database
    so that plain id values can be resolved.

    Returns parameters:
      $rootID, $id1, $id2, $nodeID                       â€” plain id values
      $deweyRoot, $deweyId1, $deweyId2, $deweyNodeID     â€” string_id values
      $prepostRoot, $prepostId1, $prepostId2, $prepostNodeID â€” integer_id values
      $REL_TYPE, $NODE_TYPE

    Dewey query templates should quote the dewey parameters, e.g.:
      MATCH (root:$NODE_TYPE {string_id: "$deweyRoot"})
    """

    def __init__(self, base_meta_path: Path, ex: Executor, db_name: str = None):
        super().__init__(base_meta_path, ex, db_name=db_name)
        self.prepost_meta = None
        self._dewey_to_id = None

    def set_metadata(self, metadata_name: str):
        super().set_metadata(metadata_name)

        # Load prepost metadata
        prepost_name = metadata_name.replace("_dewey", "_prepost")
        prepost_path = self.base_meta_path / f"{prepost_name}.json"
        if prepost_path.exists():
            with open(prepost_path, "r") as f:
                self.prepost_meta = json.load(f)
        else:
            print(f"Warning: Prepost metadata not found: {prepost_path}")
            self.prepost_meta = None

        # Build string_id â†’ plain id mapping by querying the dewey database
        # (which now includes the id column alongside string_id).
        self._dewey_to_id = {}
        if self.current_meta is not None:
            try:
                _, rows = self.ex.execute_query(
                    f"MATCH (n:{self.node_name}) RETURN n.string_id, n.id"
                )
                for row in rows:
                    string_id = row[0]
                    plain_id = row[1]
                    self._dewey_to_id[string_id] = plain_id
            except Exception as e:
                print(f"Warning: could not build deweyâ†’id mapping: {e}")

    def _resolve_id(self, dewey_value):
        """Resolve a dewey string_id to its plain id."""
        if self._dewey_to_id and dewey_value in self._dewey_to_id:
            return self._dewey_to_id[dewey_value]
        return dewey_value

    def sample_n(self, n: int):
        if self.current_meta is None:
            return []

        dewey_roots = self.current_meta["roots"]
        dewey_ids = self.current_meta["id_list"]
        prepost_roots = self.prepost_meta["roots"] if self.prepost_meta else dewey_roots
        prepost_ids = self.prepost_meta["id_list"] if self.prepost_meta else dewey_ids

        parameter_sample = []
        for _ in range(n):
            root_idx = random.randrange(len(dewey_roots))
            id1_idx = random.randrange(len(dewey_ids))
            id2_idx = random.randrange(len(dewey_ids))
            nodeID_idx = random.randrange(len(dewey_ids))

            parameter_sample.append(
                {
                    "$rootID": self._resolve_id(dewey_roots[root_idx]),
                    "$deweyRoot": dewey_roots[root_idx],
                    "$prepostRoot": prepost_roots[root_idx],
                    "$id1": self._resolve_id(dewey_ids[id1_idx]),
                    "$deweyId1": dewey_ids[id1_idx],
                    "$prepostId1": prepost_ids[id1_idx],
                    "$id2": self._resolve_id(dewey_ids[id2_idx]),
                    "$deweyId2": dewey_ids[id2_idx],
                    "$prepostId2": prepost_ids[id2_idx],
                    "$nodeID": self._resolve_id(dewey_ids[nodeID_idx]),
                    "$deweyNodeID": dewey_ids[nodeID_idx],
                    "$prepostNodeID": prepost_ids[nodeID_idx],
                    "$REL_TYPE": self.relation_name,
                    "$NODE_TYPE": self.node_name,
                }
            )
        return parameter_sample


class ReducedKuzuParametrizer(KuzuParametrizer):
    """
    Fixed-root parametrizer for Kuzu, analogous to ReducedParametrizer.

    Uses the same hardcoded plain id values as ReducedParametrizer
    (which correspond to the CSV 'id' / AGE '__id__' column), then
    resolves dewey and prepost identifiers via the _dewey_to_id mapping
    and metadata index correspondence.

    For graphs listed in parameter.json, $id1_t/$id1_f and $id2_t/$id2_f
    are overridden with the values from that file. $rootID is always
    determined by _FIXED_ROOTS.
    """

    # Same values as ReducedParametrizer â€” these are CSV id values
    # (= AGE __id__ property = Kuzu id property)
    _FIXED_ROOTS = {
        "s1": 1374390095024,
        "s2": 1455,
        "s3": 1,
        "artificial_forest": 2,
        "_default": 1,
    }

    _PARAM_FILE = Path(__file__).parent / "parameter.json"

    def __init__(self, base_meta_path: Path, ex: Executor, db_name: str = None):
        super().__init__(base_meta_path, ex, db_name=db_name)
        self._fixed_rootID = None
        self._fixed_deweyRoot = None
        self._fixed_prepostRoot = None
        # TRUE pair: root + deepest leaf in tree 1
        self._fixed_id2_t = None
        self._fixed_deweyId2_t = None
        self._fixed_prepostId2_t = None
        # FALSE pair: root of tree 1 + root of tree 2 (or same as TRUE for single-tree graphs)
        self._fixed_id2_f = None
        self._fixed_deweyId2_f = None
        self._fixed_prepostId2_f = None
        # CSV-provided id1 (may differ from root; None means fall back to root)
        self._fixed_id1 = None
        self._fixed_deweyId1 = None
        self._fixed_prepostId1 = None

        self._params = {}
        if self._PARAM_FILE.exists():
            with open(self._PARAM_FILE) as f:
                self._params = json.load(f)

    def _plain_to_dewey(self, plain_id: int) -> str | None:
        """Reverse-lookup: plain id â†’ dewey string_id."""
        if self._dewey_to_id:
            for string_id, pid in self._dewey_to_id.items():
                if pid == plain_id:
                    return string_id
        return None

    def _plain_to_prepost(self, plain_id: int):
        """Resolve plain id â†’ prepost integer_id via index correspondence."""
        dewey_val = self._plain_to_dewey(plain_id)
        if dewey_val and self.current_meta and self.prepost_meta:
            dewey_ids = self.current_meta["id_list"]
            prepost_ids = self.prepost_meta["id_list"]
            if dewey_val in dewey_ids:
                idx = dewey_ids.index(dewey_val)
                if idx < len(prepost_ids):
                    return prepost_ids[idx]
        return None

    def set_metadata(self, metadata_name: str):
        # Call parent to load dewey/prepost metadata and build mappings
        super().set_metadata(metadata_name)

        # Determine the fixed plain rootID
        if "s1" in self.graph_name:
            self._fixed_rootID = self._FIXED_ROOTS["s1"]
        elif "s2" in self.graph_name:
            self._fixed_rootID = self._FIXED_ROOTS["s2"]
        elif "s3" in self.graph_name:
            self._fixed_rootID = self._FIXED_ROOTS["s3"]
        elif "artificial_forest" in self.graph_name:
            self._fixed_rootID = self._FIXED_ROOTS["artificial_forest"]
        else:
            self._fixed_rootID = self._FIXED_ROOTS["_default"]

        # Resolve dewey string_id via the _dewey_to_id mapping
        # (built by super().set_metadata from the dewey database)
        if self._dewey_to_id:
            for string_id, plain_id in self._dewey_to_id.items():
                if plain_id == self._fixed_rootID:
                    self._fixed_deweyRoot = string_id
                    break

        if self._fixed_deweyRoot is None:
            print(f"Warning: could not find dewey string_id for id={self._fixed_rootID} "
                  f"in {self.graph_name}")

        # Resolve prepost integer_id via index correspondence in metadata
        if self._fixed_deweyRoot and self.current_meta and self.prepost_meta:
            dewey_ids = self.current_meta["id_list"]
            prepost_ids = self.prepost_meta["id_list"]
            if self._fixed_deweyRoot in dewey_ids:
                idx = dewey_ids.index(self._fixed_deweyRoot)
                if idx < len(prepost_ids):
                    self._fixed_prepostRoot = prepost_ids[idx]

        if self._fixed_prepostRoot is None:
            print(f"Warning: could not find prepost integer_id for dewey={self._fixed_deweyRoot} "
                  f"in {self.graph_name}")

        # Resolve TRUE pair id2: deepest leaf in tree 1 (longest dewey string_id in id_list
        # that belongs to the same tree as the fixed root, i.e. starts with root + '.' or == root)
        if self.current_meta and self.prepost_meta and self._fixed_deweyRoot:
            dewey_ids = self.current_meta["id_list"]
            prepost_ids = self.prepost_meta["id_list"]
            root_prefix = self._fixed_deweyRoot + "."
            tree1_entries = [
                (i, d) for i, d in enumerate(dewey_ids)
                if d == self._fixed_deweyRoot or d.startswith(root_prefix)
            ]
            if tree1_entries:
                deepest_idx, deepest_dewey = max(tree1_entries, key=lambda x: x[1].count("."))
                self._fixed_deweyId2_t = deepest_dewey
                self._fixed_prepostId2_t = prepost_ids[deepest_idx]
                self._fixed_id2_t = self._resolve_id(deepest_dewey)
            else:
                print(f"Warning: could not find deepest leaf for root={self._fixed_deweyRoot} "
                      f"in {self.graph_name}")

        # Resolve FALSE pair id2: root of the second tree (if this graph has multiple trees)
        # Falls back to TRUE id2 for single-tree graphs (the false queries will return
        # non-false results for those, as acknowledged).
        if self.current_meta and self.prepost_meta:
            dewey_roots = self.current_meta["roots"]
            prepost_roots = self.prepost_meta["roots"]
            if len(dewey_roots) > 1:
                false_dewey = dewey_roots[1]
                self._fixed_deweyId2_f = false_dewey
                self._fixed_prepostId2_f = prepost_roots[1]
                self._fixed_id2_f = self._resolve_id(false_dewey)
            else:
                self._fixed_deweyId2_f = self._fixed_deweyId2_t
                self._fixed_prepostId2_f = self._fixed_prepostId2_t
                self._fixed_id2_f = self._fixed_id2_t

        # Override id1/id2 from parameter.json if this graph is covered
        metadata_base = metadata_name.removesuffix("_dewey")
        if metadata_base in self._params:
            entry = self._params[metadata_base]
            csv_id1, csv_id2 = entry["id1"], entry["id2"]

            self._fixed_id1 = csv_id1
            self._fixed_deweyId1 = self._plain_to_dewey(csv_id1)
            self._fixed_prepostId1 = self._plain_to_prepost(csv_id1)

            csv_id2_dewey = self._plain_to_dewey(csv_id2)
            csv_id2_prepost = self._plain_to_prepost(csv_id2)
            self._fixed_id2_t = csv_id2
            self._fixed_deweyId2_t = csv_id2_dewey
            self._fixed_prepostId2_t = csv_id2_prepost
            self._fixed_id2_f = csv_id2
            self._fixed_deweyId2_f = csv_id2_dewey
            self._fixed_prepostId2_f = csv_id2_prepost

    def sample_n(self, n: int):
        # Use CSV-provided id1 if available, otherwise fall back to root
        id1 = self._fixed_id1 if self._fixed_id1 is not None else self._fixed_rootID
        dewey_id1 = self._fixed_deweyId1 if self._fixed_id1 is not None else self._fixed_deweyRoot
        prepost_id1 = self._fixed_prepostId1 if self._fixed_id1 is not None else self._fixed_prepostRoot

        parameter_sample = []
        for _ in range(n):
            parameter_sample.append(
                {
                    "$rootID": self._fixed_rootID,
                    "$deweyRoot": self._fixed_deweyRoot,
                    "$prepostRoot": self._fixed_prepostRoot,
                    # TRUE pair
                    "$id1_t": id1,
                    "$id2_t": self._fixed_id2_t,
                    "$deweyId1_t": dewey_id1,
                    "$deweyId2_t": self._fixed_deweyId2_t,
                    "$prepostId1_t": prepost_id1,
                    "$prepostId2_t": self._fixed_prepostId2_t,
                    # FALSE pair
                    "$id1_f": id1,
                    "$id2_f": self._fixed_id2_f,
                    "$deweyId1_f": dewey_id1,
                    "$deweyId2_f": self._fixed_deweyId2_f,
                    "$prepostId1_f": prepost_id1,
                    "$prepostId2_f": self._fixed_prepostId2_f,
                    # Single node for all_ancestors (same as TRUE id2: deepest leaf)
                    "$nodeID": self._fixed_id2_t,
                    "$deweyId": self._fixed_deweyId2_t,
                    "$prepostId": self._fixed_prepostId2_t,
                    "$REL_TYPE": self.relation_name,
                    "$NODE_TYPE": self.node_name,
                }
            )
        return parameter_sample


class ReducedParametrizer(Parametrizer):
    _FIXED_ROOTS = {
        "s1": 1374390095024,
        "s2": 1455,
        "s3": 1,
        "artificial_forest": 2,
        "_default": 1,
    }

    def __init__(self, base_meta_path: Path, ex: Executor, db_name: str = None):
        super().__init__(base_meta_path, ex, db_name=db_name)
        self._fixed_rootID = None
        self._fixed_id2_t = None
        self._fixed_id2_f = None

    def _dewey_to_plain_id(self, dewey_string_id: str) -> int | None:
        """Query AGE to get the __id__ property of the node with the given string_id."""
        graph = self.db_name or self.graph_name
        query = f"""SELECT __id__::bigint
FROM cypher('{graph}', $$
    MATCH (n:{self.node_name})
    WHERE n.string_id = '{dewey_string_id}'
    RETURN n.__id__
$$) AS (__id__ agtype);"""
        _, result = self.ex.execute_query(query)
        return result[0][0] if result else None

    def _plain_id_to_dewey(self, target_id: int) -> str | None:
        """Query AGE to get the string_id property of the node with the given __id__."""
        graph = self.db_name or self.graph_name
        query = f"""SELECT TRIM('"' FROM string_id::text)
FROM cypher('{graph}', $$
    MATCH (n:{self.node_name})
    WHERE n.__id__ = {target_id}
    RETURN n.string_id
$$) AS (string_id agtype);"""
        _, result = self.ex.execute_query(query)
        return result[0][0] if result else None

    def set_metadata(self, metadata_name: str):
        super().set_metadata(metadata_name)

        if "s1" in self.graph_name:
            self._fixed_rootID = self._FIXED_ROOTS["s1"]
        elif "s2" in self.graph_name:
            self._fixed_rootID = self._FIXED_ROOTS["s2"]
        elif "s3" in self.graph_name:
            self._fixed_rootID = self._FIXED_ROOTS["s3"]
        elif "artificial_forest" in self.graph_name:
            self._fixed_rootID = self._FIXED_ROOTS["artificial_forest"]
        else:
            self._fixed_rootID = self._FIXED_ROOTS["_default"]

        if self.current_meta is None:
            return

        dewey_roots = self.current_meta["roots"]
        dewey_ids = self.current_meta["id_list"]

        # Find the dewey string_id of the fixed root by querying __id__ property
        fixed_dewey_root = self._plain_id_to_dewey(self._fixed_rootID)

        if fixed_dewey_root is None:
            print(f"Warning: could not find dewey root for __id__={self._fixed_rootID} in {self.graph_name}")
            return

        # TRUE pair id2: deepest leaf in root's subtree
        root_prefix = fixed_dewey_root + "."
        tree1_dewey = [d for d in dewey_ids if d == fixed_dewey_root or d.startswith(root_prefix)]
        if tree1_dewey:
            deepest_dewey = max(tree1_dewey, key=lambda x: x.count("."))
            self._fixed_id2_t = self._dewey_to_plain_id(deepest_dewey)
        else:
            self._fixed_id2_t = self._fixed_rootID

        # FALSE pair id2: second root (for forests) or same as TRUE id2
        if len(dewey_roots) > 1:
            self._fixed_id2_f = self._dewey_to_plain_id(dewey_roots[1])
        else:
            self._fixed_id2_f = self._fixed_id2_t

    def sample_n(self, n: int):
        parameter_sample = []
        for _ in range(n):
            parameter_sample.append({
                "$rootID": self._fixed_rootID,
                "$id1_t": self._fixed_rootID,
                "$id2_t": self._fixed_id2_t,
                "$id1_f": self._fixed_rootID,
                "$id2_f": self._fixed_id2_f,
                "$nodeID": self._fixed_id2_t,
                "$REL_TYPE": self.relation_name,
                "$NODE_TYPE": self.node_name,
            })
        return parameter_sample
