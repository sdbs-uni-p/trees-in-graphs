#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-only
"""Create the three complete SNB SF1 databases used by LDBC queries."""

import tempfile

import base_init


def main():
    labels = base_init.TREE_ANNOTATED_LABELS
    metadata = base_init.determine_depth_metadata([])
    base_init.write_depth_metadata(metadata)
    with tempfile.TemporaryDirectory(prefix="kuzu-ldbc-") as temporary:
        for annotation in base_init.ANNOTATION_TYPES:
            base_init.create_snb_kuzu_database(
                None,
                annotation,
                temporary,
                graph_name=f"snb_sf1_{base_init.graph_variant(annotation)}",
                annotated_labels=labels,
            )


if __name__ == "__main__":
    main()
