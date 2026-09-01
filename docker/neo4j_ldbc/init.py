#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-only
"""Create the three complete SNB SF1 databases used by LDBC queries."""

import base_init
from neo4j import GraphDatabase


def main():
    driver = GraphDatabase.driver(
        base_init.NEO4J_URI,
        auth=(base_init.NEO4J_USER, base_init.NEO4J_PASSWORD),
    )
    try:
        for annotation in base_init.ANNOTATION_TYPES:
            base_init.create_neo4j_snb_database(
                driver,
                None,
                annotation,
                graph_name=f"snb_sf1_{base_init.graph_variant(annotation)}",
                annotated_labels=base_init.TREE_ANNOTATED_LABELS,
            )
    finally:
        driver.close()


if __name__ == "__main__":
    main()
