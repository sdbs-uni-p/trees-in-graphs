#!/bin/bash
# SPDX-License-Identifier: GPL-3.0-only

set -e

# Run init if databases have not been created yet
if [ ! -f /app/.initialized ]; then
    echo "Initializing Neo4j databases..."
    python /app/init/00_init_neo4j.py
    touch /app/.initialized
    echo "Neo4j databases initialized successfully."
else
    echo "Neo4j databases already initialized (found /app/.initialized)."
fi

echo "Container ready. Keeping alive for experiment execution..."
tail -f /dev/null
