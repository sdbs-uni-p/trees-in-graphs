#!/usr/bin/env bash
# SPDX-License-Identifier: GPL-3.0-only
set -euo pipefail

if [ ! -f /app/.initialized ]; then
    echo "Initializing Neo4j LDBC databases..."
    python -u /app/init.py
    touch /app/.initialized
    echo "Neo4j LDBC databases initialized successfully."
else
    echo "Neo4j LDBC databases already initialized (found /app/.initialized)."
fi
echo "Container ready. Keeping alive for experiment execution..."
tail -f /dev/null
