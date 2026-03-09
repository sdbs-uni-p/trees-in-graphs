#!/bin/bash
# SPDX-License-Identifier: GPL-3.0-only

set -e

# Run init if databases have not been created yet
if [ ! -f /kuzu_data/.initialized ]; then
    echo "Initializing Kuzu databases..."
    python /app/init/00_init_kuzu.py
    touch /kuzu_data/.initialized
    echo "Kuzu databases initialized successfully."
else
    echo "Kuzu databases already initialized (found /kuzu_data/.initialized)."
fi

echo "Container ready. Keeping alive for experiment execution..."
tail -f /dev/null
