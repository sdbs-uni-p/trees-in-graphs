#!/bin/bash
# SPDX-License-Identifier: GPL-3.0-only

set -euo pipefail

normalize_kuzu_permissions() {
    # Keep /kuzu_data writable for arbitrary docker exec --user values on Linux.
    if [ "$(id -u)" -ne 0 ]; then
        return
    fi

    if [ -d /kuzu_data ]; then
        chmod a+rwX /kuzu_data || true
        chmod -R a+rwX /kuzu_data || true
    fi
}

normalize_kuzu_permissions

# Run init if databases have not been created yet
if [ ! -f /kuzu_data/.initialized ]; then
    echo "Initializing Kuzu databases..."
    python /app/init/00_init_kuzu.py
    touch /kuzu_data/.initialized
    echo "Kuzu databases initialized successfully."
else
    echo "Kuzu databases already initialized (found /kuzu_data/.initialized)."
fi

normalize_kuzu_permissions

echo "Container ready. Keeping alive for experiment execution..."
tail -f /dev/null
