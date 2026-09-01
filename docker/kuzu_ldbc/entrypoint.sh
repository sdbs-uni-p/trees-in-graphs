#!/usr/bin/env bash
# SPDX-License-Identifier: GPL-3.0-only
set -euo pipefail

chmod -R a+rwX /kuzu_data || true
if [ ! -f /kuzu_data/.initialized ]; then
    echo "Initializing Kuzu LDBC databases..."
    python -u /app/init.py
    touch /kuzu_data/.initialized
    echo "Kuzu LDBC databases initialized successfully."
else
    echo "Kuzu LDBC databases already initialized (found /kuzu_data/.initialized)."
fi
chmod -R a+rwX /kuzu_data || true
echo "Container ready. Keeping alive for experiment execution..."
tail -f /dev/null
