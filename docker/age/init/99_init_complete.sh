#!/usr/bin/env bash
# SPDX-License-Identifier: GPL-3.0-only

set -euo pipefail

MARKER_FILE="${PGDATA:-/var/lib/postgresql/data/pgdata}/.init_complete"

echo "[init] all init scripts finished; writing marker: $MARKER_FILE"
touch "$MARKER_FILE"
echo "[init] initialization complete"
