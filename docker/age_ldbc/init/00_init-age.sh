#!/usr/bin/env bash
# SPDX-License-Identifier: GPL-3.0-only

set -euo pipefail

echo "Initializing AGE extension..."

INIT_ROOT="${INIT_ROOT:-/docker-entrypoint-initdb.d}"
SQL_FILE="${INIT_ROOT}/sql_scripts/00_age_setup.sql"
if [[ ! -f "$SQL_FILE" ]]; then
  echo "ERROR: SQL file not found: $SQL_FILE"
  exit 1
fi

psql -v ON_ERROR_STOP=1 \
  --echo-errors \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  -f "$SQL_FILE"

echo "AGE extension loaded successfully"
