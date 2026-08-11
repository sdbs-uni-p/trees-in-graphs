#!/usr/bin/env bash
# SPDX-License-Identifier: GPL-3.0-only

set -euo pipefail

# Usage:
#   10_create_graphs.sh [GRAPH_PATH ...]
#
# If GRAPH_PATH arguments are provided, only those graph directories are processed.
# Otherwise, DATA_PATHS/DATA_PATH can be used to select graph directories.
# If neither is provided, DATA_ROOT is processed. In this container the default
# is the SF1 graph directory: /data/prepared/snb/sf1.
# Optional env vars:
#   DATA_ROOT       Base directory to scan (default: /data/prepared/snb/sf1)
#   DATA_PATH       Single graph path used when no CLI argument is provided (legacy)
#   DATA_PATHS      Space-separated list of graph paths or globs (preferred)
#   GRAPH_SUFFIXES  Comma-separated suffix list for raw graphs
#                  (default: _baseline,_dewey,_prepost)

DATA_ROOT="${DATA_ROOT:-/data/prepared/snb/sf1}"
DATA_PATHS="${DATA_PATHS:-${DATA_PATH:-}}"

print_usage() {
  echo "Usage: 10_create_graphs.sh [GRAPH_PATH ...]"
  echo "  GRAPH_PATH  Optional. One or more graph directories (supports globs)."
  echo "  DATA_ROOT       Optional env var. Base directory to scan (default: /data/prepared/snb/sf1)."
  echo "  DATA_PATH       Optional env var. Single graph path if no CLI argument is given."
  echo "  DATA_PATHS      Optional env var. Space-separated graph paths or globs."
  echo "  GRAPH_SUFFIXES  Optional env var. Comma-separated suffix list for raw graphs."
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  print_usage
  exit 0
fi

INIT_ROOT="${INIT_ROOT:-/docker-entrypoint-initdb.d}"
SQL_FILE="${INIT_ROOT}/sql_scripts/10_create_graph_schema.sql"

if [[ ! -f "$SQL_FILE" ]]; then
  echo "ERROR: SQL file not found: $SQL_FILE" >&2
  exit 1
fi

echo "[10-create] start DATA_ROOT=$DATA_ROOT SQL_FILE=$SQL_FILE"

IFS=',' read -r -a GRAPH_SUFFIXES_ARR <<< "${GRAPH_SUFFIXES:-_baseline,_dewey,_prepost}"
unset IFS

process_graph_dir() {
  local graph_dir="$1"
  local relpath graph_base suffix

  if [[ ! -d "$graph_dir/nodes" || ! -d "$graph_dir/edges" ]]; then
    return
  fi

  relpath="${graph_dir#${DATA_ROOT%/}/}"
  if [[ "$relpath" == "$graph_dir" ]]; then
    relpath="${graph_dir#/}"
  fi
  graph_base="${relpath//\//_}"

  echo "[10-create] graph_dir=$graph_dir graph_base=$graph_base"

  for suffix in "${GRAPH_SUFFIXES_ARR[@]}"; do
    echo "[10-create] creating graph=${graph_base}${suffix} source_dir=$graph_dir via $(basename "$SQL_FILE")"
    psql -v ON_ERROR_STOP=1 \
      --echo-errors \
      --username "$POSTGRES_USER" \
      --dbname "$POSTGRES_DB" \
      -v graph_path="$graph_dir" \
      -v graph_name="${graph_base}${suffix}" \
      -f "$SQL_FILE"
  done
}

if [[ $# -ge 1 ]]; then
  for pattern in "$@"; do
    for graph_dir in $pattern; do
      process_graph_dir "$graph_dir"
    done
  done
  exit 0
fi

if [[ -n "$DATA_PATHS" ]]; then
  for pattern in $DATA_PATHS; do
    for graph_dir in $pattern; do
      process_graph_dir "$graph_dir"
    done
  done
  exit 0
fi

if [[ -d "$DATA_ROOT/nodes" && -d "$DATA_ROOT/edges" ]]; then
  process_graph_dir "$DATA_ROOT"
  exit 0
fi

if [[ ! -d "$DATA_ROOT" ]]; then
  echo "ERROR: DATA_ROOT does not exist: $DATA_ROOT" >&2
  exit 1
fi

declare -A seen
while IFS= read -r -d '' nodes_dir; do
  graph_dir="$(dirname "$nodes_dir")"
  if [[ -d "$graph_dir/edges" ]]; then
    if [[ -z "${seen[$graph_dir]+x}" ]]; then
      seen[$graph_dir]=1
      process_graph_dir "$graph_dir"
    fi
  fi
done < <(find "$DATA_ROOT" -type d -name nodes -print0)

echo "[10-create] finished"
