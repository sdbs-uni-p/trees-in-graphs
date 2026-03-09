#!/usr/bin/env bash
# SPDX-License-Identifier: GPL-3.0-only

set -euo pipefail

# Usage:
#   10_create_graphs.sh [GRAPH_PATH ...]
#
# If GRAPH_PATH arguments are provided, only those graphs are loaded.
# Otherwise, DATA_PATHS/DATA_PATH can be used to select graphs.
# If neither is provided, all graphs under DATA_ROOT (default: /data/prepared) are processed.
# Optional env vars:
#   DATA_ROOT       Base directory to scan (default: /data/prepared)
#   DATA_PATH       Single graph path used when no CLI argument is provided (legacy)
#   DATA_PATHS      Space-separated list of graph paths or globs (preferred)
#   GRAPH_SUFFIXES  Comma-separated suffix list for raw graphs
#                  (default: _baseline,_dewey,_prepost)

DATA_ROOT="${DATA_ROOT:-/data/prepared}"
DATA_PATHS="${DATA_PATHS:-${DATA_PATH:-}}"

print_usage() {
  echo "Usage: 10_create_graphs.sh [GRAPH_PATH ...]"
  echo "  GRAPH_PATH  Optional. One or more graph directories (supports globs)."
  echo "  DATA_ROOT       Optional env var. Base directory to scan (default: /data/prepared)."
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

trim() {
  local s="$1"
  s="${s//$'\r'/}"
  s="${s#${s%%[![:space:]]*}}"
  s="${s%${s##*[![:space:]]}}"
  printf "%s" "$s"
}

normalize_tree_name() {
  local raw="$1"
  local normalized
  normalized="$(echo "$raw" | tr '[:upper:]' '[:lower:]')"
  normalized="${normalized// /_}"
  normalized="$(echo "$normalized" | sed -E 's/[^a-z0-9_]+/_/g; s/_+/_/g; s/^_+//; s/_+$//')"
  printf "%s" "$normalized"
}

strip_numbers() {
  echo "$1" | sed -E 's/([._-]?[0-9]+)+$//g'
}

node_label_from_file() {
  local filename="$1"
  local base
  local label_raw

  base="$(basename "$filename")"
  base="$(echo "$base" | sed -E 's/\.[cC][sS][vV]$//')"
  base="$(echo "$base" | sed -E 's/_(baseline|dewey|prepost|plain)$//')"

  label_raw="$(strip_numbers "$base")"
  label_raw="$(strip_numbers "$label_raw")"

  if [[ -z "$label_raw" ]]; then
    echo ""
    return
  fi

  echo "${label_raw^}"
}

collect_tree_names() {
  local graph_dir="$1"
  local node_dir="$graph_dir/nodes"
  local plain_file plain_name label normalized
  local -A seen=()

  shopt -s nullglob
  for plain_file in "$node_dir"/*.csv; do
    plain_name="$(basename "$plain_file")"

    # AGE uses only plain files, but tree labels are detected by companion dewey/prepost files.
    if [[ "$plain_name" =~ _dewey\.csv$ || "$plain_name" =~ _prepost\.csv$ ]]; then
      continue
    fi

    if [[ ! -f "$node_dir/${plain_name%.csv}_dewey.csv" ]]; then
      continue
    fi
    if [[ ! -f "$node_dir/${plain_name%.csv}_prepost.csv" ]]; then
      continue
    fi

    label="$(node_label_from_file "$plain_name")"
    [[ -z "$label" ]] && continue

    normalized="$(normalize_tree_name "$label")"
    [[ -z "$normalized" ]] && continue

    if [[ -z "${seen[$normalized]+x}" ]]; then
      seen[$normalized]=1
      echo "$normalized"
    fi
  done
  shopt -u nullglob
}

function process_graph_dir() {
  local graph_dir="$1"
  local node_dir edge_dir
  local relpath graph_base
  local suffix
  local -a tree_names

  node_dir="$graph_dir/nodes"
  edge_dir="$graph_dir/edges"

  if [[ ! -d "$node_dir" || ! -d "$edge_dir" ]]; then
    return
  fi

  relpath="${graph_dir#${DATA_ROOT%/}/}"
  if [[ "$relpath" == "$graph_dir" ]]; then
    relpath="${graph_dir#/}"
  fi
  graph_base="${relpath//\//_}"

  echo "[10-create] graph_dir=$graph_dir graph_base=$graph_base"

  mapfile -t tree_names < <(collect_tree_names "$graph_dir")
  if [[ ${#tree_names[@]} -gt 0 ]]; then
    echo "[10-create] tree names: ${tree_names[*]}"
  fi

  if [[ ${#tree_names[@]} -gt 0 ]]; then
    local tree_name
    for tree_name in "${tree_names[@]}"; do
      for suffix in "${GRAPH_SUFFIXES_ARR[@]}"; do
        echo "[10-create] creating graph=${graph_base}_${tree_name}${suffix} source_dir=$graph_dir via $(basename "$SQL_FILE")"
        psql -v ON_ERROR_STOP=1 \
          --echo-errors \
          --username "$POSTGRES_USER" \
          --dbname "$POSTGRES_DB" \
          -v graph_path="$graph_dir" \
          -v graph_name="${graph_base}_${tree_name}${suffix}" \
          -f "$SQL_FILE"
      done
    done
    return
  fi

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
