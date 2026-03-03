#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   30_add_tree_indexes.sh [GRAPH_NAME...]
#
# If GRAPH_NAME arguments are provided, only those graphs are processed.
# Otherwise, graph names are derived from PostgreSQL.
#
# Optional env vars:
#   GRAPH_SUFFIXES Comma-separated suffix list (default: _baseline,_dewey,_prepost)
#   GRAPH_FILTER   Optional regex to filter graph names (applied to psql graph list)
#   DATA_ROOT      Base directory with prepared graph folders (default: /data/prepared)
#
# Tree detection rules (no trees.csv required):
# - A tree label is detected by a node-file triad in nodes/:
#   <label>.csv, <label>_dewey.csv, <label>_prepost.csv
# - AGE always uses the plain node CSV (<label>.csv); dewey/prepost values are derived in DB.
# - The corresponding edge file is identified by CSV content where
#   start_vertex_type == end_vertex_type == <NodeLabel>.
INIT_ROOT="${INIT_ROOT:-/docker-entrypoint-initdb.d}"
SQL_FILE="${INIT_ROOT}/sql_scripts/30_add_tree_indexes.sql"
DATA_ROOT="${DATA_ROOT:-/data/prepared}"

print_usage() {
  echo "Usage: 30_add_tree_indexes.sh [GRAPH_NAME...]"
  echo "  GRAPH_NAME  Optional. One or more graph schema names to process."
  echo "  GRAPH_SUFFIXES Optional env var. Comma-separated suffix list."
  echo "  GRAPH_FILTER   Optional env var. Regex filter for psql graph list."
  echo "  DATA_ROOT      Optional env var. Base dir containing graph data folders."
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  print_usage
  exit 0
fi

if [[ ! -f "$SQL_FILE" ]]; then
  echo "ERROR: SQL file not found: $SQL_FILE" >&2
  exit 1
fi

echo "[30-index] start DATA_ROOT=$DATA_ROOT SQL_FILE=$SQL_FILE"

declare -a graph_names
declare -A graph_dir_map

function add_graph_name() {
  local name="$1"
  if [[ -z "$name" ]]; then
    return
  fi
  graph_names+=("$name")
}

function collect_from_psql() {
  local regex="${GRAPH_FILTER:-(_baseline|_dewey|_prepost)$}"
  mapfile -t psql_graphs < <(
    psql -v ON_ERROR_STOP=1 --echo-errors \
      --username "$POSTGRES_USER" \
      --dbname "$POSTGRES_DB" \
      -At -c "SELECT name FROM ag_catalog.ag_graph WHERE name ~ '$regex' ORDER BY name"
  )

  for g in "${psql_graphs[@]}"; do
    add_graph_name "$g"
  done
}

function sql_quote() {
  local s="$1"
  s=${s//\'/\'\'}
  printf "%s" "$s"
}

function trim() {
  local s="$1"
  s="${s//$'\r'/}"
  s="${s#${s%%[![:space:]]*}}"
  s="${s%${s##*[![:space:]]}}"
  printf "%s" "$s"
}

function strip_numbers() {
  echo "$1" | sed -E 's/([._-]?[0-9]+)+$//g'
}

function normalize_tree_name() {
  local raw="$1"
  local normalized

  normalized="$(echo "$raw" | tr '[:upper:]' '[:lower:]')"
  normalized="${normalized// /_}"
  normalized="$(echo "$normalized" | sed -E 's/[^a-z0-9_]+/_/g; s/_+/_/g; s/^_+//; s/_+$//')"
  printf "%s" "$normalized"
}

function node_label_from_file() {
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

function edge_label_from_file() {
  local filename="$1"
  local base
  local label_raw

  base="$(basename "$filename")"
  base="$(echo "$base" | sed -E 's/\.[cC][sS][vV]$//')"
  label_raw="$(strip_numbers "$base")"

  if [[ -z "$label_raw" ]]; then
    echo ""
    return
  fi

  echo "$label_raw"
}

function edge_file_matches_node_label() {
  local edge_file="$1"
  local node_label="$2"
  local raw
  local start_type
  local end_type

  raw="$(awk -F',' '
    NR==1 {
      for (i = 1; i <= NF; i++) {
        col = $i
        gsub(/^[ \t\r\"]+|[ \t\r\"]+$/, "", col)
        if (col == "start_vertex_type") s = i
        if (col == "end_vertex_type") e = i
      }
      next
    }
    NR > 1 {
      if (!s || !e) next
      sv = $s
      ev = $e
      gsub(/^[ \t\r\"]+|[ \t\r\"]+$/, "", sv)
      gsub(/^[ \t\r\"]+|[ \t\r\"]+$/, "", ev)
      print sv "|" ev
      exit
    }
  ' "$edge_file")"

  [[ -z "$raw" ]] && return 1
  start_type="${raw%%|*}"
  end_type="${raw##*|}"
  [[ "$start_type" == "$node_label" && "$end_type" == "$node_label" ]]
}

function graph_stem_from_name() {
  local name="$1"
  local base="$name"
  base="${base%_baseline}"
  base="${base%_dewey}"
  base="${base%_prepost}"
  echo "$base"
}

function resolve_graph_base_and_tree() {
  local graph_name="$1"
  local stem
  local candidate best_base=""
  local best_len=0

  stem="$(graph_stem_from_name "$graph_name")"

  if [[ -n "${graph_dir_map[$stem]-}" ]]; then
    echo "$stem|"
    return 0
  fi

  for candidate in "${!graph_dir_map[@]}"; do
    if [[ "$stem" == "${candidate}_"* ]]; then
      if (( ${#candidate} > best_len )); then
        best_base="$candidate"
        best_len=${#candidate}
      fi
    fi
  done

  if [[ -z "$best_base" ]]; then
    return 1
  fi

  local tree_name_part
  tree_name_part="${stem#${best_base}_}"
  tree_name_part="$(normalize_tree_name "$tree_name_part")"
  echo "$best_base|$tree_name_part"
}

function build_graph_dir_map() {
  graph_dir_map=()
  while IFS= read -r -d '' nodes_dir; do
    local graph_dir relpath graph_base
    graph_dir="$(dirname "$nodes_dir")"
    [[ -d "$graph_dir/edges" ]] || continue

    relpath="${graph_dir#${DATA_ROOT%/}/}"
    if [[ "$relpath" == "$graph_dir" ]]; then
      relpath="${graph_dir#/}"
    fi
    graph_base="${relpath//\//_}"
    graph_dir_map["$graph_base"]="$graph_dir"
  done < <(find "$DATA_ROOT" -type d -name nodes -print0)
}

function config_has_declarations() {
  local config_file="$1"
  [[ -f "$config_file" ]] || return 1

  while IFS= read -r line || [[ -n "$line" ]]; do
    line="$(trim "$line")"
    [[ -z "$line" ]] && continue
    [[ "$line" == \#* ]] && continue
    return 0
  done < "$config_file"

  return 1
}

function table_exists() {
  local graph_name="$1"
  local table_name="$2"
  local graph_quoted table_quoted

  graph_quoted=$(sql_quote "$graph_name")
  table_quoted=$(sql_quote "$table_name")

  local found
  found=$(psql -v ON_ERROR_STOP=1 --echo-errors \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    -At \
    -c "
      SELECT 1
      FROM information_schema.tables t
      WHERE t.table_schema = '$graph_quoted'
        AND t.table_name = '$table_quoted'
      LIMIT 1;
    "
  )

  [[ "$found" == "1" ]]
}

function resolve_tree_table_pairs_for_graph() {
  local graph_name="$1"
  local base tree_name graph_dir node_dir edge_dir
  local resolved
  local -a node_files edge_files
  local node_file edge_file node_name node_label edge_label family_base
  local -a matching_edges
  local lower_tree_name
  local -A plain_node_files dewey_node_files prepost_node_files

  resolved="$(resolve_graph_base_and_tree "$graph_name" || true)"
  if [[ -z "$resolved" ]]; then
    echo "Skipping $graph_name (no matching data directory found under $DATA_ROOT)" >&2
    return 1
  fi

  base="${resolved%%|*}"
  tree_name="${resolved##*|}"
  graph_dir="${graph_dir_map[$base]-}"

  if [[ -n "$tree_name" ]]; then
    echo "[30-index] graph=$graph_name resolved base=$base tree=$tree_name" >&2
  else
    echo "[30-index] graph=$graph_name resolved base=$base" >&2
  fi

  if [[ -z "$graph_dir" ]]; then
    echo "Skipping $graph_name (no data directory found under $DATA_ROOT)" >&2
    return 1
  fi

  node_dir="$graph_dir/nodes"
  edge_dir="$graph_dir/edges"

  if [[ ! -d "$node_dir" || ! -d "$edge_dir" ]]; then
    echo "Skipping $graph_name (missing nodes/edges folders in $graph_dir)" >&2
    return 1
  fi

  shopt -s nullglob
  node_files=("$node_dir"/*.csv)
  edge_files=("$edge_dir"/*.csv)
  shopt -u nullglob

  plain_node_files=()
  dewey_node_files=()
  prepost_node_files=()

  for node_file in "${node_files[@]}"; do
    node_name="$(basename "$node_file")"
    if [[ "$node_name" =~ ^(.+)_dewey\.csv$ ]]; then
      dewey_node_files["${BASH_REMATCH[1]}"]="$node_name"
    elif [[ "$node_name" =~ ^(.+)_prepost\.csv$ ]]; then
      prepost_node_files["${BASH_REMATCH[1]}"]="$node_name"
    elif [[ "$node_name" =~ ^(.+)\.csv$ ]]; then
      plain_node_files["${BASH_REMATCH[1]}"]="$node_name"
    fi
  done

  lower_tree_name="$(normalize_tree_name "$tree_name")"

  for family_base in "${!plain_node_files[@]}"; do
    if [[ -z "${dewey_node_files[$family_base]-}" || -z "${prepost_node_files[$family_base]-}" ]]; then
      continue
    fi

    node_file="${plain_node_files[$family_base]}"
    node_label="$(node_label_from_file "$node_file")"
    [[ -z "$node_label" ]] && continue

    if [[ -n "$lower_tree_name" ]]; then
      if [[ "$(normalize_tree_name "$node_label")" != "$lower_tree_name" ]]; then
        continue
      fi
    fi

    if ! table_exists "$graph_name" "$node_label"; then
      echo "Skipping label $node_label in $graph_name (node table not found): $graph_name.$node_label" >&2
      continue
    fi

    matching_edges=()
    for edge_file in "${edge_files[@]}"; do
      if edge_file_matches_node_label "$edge_file" "$node_label"; then
        matching_edges+=("$edge_file")
      fi
    done

    if [[ ${#matching_edges[@]} -eq 0 ]]; then
      continue
    fi
    if [[ ${#matching_edges[@]} -gt 1 ]]; then
      echo "Skipping label $node_label in $graph_name (multiple matching edge CSVs for $node_label)" >&2
      continue
    fi

    edge_label="$(edge_label_from_file "$(basename "${matching_edges[0]}")")"
    [[ -z "$edge_label" ]] && continue

    if ! table_exists "$graph_name" "$edge_label"; then
      echo "Skipping label $node_label in $graph_name (edge table not found): $graph_name.$edge_label" >&2
      continue
    fi

    echo "[30-index] tree match graph=$graph_name label=$node_label node_file=$node_file edge_file=$(basename "${matching_edges[0]}")" >&2
    echo "$node_label|$edge_label"
  done

  return 0
}

function resolve_tables() {
  local graph_name="$1"
  local node_table edge_table
  local schema_quoted

  schema_quoted=$(sql_quote "$graph_name")

  node_table=$(psql -v ON_ERROR_STOP=1 --echo-errors \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    -At \
    -c "
      SELECT t.table_name
      FROM information_schema.tables t
      WHERE t.table_schema = '$schema_quoted'
        AND t.table_type = 'BASE TABLE'
        AND t.table_name NOT LIKE '\\_ag_%'
        AND EXISTS (
          SELECT 1 FROM information_schema.columns c
          WHERE c.table_schema = t.table_schema
            AND c.table_name = t.table_name
            AND c.column_name = 'id'
        )
        AND NOT EXISTS (
          SELECT 1 FROM information_schema.columns c
          WHERE c.table_schema = t.table_schema
            AND c.table_name = t.table_name
            AND c.column_name IN ('start_id', 'end_id')
        )
      ORDER BY t.table_name
      LIMIT 1;
    "
  )

  edge_table=$(psql -v ON_ERROR_STOP=1 --echo-errors \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    -At \
    -c "
      SELECT t.table_name
      FROM information_schema.tables t
      WHERE t.table_schema = '$schema_quoted'
        AND t.table_type = 'BASE TABLE'
        AND t.table_name NOT LIKE '\\_ag_%'
        AND EXISTS (
          SELECT 1 FROM information_schema.columns c
          WHERE c.table_schema = t.table_schema
            AND c.table_name = t.table_name
            AND c.column_name = 'start_id'
        )
        AND EXISTS (
          SELECT 1 FROM information_schema.columns c
          WHERE c.table_schema = t.table_schema
            AND c.table_name = t.table_name
            AND c.column_name = 'end_id'
        )
      ORDER BY t.table_name
      LIMIT 1;
    "
  )

  if [[ -z "$node_table" || -z "$edge_table" ]]; then
    echo "Skipping $graph_name (could not resolve node/edge tables)"
    return 1
  fi

  echo "$node_table|$edge_table"
  return 0
}

if [[ $# -gt 0 ]]; then
  for g in "$@"; do
    add_graph_name "$g"
  done
else
  collect_from_psql
fi

build_graph_dir_map

if [[ ${#graph_names[@]} -eq 0 ]]; then
  echo "No graph names found. Nothing to do."
  exit 0
fi

for graph_name in "${graph_names[@]}"; do
  case "$graph_name" in
    *_baseline)
      index_kind="baseline"
      ;;
    *_dewey)
      index_kind="dewey"
      ;;
    *_prepost)
      index_kind="prepost"
      ;;
    *)
      echo "Skipping $graph_name (suffix not recognized)"
      continue
      ;;
  esac

  mapfile -t tree_pairs < <(resolve_tree_table_pairs_for_graph "$graph_name")
  if [[ ${#tree_pairs[@]} -eq 0 ]]; then
    echo "Skipping $graph_name (no valid tree declarations to index)"
    continue
  fi

  echo "[30-index] graph=$graph_name index_kind=$index_kind tree_pairs=${#tree_pairs[@]}"

  for tables in "${tree_pairs[@]}"; do
    node_table="${tables%%|*}"
    edge_table="${tables##*|}"

    if [[ "$index_kind" == "dewey" ]]; then
      echo "[30-index] apply dewey graph=$graph_name node=$node_table edge=$edge_table via $(basename "$SQL_FILE")"
    elif [[ "$index_kind" == "baseline" ]]; then
      echo "[30-index] apply baseline graph=$graph_name node=$node_table edge=$edge_table via $(basename "$SQL_FILE")"
    else
      echo "[30-index] apply prepost graph=$graph_name node=$node_table edge=$edge_table via $(basename "$SQL_FILE")"
    fi

    psql -v ON_ERROR_STOP=1 \
      --echo-errors \
      --username "$POSTGRES_USER" \
      --dbname "$POSTGRES_DB" \
      -v graph_name="$graph_name" \
      -v node_table="$node_table" \
      -v edge_table="$edge_table" \
      -v index_kind="$index_kind" \
      -f "$SQL_FILE"
  done
done

echo "[30-index] finished"
