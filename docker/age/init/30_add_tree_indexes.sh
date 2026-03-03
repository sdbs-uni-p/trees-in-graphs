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
#   TREE_CONFIG_FILE  Tree declaration file name inside each graph folder (default: trees.csv)
#
# Tree config format (CSV, without header):
#   tree_name,node_file.csv,edge_file.csv
#
# Rules:
# - Multiple trees can be declared.
# - Each node file and each edge file may appear in at most one tree declaration.
# - If config file is missing or has no declarations, no tree is indexed,
#   except when exactly one node CSV and exactly one edge CSV exist.
INIT_ROOT="${INIT_ROOT:-/docker-entrypoint-initdb.d}"
SQL_FILE="${INIT_ROOT}/sql_scripts/30_add_tree_indexes.sql"
DATA_ROOT="${DATA_ROOT:-/data/prepared}"
TREE_CONFIG_FILE="${TREE_CONFIG_FILE:-trees.csv}"

print_usage() {
  echo "Usage: 30_add_tree_indexes.sh [GRAPH_NAME...]"
  echo "  GRAPH_NAME  Optional. One or more graph schema names to process."
  echo "  GRAPH_SUFFIXES Optional env var. Comma-separated suffix list."
  echo "  GRAPH_FILTER   Optional env var. Regex filter for psql graph list."
  echo "  DATA_ROOT      Optional env var. Base dir containing graph data folders."
  echo "  TREE_CONFIG_FILE Optional env var. Tree declaration file name (default: trees.csv)."
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  print_usage
  exit 0
fi

if [[ ! -f "$SQL_FILE" ]]; then
  echo "ERROR: SQL file not found: $SQL_FILE" >&2
  exit 1
fi

echo "[30-index] start DATA_ROOT=$DATA_ROOT TREE_CONFIG_FILE=$TREE_CONFIG_FILE SQL_FILE=$SQL_FILE"

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
  local base tree_name graph_dir node_dir edge_dir config_file
  local resolved
  local -a node_files edge_files
  local line a b c tree_name_cfg node_file edge_file node_label edge_label
  local -A used_node_files used_edge_files

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
  config_file="$graph_dir/$TREE_CONFIG_FILE"

  if [[ ! -d "$node_dir" || ! -d "$edge_dir" ]]; then
    echo "Skipping $graph_name (missing nodes/edges folders in $graph_dir)" >&2
    return 1
  fi

  shopt -s nullglob
  node_files=("$node_dir"/*.csv)
  edge_files=("$edge_dir"/*.csv)
  shopt -u nullglob

  used_node_files=()
  used_edge_files=()

  if config_has_declarations "$config_file"; then
    while IFS= read -r line || [[ -n "$line" ]]; do
      line="$(trim "$line")"
      [[ -z "$line" ]] && continue
      [[ "$line" == \#* ]] && continue

      IFS=',' read -r a b c _ <<< "$line"
      a="$(trim "$a")"
      b="$(trim "$b")"
      c="$(trim "$c")"

      tree_name_cfg="$(normalize_tree_name "$a")"
      node_file="$b"
      edge_file="$c"

      if [[ -z "$tree_name_cfg" || -z "$node_file" || -z "$edge_file" ]]; then
        echo "Skipping malformed tree declaration in $config_file: $line" >&2
        continue
      fi

      if [[ -n "$tree_name" && "$tree_name_cfg" != "$tree_name" ]]; then
        continue
      fi

      node_file="$(trim "$node_file")"
      edge_file="$(trim "$edge_file")"

      if [[ -z "$node_file" || -z "$edge_file" ]]; then
        echo "Skipping malformed tree declaration in $config_file: $line" >&2
        continue
      fi

      if [[ -n "${used_node_files[$node_file]-}" ]]; then
        echo "Skipping duplicate node file in $config_file: $node_file" >&2
        continue
      fi
      if [[ -n "${used_edge_files[$edge_file]-}" ]]; then
        echo "Skipping duplicate edge file in $config_file: $edge_file" >&2
        continue
      fi

      if [[ ! -f "$node_dir/$node_file" ]]; then
        echo "Skipping declaration (node file not found): $node_dir/$node_file" >&2
        continue
      fi
      if [[ ! -f "$edge_dir/$edge_file" ]]; then
        echo "Skipping declaration (edge file not found): $edge_dir/$edge_file" >&2
        continue
      fi

      node_label="$(node_label_from_file "$node_file")"
      edge_label="$(edge_label_from_file "$edge_file")"

      if [[ -z "$node_label" || -z "$edge_label" ]]; then
        echo "Skipping declaration (empty derived label): $line" >&2
        continue
      fi

      if ! table_exists "$graph_name" "$node_label"; then
        echo "Skipping declaration (node table not found): $graph_name.$node_label" >&2
        continue
      fi
      if ! table_exists "$graph_name" "$edge_label"; then
        echo "Skipping declaration (edge table not found): $graph_name.$edge_label" >&2
        continue
      fi

      used_node_files["$node_file"]=1
      used_edge_files["$edge_file"]=1
      echo "[30-index] tree match graph=$graph_name tree=$tree_name_cfg node_file=$node_file edge_file=$edge_file" >&2
      echo "$node_label|$edge_label"
    done < "$config_file"

    if [[ -n "$tree_name" ]]; then
      return 0
    fi

    return 0
  fi

  # No declaration file (or no declarations): only allow the unambiguous single-file fallback.
  if [[ ${#node_files[@]} -eq 1 && ${#edge_files[@]} -eq 1 ]]; then
    node_label="$(node_label_from_file "$(basename "${node_files[0]}")")"
    edge_label="$(edge_label_from_file "$(basename "${edge_files[0]}")")"

    if [[ -z "$node_label" || -z "$edge_label" ]]; then
      echo "Skipping $graph_name (could not derive labels from single node/edge CSV files)" >&2
      return 1
    fi

    if ! table_exists "$graph_name" "$node_label"; then
      echo "Skipping $graph_name (single node table not found): $graph_name.$node_label" >&2
      return 1
    fi
    if ! table_exists "$graph_name" "$edge_label"; then
      echo "Skipping $graph_name (single edge table not found): $graph_name.$edge_label" >&2
      return 1
    fi

    echo "$node_label|$edge_label"
    return 0
  fi

  echo "No tree declarations for $graph_name and no unique 1-node/1-edge fallback. Skipping." >&2
  return 1
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
