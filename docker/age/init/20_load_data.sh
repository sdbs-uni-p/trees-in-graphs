#!/usr/bin/env bash
# SPDX-License-Identifier: GPL-3.0-only

set -euo pipefail

# Usage:
#   20_load_data.sh
#
# Loads node/edge CSVs for all graphs already created in PostgreSQL.
# Optional env vars:
#   DATA_ROOT       Base directory to scan (default: /data/prepared)

DATA_ROOT="${DATA_ROOT:-/data/prepared}"
INIT_ROOT="${INIT_ROOT:-/docker-entrypoint-initdb.d}"
SQL_FILE="${INIT_ROOT}/sql_scripts/20_load_data.sql"

if [[ ! -f "$SQL_FILE" ]]; then
  echo "ERROR: SQL file not found: $SQL_FILE" >&2
  exit 1
fi

echo "[20-load] start DATA_ROOT=$DATA_ROOT SQL_FILE=$SQL_FILE"

strip_numbers() {
  echo "$1" | sed -E 's/([._-]?[0-9]+)+$//g'
}

node_label_from_file() {
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

edge_label_from_file() {
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

trim() {
  local s="$1"
  s="${s//$'\r'/}"
  s="${s#${s%%[![:space:]]*}}"
  s="${s%${s##*[![:space:]]}}"
  printf "%s" "$s"
}

include_node_file_for_age() {
  local filename="$1"
  [[ ! "$filename" =~ _dewey\.csv$ && ! "$filename" =~ _prepost\.csv$ ]]
}

declare -A graph_dir_map
declare -A graph_base_keys
declare -A group_source
declare -A group_members

resolve_suffix() {
  local graph_name="$1"
  if [[ "$graph_name" == *_baseline ]]; then
    echo "_baseline"
    return 0
  fi
  if [[ "$graph_name" == *_dewey ]]; then
    echo "_dewey"
    return 0
  fi
  if [[ "$graph_name" == *_prepost ]]; then
    echo "_prepost"
    return 0
  fi
  return 1
}

resolve_graph_base() {
  local graph_name="$1"
  local suffix stem
  local candidate best=""
  local best_len=0

  suffix="$(resolve_suffix "$graph_name")" || return 1
  stem="${graph_name%$suffix}"

  if [[ -n "${graph_base_keys[$stem]+x}" ]]; then
    echo "$stem"
    return 0
  fi

  for candidate in "${!graph_base_keys[@]}"; do
    if [[ "$stem" == "${candidate}_"* ]]; then
      if (( ${#candidate} > best_len )); then
        best="$candidate"
        best_len=${#candidate}
      fi
    fi
  done

  if [[ -n "$best" ]]; then
    echo "$best"
    return 0
  fi

  return 1
}

load_graph_from_csv() {
  local graph_name="$1"
  local data_dir="$2"
  local node_dir edge_dir
  local file label_name
  local node_files_found=0
  local edge_files_found=0
  local node_files_loaded=0
  local edge_files_loaded=0

  node_dir="$data_dir/nodes"
  edge_dir="$data_dir/edges"

  echo "[20-load] CSV->graph source_graph=$graph_name data_dir=$data_dir via $(basename "$SQL_FILE")"

  for file in "$node_dir"/*.csv; do
    [[ -e "$file" ]] || break
    include_node_file_for_age "$(basename "$file")" || continue
    node_files_found=$((node_files_found + 1))
  done
  for file in "$edge_dir"/*.csv; do
    [[ -e "$file" ]] || break
    edge_files_found=$((edge_files_found + 1))
  done

  echo "[20-load] source_graph=$graph_name discovered node_csv=$node_files_found edge_csv=$edge_files_found"

  for file in "$node_dir"/*.csv; do
    [[ -e "$file" ]] || break
    include_node_file_for_age "$(basename "$file")" || continue

    label_name="$(node_label_from_file "$file")"
    if [[ -z "$label_name" ]]; then
      echo "Skipping node file (empty label): $file"
      continue
    fi
    echo "[20-load] loading node label=$label_name file=$file graph=$graph_name"

    psql -v ON_ERROR_STOP=1 \
      --echo-errors \
      --username "$POSTGRES_USER" \
      --dbname "$POSTGRES_DB" \
      -v graph_name="$graph_name" \
      -v label_kind="v" \
      -v label_name="$label_name" \
      -v file_path="$file" \
      -f "$SQL_FILE"
    node_files_loaded=$((node_files_loaded + 1))
  done

  for file in "$edge_dir"/*.csv; do
    [[ -e "$file" ]] || break

    label_name="$(edge_label_from_file "$file")"
    if [[ -z "$label_name" ]]; then
      echo "Skipping edge file (empty label): $file"
      continue
    fi
    echo "[20-load] loading edge label=$label_name file=$file graph=$graph_name"

    psql -v ON_ERROR_STOP=1 \
      --echo-errors \
      --username "$POSTGRES_USER" \
      --dbname "$POSTGRES_DB" \
      -v graph_name="$graph_name" \
      -v label_kind="e" \
      -v label_name="$label_name" \
      -v file_path="$file" \
      -f "$SQL_FILE"
    edge_files_loaded=$((edge_files_loaded + 1))
  done

  echo "[20-load] loaded source_graph=$graph_name nodes=$node_files_loaded/$node_files_found edges=$edge_files_loaded/$edge_files_found"
}

clone_graph_data() {
  local src_graph="$1"
  local dst_graph="$2"

  [[ "$src_graph" == "$dst_graph" ]] && return 0

  echo "[20-load] clone source_graph=$src_graph target_graph=$dst_graph"
  psql -v ON_ERROR_STOP=1 \
    --echo-errors \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    -v src_graph="$src_graph" \
    -v dst_graph="$dst_graph" \
    <<'SQL'
CREATE TEMP TABLE IF NOT EXISTS _clone_vars (
  src_graph text,
  dst_graph text
);

TRUNCATE _clone_vars;
INSERT INTO _clone_vars (src_graph, dst_graph)
VALUES (
  NULLIF(:'src_graph', ''),
  NULLIF(:'dst_graph', '')
);

DO $$
DECLARE
  src_graph text;
  dst_graph text;
  src_id int;
  dst_id int;
  lbl RECORD;
BEGIN
  SELECT v.src_graph, v.dst_graph
  INTO src_graph, dst_graph
  FROM _clone_vars v
  LIMIT 1;

  IF src_graph IS NULL OR src_graph = '' THEN
    RAISE NOTICE 'source graph variable is empty';
    RETURN;
  END IF;
  IF dst_graph IS NULL OR dst_graph = '' THEN
    RAISE NOTICE 'destination graph variable is empty';
    RETURN;
  END IF;

  SELECT g.graphid INTO src_id FROM ag_catalog.ag_graph g WHERE g.name = src_graph;
  SELECT g.graphid INTO dst_id FROM ag_catalog.ag_graph g WHERE g.name = dst_graph;

  IF src_id IS NULL THEN
    RAISE NOTICE 'source graph not found: %', src_graph;
    RETURN;
  END IF;
  IF dst_id IS NULL THEN
    RAISE NOTICE 'destination graph not found: %', dst_graph;
    RETURN;
  END IF;

  FOR lbl IN
    SELECT l.name, l.kind
    FROM ag_catalog.ag_label l
    WHERE l.graph = src_id
      AND left(l.name, 4) <> '_ag_'
    ORDER BY CASE WHEN l.kind = 'v' THEN 0 ELSE 1 END, l.name
  LOOP
    IF NOT EXISTS (
      SELECT 1
      FROM ag_catalog.ag_label dl
      WHERE dl.graph = dst_id
        AND dl.name = lbl.name
    ) THEN
      IF lbl.kind = 'v' THEN
        PERFORM ag_catalog.create_vlabel(dst_graph, lbl.name);
      ELSE
        PERFORM ag_catalog.create_elabel(dst_graph, lbl.name);
      END IF;
    END IF;

    EXECUTE format('TRUNCATE TABLE ONLY %I.%I', dst_graph, lbl.name);
    EXECUTE format('INSERT INTO %I.%I SELECT * FROM %I.%I', dst_graph, lbl.name, src_graph, lbl.name);
    EXECUTE format('ANALYZE %I.%I', dst_graph, lbl.name);
  END LOOP;
END $$;
SQL
}

while IFS= read -r -d '' nodes_dir; do
  graph_dir="$(dirname "$nodes_dir")"
  if [[ ! -d "$graph_dir/edges" ]]; then
    continue
  fi

  relpath="${graph_dir#${DATA_ROOT%/}/}"
  if [[ "$relpath" == "$graph_dir" ]]; then
    relpath="${graph_dir#/}"
  fi
  graph_base="${relpath//\//_}"

  graph_dir_map["$graph_base"]="$graph_dir"
  graph_base_keys["$graph_base"]=1

done < <(find "$DATA_ROOT" -type d -name nodes -print0)

mapfile -t graphs < <(
  psql -v ON_ERROR_STOP=1 --echo-errors \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    -At -c "SELECT name FROM ag_catalog.ag_graph WHERE name NOT LIKE '\\_ag\\_%' ORDER BY name;"
)

if [[ ${#graphs[@]} -eq 0 ]]; then
  echo "No graphs found. Nothing to load."
  exit 0
fi

echo "[20-load] discovered ${#graphs[@]} graph schemas in PostgreSQL"

total_groups=0
total_groups_skipped=0
total_groups_loaded=0
total_graph_clones=0

for graph_name in "${graphs[@]}"; do
  suffix="$(resolve_suffix "$graph_name" || true)"
  if [[ -z "$suffix" ]]; then
    echo "Skipping $graph_name (suffix not recognized)"
    continue
  fi

  base="$(resolve_graph_base "$graph_name" || true)"
  if [[ -z "$base" ]]; then
    echo "Skipping $graph_name (no matching data directory for base graph)"
    continue
  fi

  group_key="$base"

  if [[ -z "${group_source[$group_key]-}" ]]; then
    group_source["$group_key"]="$graph_name"
  fi

  group_members["$group_key"]+="$graph_name"$'\n'
done

for group_key in "${!group_members[@]}"; do
  total_groups=$((total_groups + 1))
  base="$group_key"
  source_graph="${group_source[$group_key]}"
  graph_dir="${graph_dir_map[$base]-}"
  loaded_count=0
  cloned_count=0
  members_csv=""
  clone_targets_csv=""

  if [[ -z "$graph_dir" ]]; then
    echo "Skipping group $group_key (no data directory found)"
    total_groups_skipped=$((total_groups_skipped + 1))
    continue
  fi

  node_dir="$graph_dir/nodes"
  edge_dir="$graph_dir/edges"
  if [[ ! -d "$node_dir" || ! -d "$edge_dir" ]]; then
    echo "Skipping group $group_key (missing nodes/edges in $graph_dir)"
    total_groups_skipped=$((total_groups_skipped + 1))
    continue
  fi

  member_count=0
  while IFS= read -r member_graph; do
    member_graph="$(trim "$member_graph")"
    [[ -z "$member_graph" ]] && continue
    member_count=$((member_count + 1))
    if [[ -z "$members_csv" ]]; then
      members_csv="$member_graph"
    else
      members_csv="$members_csv,$member_graph"
    fi
    if [[ "$member_graph" != "$source_graph" ]]; then
      if [[ -z "$clone_targets_csv" ]]; then
        clone_targets_csv="$member_graph"
      else
        clone_targets_csv="$clone_targets_csv,$member_graph"
      fi
    fi
  done <<< "${group_members[$group_key]}"

  echo "[20-load] group base=$group_key graph_dir=$graph_dir source=$source_graph members=$member_count"
  echo "[20-load] group_members base=$group_key all=[$members_csv] clone_targets=[${clone_targets_csv:-none}]"
  load_graph_from_csv "$source_graph" "$graph_dir"
  loaded_count=1
  total_groups_loaded=$((total_groups_loaded + 1))

  while IFS= read -r member_graph; do
    member_graph="$(trim "$member_graph")"
    [[ -z "$member_graph" ]] && continue
    [[ "$member_graph" == "$source_graph" ]] && continue
    clone_graph_data "$source_graph" "$member_graph"
    cloned_count=$((cloned_count + 1))
    total_graph_clones=$((total_graph_clones + 1))
  done <<< "${group_members[$group_key]}"

  echo "[20-load] summary base=$group_key loaded=$loaded_count cloned=$cloned_count source=$source_graph"
done

echo "[20-load] totals groups=$total_groups loaded_groups=$total_groups_loaded skipped_groups=$total_groups_skipped cloned_graphs=$total_graph_clones"
echo "[20-load] finished"
