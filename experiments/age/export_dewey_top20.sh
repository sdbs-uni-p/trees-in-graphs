#!/bin/bash
# SPDX-License-Identifier: GPL-3.0-only

set -euo pipefail

usage() {
	cat <<'EOF'
Usage: export_dewey_top20.sh [OPTIONS]

Exports eight Top-20 reports for every AGE graph whose name ends in _dewey.

Options:
	-d, --datasets LIST  Comma-separated graph/base-name filters (globs allowed).
	-o, --output DIR     Output directory (default: /results/age/dewey_top20).
	-h, --help           Show this help.

Environment variables:
	POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
	DATASET_FILTER       Same as --datasets; CLI takes precedence.
	OUTPUT_DIR           Same as --output; CLI takes precedence.
EOF
}

DATASET_FILTER="${DATASET_FILTER:-}"
OUTPUT_DIR="${OUTPUT_DIR:-/results/age/dewey_top20}"

while [[ $# -gt 0 ]]; do
	case "$1" in
		-d|--datasets)
			DATASET_FILTER="${2:-}"
			shift 2
			;;
		-o|--output)
			OUTPUT_DIR="${2:-}"
			shift 2
			;;
		-h|--help)
			usage
			exit 0
			;;
		*)
			echo "Unknown argument: $1" >&2
			usage >&2
			exit 1
			;;
	esac
done

POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_DB="${POSTGRES_DB:-postgresDB}"
POSTGRES_USER="${POSTGRES_USER:-postgresUser}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-postgresPW}"

export PGPASSWORD="$POSTGRES_PASSWORD"

PSQL=(
	psql
	-v ON_ERROR_STOP=1
	-h "$POSTGRES_HOST"
	-p "$POSTGRES_PORT"
	-U "$POSTGRES_USER"
	-d "$POSTGRES_DB"
)

mkdir -p "$OUTPUT_DIR"

quote_identifier() {
	local value="$1"
	printf '"%s"' "${value//\"/\"\"}"
}

safe_filename() {
	printf '%s' "$1" | tr '/\\: ' '____'
}

matches_filter() {
	local graph="$1"
	local base="${graph%_dewey}"
	local token

	[[ -z "$DATASET_FILTER" ]] && return 0
	IFS=',' read -r -a filter_tokens <<< "$DATASET_FILTER"
	for token in "${filter_tokens[@]}"; do
		token="${token//[[:space:]]/}"
		[[ -z "$token" ]] && continue
		if [[ "$graph" == $token || "$base" == $token ]]; then
			return 0
		fi
	done
	return 1
}

export_csv() {
	local sql="$1"
	local destination="$2"
	local destination_literal="${destination//\'/\'\'}"

	"${PSQL[@]}" -q -c "\\copy ($sql) TO '$destination_literal' WITH (FORMAT CSV, HEADER true)"
}

readarray -t GRAPHS < <(
	"${PSQL[@]}" -At -c \
		"SELECT name FROM ag_catalog.ag_graph WHERE name LIKE '%\_dewey' ESCAPE '\\' ORDER BY name;"
)

if [[ ${#GRAPHS[@]} -eq 0 ]]; then
	echo "No AGE graphs ending in _dewey found." >&2
	exit 0
fi

for graph in "${GRAPHS[@]}"; do
	if ! matches_filter "$graph"; then
		continue
	fi

	graph_literal="${graph//\'/\'\'}"
	readarray -t node_labels < <(
		"${PSQL[@]}" -At -c "
			SELECT l.name
			FROM ag_catalog.ag_label l
			JOIN ag_catalog.ag_graph g ON g.graphid = l.graph
			JOIN information_schema.columns d
			  ON d.table_schema = g.name AND d.table_name = l.name AND d.column_name = 'dewey'
			WHERE g.name = '$graph_literal'
			  AND l.kind = 'v'
			  AND l.name NOT LIKE '\\_ag\\_%' ESCAPE '\\'
			ORDER BY l.name;"
	)

	if [[ ${#node_labels[@]} -eq 0 ]]; then
		echo "Skipping $graph: no vertex label with a dewey column." >&2
		continue
	fi
	if [[ ${#node_labels[@]} -gt 1 ]]; then
		echo "Skipping $graph: multiple vertex labels with a dewey column: ${node_labels[*]}" >&2
		continue
	fi

	node_label="${node_labels[0]}"
	table_ref="$(quote_identifier "$graph").$(quote_identifier "$node_label")"
	graph_dir="$OUTPUT_DIR/$(safe_filename "$graph")"
	mkdir -p "$graph_dir"

	# depth is derived from the number of separators in the Dewey path.
	# parent_dewey is NULL for roots and otherwise the path without its last segment.
	nodes_cte="
		WITH nodes AS (
			SELECT
				(properties ->> '\"__id__\"'::agtype)::bigint AS id,
				dewey::text AS dewey,
				length(dewey::text) - length(replace(dewey::text, '.', '')) AS depth,
				CASE
					WHEN strpos(dewey::text, '.') = 0 THEN NULL
					ELSE regexp_replace(dewey::text, '\\.[^.]+$', '')
				END AS parent_dewey,
				split_part(dewey::text, '.', 1) AS root_dewey
			FROM $table_ref
			WHERE dewey IS NOT NULL
		),
		child_counts AS (
			SELECT parent_dewey AS dewey, count(*)::bigint AS child_count
			FROM nodes
			WHERE parent_dewey IS NOT NULL
			GROUP BY parent_dewey
		),
		node_stats AS (
			SELECT n.id, n.dewey, n.depth, n.parent_dewey, n.root_dewey,
			       coalesce(c.child_count, 0)::bigint AS child_count
			FROM nodes n
			LEFT JOIN child_counts c ON c.dewey = n.dewey
		),
		roots AS (
			SELECT id AS root_id, dewey AS root_dewey, depth AS root_depth
			FROM nodes
			WHERE parent_dewey IS NULL
		),
		leaves AS (
			SELECT n.*
			FROM node_stats n
			WHERE n.child_count = 0
		)"

	export_csv "$nodes_cte
		SELECT r.root_id, r.root_dewey AS dewey,
		       count(n.id)::bigint AS size,
		       max(n.depth - r.root_depth)::bigint AS depth
		FROM roots r
		JOIN nodes n ON n.root_dewey = r.root_dewey
		GROUP BY r.root_id, r.root_dewey
		ORDER BY size DESC, depth DESC, r.root_id
		LIMIT 20" "$graph_dir/01_trees_by_size_depth.csv"

	export_csv "$nodes_cte
		SELECT r.root_id, r.root_dewey AS dewey,
		       count(n.id)::bigint AS size,
		       max(n.depth - r.root_depth)::bigint AS depth
		FROM roots r
		JOIN nodes n ON n.root_dewey = r.root_dewey
		GROUP BY r.root_id, r.root_dewey
		ORDER BY depth DESC, size DESC, r.root_id
		LIMIT 20" "$graph_dir/02_trees_by_depth_size.csv"

	export_csv "$nodes_cte
		SELECT DISTINCT p.id, p.dewey, p.child_count, p.depth
		FROM leaves l
		JOIN node_stats p ON p.dewey = l.parent_dewey
		ORDER BY p.child_count DESC, p.depth DESC, p.id
		LIMIT 20" "$graph_dir/03_leaf_parents_by_children_desc.csv"

	export_csv "$nodes_cte
		SELECT DISTINCT p.id, p.dewey, p.child_count, p.depth
		FROM leaves l
		JOIN node_stats p ON p.dewey = l.parent_dewey
		ORDER BY p.child_count ASC, p.depth DESC, p.id
		LIMIT 20" "$graph_dir/04_leaf_parents_by_children_asc.csv"

	export_csv "$nodes_cte
		SELECT id, dewey, child_count, depth
		FROM node_stats
		ORDER BY child_count DESC, depth DESC, id
		LIMIT 20" "$graph_dir/05_all_nodes_by_children_desc.csv"

	export_csv "$nodes_cte
		SELECT id, dewey, child_count, depth
		FROM node_stats
		ORDER BY child_count ASC, depth DESC, id
		LIMIT 20" "$graph_dir/06_all_nodes_by_children_asc.csv"

	export_csv "$nodes_cte
		SELECT l.id AS leaf_id, l.dewey AS leaf_dewey,
		       r.root_id, r.root_dewey,
		       (l.depth - r.root_depth)::bigint AS distance
		FROM leaves l
		JOIN roots r ON r.root_dewey = l.root_dewey
		ORDER BY distance DESC, l.id
		LIMIT 20" "$graph_dir/07_leaves_by_root_distance.csv"

	export_csv "$nodes_cte
		SELECT l.id AS leaf_id, l.dewey AS leaf_dewey,
		       p.id AS parent_id, p.dewey AS parent_dewey,
		       l.depth::bigint AS depth
		FROM leaves l
		JOIN node_stats p ON p.dewey = l.parent_dewey
		ORDER BY depth DESC, l.id
		LIMIT 20" "$graph_dir/08_leaves_with_parent_by_depth.csv"

	echo "Exported $graph (label=$node_label) to $graph_dir"
done

echo "Done. CSV files: $OUTPUT_DIR"
