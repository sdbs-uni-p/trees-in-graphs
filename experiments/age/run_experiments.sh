#!/bin/bash
# SPDX-License-Identifier: GPL-3.0-only

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
	cat <<'EOF'
Usage: run_experiments.sh [OPTIONS]

Options:
	-q, --queries LIST   Comma-separated query ids or filenames to run.
	                     Examples: 01,02 or 01_foo.sql,02_bar.sql or 0* (glob).
	-d, --datasets LIST  Comma-separated dataset/base names or graph names to run.
	                     Examples: snb_sf1_comment,artificial_trees_truebase_100 or snb* (glob).
	-n, --note TEXT      Optional note text to append to notes.txt for this run.
	-w, --warmup         Run one warmup execution per query before measurements.
	-r, --runs N         Number of measurement runs per query (default: 1).
	-t, --timeout-ms N   Statement timeout in ms (default: 3600000 = 1 hour).
	--timing-off         Use EXPLAIN (ANALYZE, TIMING OFF) for plan runtime.
	--save-plans         After runs, save one EXPLAIN ANALYZE plan per graph/query.
	--save-results       After runs, save one result output per graph/query.
	--save-queries       Persist rendered queries output (default: off).
	-h, --help           Show this help.

ENV:
	QUERY_FILTER         Same as --queries / -q; CLI overrides ENV.
	DATASET_FILTER       Same as --datasets; CLI overrides ENV.
	WARMUP               If set to 1, run one warmup execution per query.
	RUNS                 Number of measurement runs per query.
	SAVE_PLANS           If set to 1, save one EXPLAIN ANALYZE plan.
	SAVE_RESULTS         If set to 1, save one result output.
	SAVE_QUERIES         If set to 1, persist queries output (default: 0).
	TIMING_OFF           If set to 1, use TIMING OFF for EXPLAIN ANALYZE.
	TIMEOUT_MS           Same as --timeout-ms; CLI overrides ENV.
	NOTE                 Optional note text (same as --note).
EOF
}

if [ -d "/queries" ]; then
	QUERY_ROOT="/queries"
else
	QUERY_ROOT="${SCRIPT_DIR}/../../queries/age"
fi

RESULTS_BASE="${RESULTS_BASE:-/results}"
RUN_TS="$(date -u +"%Y%m%d_%H%M%S")"
OUTPUT_DIR="${RESULTS_BASE}/age/${RUN_TS}"
ERROR_DIR="${OUTPUT_DIR}/errors"
CSV_FILE="${OUTPUT_DIR}/runtimes.csv"
QUERIES_DIR="${OUTPUT_DIR}/queries"

QUERY_FILTER="${QUERY_FILTER:-}"
DATASET_FILTER="${DATASET_FILTER:-}"
NOTE="${NOTE:-}"
WARMUP="${WARMUP:-0}"
RUNS="${RUNS:-1}"
SAVE_PLANS="${SAVE_PLANS:-0}"
SAVE_RESULTS="${SAVE_RESULTS:-0}"
SAVE_QUERIES="${SAVE_QUERIES:-0}"
TIMEOUT_MS="${TIMEOUT_MS:-3600000}"
TIMING_OFF="${TIMING_OFF:-0}"

while [[ $# -gt 0 ]]; do
	case "$1" in
		-q|--queries)
			QUERY_FILTER="${2:-}"
			shift 2
			;;
		-d|--datasets)
			DATASET_FILTER="${2:-}"
			shift 2
			;;
		-n|--note)
			NOTE="${2:-}"
			shift 2
			;;
		-w|--warmup)
			WARMUP=1
			shift
			;;
		-r|--runs)
			RUNS="${2:-}"
			if ! [[ "$RUNS" =~ ^[0-9]+$ ]] || [ "$RUNS" -lt 1 ]; then
				echo "Invalid runs value: $RUNS" >&2
				exit 1
			fi
			shift 2
			;;
		-t|--timeout-ms)
			TIMEOUT_MS="${2:-}"
			shift 2
			;;
		--save-plans)
			SAVE_PLANS=1
			shift
			;;
		--save-results)
			SAVE_RESULTS=1
			shift
			;;
		--save-queries)
			SAVE_QUERIES=1
			shift
			;;
		--timing-off)
			TIMING_OFF=1
			shift
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

if ! [[ "$RUNS" =~ ^[0-9]+$ ]] || [ "$RUNS" -lt 1 ]; then
	echo "Invalid RUNS value: $RUNS" >&2
	exit 1
fi

if ! [[ "$TIMEOUT_MS" =~ ^[0-9]+$ ]]; then
	echo "Invalid TIMEOUT_MS value: $TIMEOUT_MS" >&2
	exit 1
fi

if [[ "$SAVE_QUERIES" != "0" && "$SAVE_QUERIES" != "1" ]]; then
	echo "Invalid SAVE_QUERIES value: $SAVE_QUERIES (expected 0 or 1)" >&2
	exit 1
fi

if [[ "$SAVE_PLANS" != "0" && "$SAVE_PLANS" != "1" ]]; then
	echo "Invalid SAVE_PLANS value: $SAVE_PLANS (expected 0 or 1)" >&2
	exit 1
fi

if [[ "$SAVE_RESULTS" != "0" && "$SAVE_RESULTS" != "1" ]]; then
	echo "Invalid SAVE_RESULTS value: $SAVE_RESULTS (expected 0 or 1)" >&2
	exit 1
fi

POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_DB="${POSTGRES_DB:-postgresDB}"
POSTGRES_USER="${POSTGRES_USER:-postgresUser}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-postgresPW}"

export PGPASSWORD="$POSTGRES_PASSWORD"
export PGOPTIONS="${PGOPTIONS:-} -c statement_timeout=${TIMEOUT_MS}"

PSQL_BASE=(
	psql
	-v ON_ERROR_STOP=1
	-h "$POSTGRES_HOST"
	-p "$POSTGRES_PORT"
	-U "$POSTGRES_USER"
	-d "$POSTGRES_DB"
	-At
)

if ! "${PSQL_BASE[@]}" -c "CREATE EXTENSION IF NOT EXISTS pg_hint_plan;" >/dev/null 2>&1; then
	echo "WARNING: pg_hint_plan extension could not be created/enabled; continuing without planner hints." >&2
fi

print_not_initialized_hint() {
	echo "AGE is not fully initialized yet. Skipping experiment run." >&2
	echo "Check status: docker compose -f docker/age/docker-compose.yml ps" >&2
	echo "View logs:    docker compose -f docker/age/docker-compose.yml logs -f age_treebench" >&2
}

is_age_initialized() {
	if command -v docker >/dev/null 2>&1; then
		local health
		health="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' age_treebench 2>/dev/null || true)"
		if [[ "$health" == "healthy" ]]; then
			return 0
		fi
		if [[ "$health" == "starting" || "$health" == "unhealthy" ]]; then
			return 1
		fi
	fi

	local init_ok
	init_ok="$("${PSQL_BASE[@]}" -c "WITH graph_count AS (SELECT count(*)::int AS cnt FROM ag_catalog.ag_graph), loaded_vertex_labels AS (SELECT count(*)::int AS cnt FROM ag_catalog.ag_label l JOIN ag_catalog.ag_graph g ON g.graphid = l.graph JOIN pg_namespace n ON n.nspname = g.name JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = l.name WHERE l.kind = 'v' AND l.name NOT LIKE '\\_ag\\_%' ESCAPE '\\' AND c.reltuples > 0) SELECT CASE WHEN (SELECT cnt FROM graph_count) > 0 AND (SELECT cnt FROM loaded_vertex_labels) > 0 THEN 1 ELSE 0 END;")" || return 1
	[[ "$init_ok" == "1" ]]
}

if ! is_age_initialized; then
	print_not_initialized_hint
	exit 0
fi

mkdir -p "$ERROR_DIR"
if [ "$SAVE_QUERIES" -eq 1 ]; then
	mkdir -p "$QUERIES_DIR"
fi

if [ -n "${NOTE//[[:space:]]/}" ]; then
	NOTES_FILE="${RESULTS_BASE}/age/notes.txt"
	mkdir -p "$(dirname "$NOTES_FILE")"
	year="${RUN_TS:0:4}"
	month="${RUN_TS:4:2}"
	day="${RUN_TS:6:2}"
	hour="${RUN_TS:9:2}"
	minute="${RUN_TS:11:2}"
	second="${RUN_TS:13:2}"
	notes_ts="${day}-${month}-${year} ${hour}:${minute}:${second}"
	echo "${notes_ts} ${NOTE}" >> "$NOTES_FILE"
fi

if [ "$SAVE_PLANS" -eq 1 ]; then
	PLAN_DIR="${OUTPUT_DIR}/plans"
	mkdir -p "$PLAN_DIR"
fi
if [ "$SAVE_RESULTS" -eq 1 ]; then
	RESULT_DIR="${OUTPUT_DIR}/results"
	EMPTY_RESULTS_LOG="${OUTPUT_DIR}/empty_results.log"
	mkdir -p "$RESULT_DIR"
	echo "# empty results" > "$EMPTY_RESULTS_LOG"
fi
echo "graph,query,run,runtime_ms" > "$CSV_FILE"

readarray -t GRAPHS < <("${PSQL_BASE[@]}" -c "SELECT name FROM ag_catalog.ag_graph ORDER BY name;")

if [ "${#GRAPHS[@]}" -eq 0 ]; then
	echo "No graphs found in ag_catalog.ag_graph" >&2
	exit 0
fi

get_label() {
	local graph="$1"
	local kind="$2"
	local graph_literal
	graph_literal="$(escape_sql_literal "$graph")"

	"${PSQL_BASE[@]}" -c "\
		SELECT l.name
		FROM ag_catalog.ag_label l
		JOIN ag_catalog.ag_graph g ON g.graphid = l.graph
		WHERE g.name = '$graph_literal' AND l.kind = '$kind'
		  AND l.name NOT LIKE '\\_ag\\_%' ESCAPE '\\'
		ORDER BY l.name;"
}

resolve_tree_key() {
	local graph="$1"
	local graph_lc
	graph_lc="${graph,,}"

	if [[ "$graph_lc" == *_comment_* ]]; then
		echo "comment"
	elif [[ "$graph_lc" == *_place_* ]]; then
		echo "place"
	elif [[ "$graph_lc" == *_tagclass_* ]]; then
		echo "tagclass"
	else
		echo ""
	fi
}

contains_label() {
	local needle="$1"
	shift
	local item
	for item in "$@"; do
		if [[ "$item" == "$needle" ]]; then
			return 0
		fi
	done
	return 1
}

resolve_labels_for_graph() {
	local graph="$1"
	local tree_key expected_node expected_edge
	tree_key="$(resolve_tree_key "$graph")"

	case "$tree_key" in
		comment)
			expected_node="Comment"
			expected_edge="comment_replyOf_comment"
			;;
		place)
			expected_node="Place"
			expected_edge="place_isPartOf_place"
			;;
		tagclass)
			expected_node="Tagclass"
			expected_edge="tagclass_isSubclassOf_tagclass"
			;;
		*)
			echo ""
			return 1
			;;
	esac

	echo "$expected_node|$expected_edge"
}

escape_filename() {
	echo "$1" | tr '/\\: ' '____'
}

escape_sql_literal() {
	echo "$1" | sed "s/'/''/g"
}

resolve_rootid() {
	local graph="$1"
	local graph_lc
	graph_lc="${graph,,}"

	if [[ "$graph_lc" == *_tagclass_* ]]; then
		echo "1"
	elif [[ "$graph_lc" == *_place_* ]]; then
		echo "1455"
	elif [[ "$graph_lc" == *snb* ]]; then
		echo "1374390095024"
	elif [[ "$graph_lc" == *artificial_forests* ]]; then
		echo "2"
	else
		echo "1"
	fi
}

resolve_ancestor_ids() {
	local graph="$1"
	local base

	base="$graph"
	base="${base%_baseline}"
	base="${base%_dewey}"
	base="${base%_prepost}"

	case "$base" in
		artificial_forests_40)
			echo "4|7"
			;;
		artificial_trees_truebase_10)
			echo "2|8"
			;;
		artificial_trees_truebase_100)
			echo "3|86"
			;;
		artificial_trees_truebase_1000)
			echo "3|901"
			;;
		artificial_trees_truebase_10000)
			echo "4|8065"
			;;
		artificial_trees_ultratall_10)
			echo "2|9"
			;;
		artificial_trees_ultratall_100)
			echo "2|48"
			;;
		artificial_trees_ultratall_1000)
			echo "2|961"
			;;
		artificial_trees_ultratall_10000)
			echo "4|7043"
			;;
		artificial_trees_ultrawide_10)
			echo "2|8"
			;;
		artificial_trees_ultrawide_100)
			echo "4|60"
			;;
		artificial_trees_ultrawide_1000)
			echo "4|867"
			;;
		artificial_trees_ultrawide_10000)
			echo "2|8782"
			;;
		snb_sf1_comment)
			echo "549757114012|549757114029"
			;;
		snb_sf1_place)
			echo "1455|548"
			;;
		snb_sf1_tagclass)
			echo "240|47"
			;;
		*)
			echo "1|2"
			;;
	esac
}

declare -A HINT_SAVED_KEYS

persist_hinted_query() {
	local graph="$1"
	local full_path="$2"
	local hinted_sql_file="$3"
	local query_name
	local query_set
	local graph_safe
	local query_safe
	local key
	local dest

	if [ "$SAVE_QUERIES" -ne 1 ]; then
		return
	fi

	query_name="$(basename "$full_path")"
	query_set="$(basename "$(dirname "$full_path")")"
	graph_safe="$(echo "$graph" | tr '/\\: ' '____')"
	query_safe="$(echo "${query_set}__${query_name}" | tr '/\\: ' '____')"
	key="${graph}|${query_set}|${query_name}"
	if [[ -n "${HINT_SAVED_KEYS[$key]+x}" ]]; then
		return
	fi
	HINT_SAVED_KEYS["$key"]=1
	dest="$QUERIES_DIR/${graph_safe}__${query_safe}"
	cp "$hinted_sql_file" "$dest"
}

extract_nodetype_alias() {
	local sql_file="$1"
	sed -nE 's/.*:"nodetype"[[:space:]]+([A-Za-z_][A-Za-z0-9_]*).*/\1/p' "$sql_file" | head -n 1
}

ensure_root_alias() {
	local in_file="$1"
	local out_file="$2"

	perl -0777 -pe '
		s{
			(WITH\s+root\s+AS\s*\(.*?\bFROM\s+:"graphname"\.:"nodetype")
			(\s+)(WHERE|FOR|GROUP|ORDER|LIMIT|\))
		}{$1 root_alias$2$3}isx
	' "$in_file" > "$out_file"
}

ensure_ancestor_aliases() {
	local in_file="$1"
	local out_file="$2"

	perl -0777 -pe '
		s/FROM\s+:"graphname"\.:"nodetype"\s+WHERE/FROM :"graphname".:"nodetype" n1\n  WHERE/s;
		s/FROM\s+:"graphname"\.:"nodetype"\s+WHERE/FROM :"graphname".:"nodetype" n2\n  WHERE/s;
	' "$in_file" > "$out_file"
}

resolve_hint_alias() {
	local sql_file="$1"
	if grep -Eqi 'WITH[[:space:]]+root[[:space:]]+AS' "$sql_file"; then
		echo "root_alias"
	else
		extract_nodetype_alias "$sql_file"
	fi
}

inject_seqscan_hint() {
	local in_file="$1"
	local out_file="$2"
	local alias

	if grep -Eq '/\*\+[[:space:]]*SeqScan\(' "$in_file"; then
		cp "$in_file" "$out_file"
		return
	fi

	alias="$(resolve_hint_alias "$in_file" || true)"
	if [[ -n "$alias" ]]; then
		{
			echo "/*+ SeqScan(${alias}) Parallel(${alias} 0 hard) */"
			cat "$in_file"
		} > "$out_file"
	else
		cp "$in_file" "$out_file"
	fi
}

inject_ancestor_seqscan_hints() {
	local in_file="$1"
	local out_file="$2"
	local query_set="$3"
	local alias1 alias2

	if grep -Eq '/\*\+[[:space:]]*SeqScan\(' "$in_file"; then
		cp "$in_file" "$out_file"
		return
	fi

	if [[ "$query_set" == "baseline" ]]; then
		alias1="node1"
		alias2="node2"
	else
		alias1="n1"
		alias2="n2"
	fi

	{
		echo "/*+ SeqScan(${alias1}) SeqScan(${alias2}) Parallel(${alias1} 0 hard) Parallel(${alias2} 0 hard) */"
		cat "$in_file"
	} > "$out_file"
}

wrap_baseline_transaction() {
	local in_file="$1"
	local out_file="$2"

	{
		echo "BEGIN;"
		echo ""
		echo "SET LOCAL max_parallel_workers_per_gather = 0;"
		echo ""
		cat "$in_file"
		echo ""
		echo "ROLLBACK;"
	} > "$out_file"
}

build_explain_script() {
	local source_file="$1"
	local out_file="$2"
	local explain_mode="${3:-analyze}"
	local explain_stmt

	if [[ "$explain_mode" == "explain_only" ]]; then
		explain_stmt="EXPLAIN"
	elif [ "$TIMING_OFF" -eq 1 ]; then
		explain_stmt="EXPLAIN (ANALYZE, TIMING OFF)"
	else
		explain_stmt="EXPLAIN ANALYZE"
	fi

	{
		echo "$explain_stmt"
		cat "$source_file"
	} > "$out_file"
}

prepare_execution_query() {
	local source_file="$1"
	local out_file="$2"
	local query_set="$3"

	if [[ "$query_set" == "baseline" ]] && grep -Eqi '^[[:space:]]*BEGIN[[:space:]]*;' "$source_file"; then
		awk '
			BEGIN { inside = 0 }
			/^[[:space:]]*SET[[:space:]]+LOCAL[[:space:]]+max_parallel_workers_per_gather[[:space:]]*=[[:space:]]*0;[[:space:]]*$/ { inside = 1; next }
			inside && /^[[:space:]]*ROLLBACK;[[:space:]]*$/ { exit }
			inside { print }
		' "$source_file" > "$out_file"
	else
		cp "$source_file" "$out_file"
	fi
}

render_query_template() {
	local full_path="$1"
	local out_file="$2"
	local nodetype="$3"
	local reltype="$4"
	local rootid="$5"
	local graph="$6"
	local query_set="$7"
	local id1="$rootid"
	local id2="$rootid"
	local query_file_name
	local raw_file

	query_file_name="$(basename "$full_path")"
	if [[ "$query_file_name" == "11_check_if_ancestor.sql" ]]; then
		local ancestor_ids
		ancestor_ids="$(resolve_ancestor_ids "$graph")"
		id1="${ancestor_ids%%|*}"
		id2="${ancestor_ids##*|}"
	fi

	raw_file="$(mktemp)"
	local aliased_file
	aliased_file="$(mktemp)"
	local hinted_file
	hinted_file="$(mktemp)"
	sed -e 's/\$NODE_TYPE/'"$nodetype"'/g' \
			-e 's/\$REL_TYPE/'"$reltype"'/g' \
			-e 's/\$rootID/'"$rootid"'/g' \
			-e 's/\$rootId/'"$rootid"'/g' \
			-e 's/\$ROOTID/'"$rootid"'/g' \
			-e 's/\$id1/'"$id1"'/g' \
			-e 's/\$id2/'"$id2"'/g' \
			-e 's/:id1/'"$id1"'/g' \
			-e 's/:id2/'"$id2"'/g' \
			-e 's/\$GRAPHNAME/'"$graph"'/g' \
			"$full_path" > "$raw_file"

	if [[ "$query_file_name" == "11_check_if_ancestor.sql" ]]; then
		if [[ "$query_set" == "baseline" ]]; then
			cp "$raw_file" "$aliased_file"
		else
			ensure_ancestor_aliases "$raw_file" "$aliased_file"
		fi
		inject_ancestor_seqscan_hints "$aliased_file" "$hinted_file" "$query_set"
	else
		ensure_root_alias "$raw_file" "$aliased_file"
		inject_seqscan_hint "$aliased_file" "$hinted_file"
	fi
	if [[ "$query_set" == "baseline" ]]; then
		wrap_baseline_transaction "$hinted_file" "$out_file"
	else
		cp "$hinted_file" "$out_file"
	fi
	persist_hinted_query "$graph" "$full_path" "$out_file"
	rm -f "$raw_file" "$aliased_file" "$hinted_file"
}

parse_timing_ms() {
	awk '/^Time:/{val=$2; unit=$3} END {if (val == "") exit 1; if (unit == "ms") printf "%.3f", val; else if (unit == "s") printf "%.3f", (val * 1000); else exit 1}'
}

measure_psql_timing_ms_null() {
	local source_file="$1"
	local graph="$2"
	local nodetype="$3"
	local reltype="$4"
	local rootid="$5"
	local query_set="$6"
	local exec_file
	exec_file="$(mktemp)"
	prepare_execution_query "$source_file" "$exec_file" "$query_set"
	local timing_script
	timing_script="$(mktemp)"
	{
		echo "\\timing on"
		echo "\\o /dev/null"
		cat "$exec_file"
		echo "\\o"
	} > "$timing_script"

	local timing_out
	timing_out="$(mktemp)"
	if ! (
		if [[ "$query_set" == "baseline" ]]; then
			PGOPTIONS="-c statement_timeout=${TIMEOUT_MS} -c max_parallel_workers_per_gather=0" "${PSQL_BASE[@]}" \
				-v graphname="$graph" \
				-v nodetype="$nodetype" \
				-v reltype="$reltype" \
				-v rootid="$rootid" \
				-f "$timing_script" > "$timing_out" 2>&1
		else
			"${PSQL_BASE[@]}" \
				-v graphname="$graph" \
				-v nodetype="$nodetype" \
				-v reltype="$reltype" \
				-v rootid="$rootid" \
				-f "$timing_script" > "$timing_out" 2>&1
		fi
	); then
		if grep -Eqi 'statement timeout|canceling statement due to statement timeout' "$timing_out"; then
			rm -f "$exec_file" "$timing_script" "$timing_out"
			return 124
		fi
		rm -f "$exec_file" "$timing_script" "$timing_out"
		return 1
	fi

	if ! parse_timing_ms < "$timing_out"; then
		rm -f "$exec_file" "$timing_script" "$timing_out"
		return 1
	fi

	rm -f "$exec_file" "$timing_script" "$timing_out"
}

run_warmup() {
	local graph="$1"
	local nodetype="$2"
	local reltype="$3"
	local rootid="$4"
	local query_set="$5"
	local query_file="$6"
	local query_root="$7"

	local full_path="$query_root/$query_set/$query_file"
	local tmpfile
	tmpfile="$(mktemp)"
	render_query_template "$full_path" "$tmpfile" "$nodetype" "$reltype" "$rootid" "$graph" "$query_set"

	measure_psql_timing_ms_null "$tmpfile" "$graph" "$nodetype" "$reltype" "$rootid" "$query_set" >/dev/null 2>&1 || true

	rm -f "$tmpfile"
}

run_plan() {
	local graph="$1"
	local nodetype="$2"
	local reltype="$3"
	local rootid="$4"
	local query_set="$5"
	local query_file="$6"
	local query_root="$7"
	local explain_mode="${8:-analyze}"
	local query_base
	query_base="${query_file%.sql}"

	local full_path="$query_root/$query_set/$query_file"
	local tmpfile
	tmpfile="$(mktemp)"
	render_query_template "$full_path" "$tmpfile" "$nodetype" "$reltype" "$rootid" "$graph" "$query_set"
	local exec_file
	exec_file="$(mktemp)"
	prepare_execution_query "$tmpfile" "$exec_file" "$query_set"

	local explain_file
	explain_file="$(mktemp)"
	build_explain_script "$exec_file" "$explain_file" "$explain_mode"

	local err_file
	err_file="$ERROR_DIR/$(escape_filename "${graph}_${query_set}_${query_base}_plan").log"
	local plan_file
	plan_file="$PLAN_DIR/$(escape_filename "${graph}_${query_set}_${query_base}").plan.txt"
	local plan_err
	plan_err="$(mktemp)"

	if ! (
		if [[ "$query_set" == "baseline" ]]; then
			PGOPTIONS="-c statement_timeout=${TIMEOUT_MS} -c max_parallel_workers_per_gather=0" "${PSQL_BASE[@]}" \
				-v graphname="$graph" \
				-v nodetype="$nodetype" \
				-v reltype="$reltype" \
				-v rootid="$rootid" \
				-f "$explain_file" > "$plan_file" 2> "$plan_err"
		else
			"${PSQL_BASE[@]}" \
				-v graphname="$graph" \
				-v nodetype="$nodetype" \
				-v reltype="$reltype" \
				-v rootid="$rootid" \
				-f "$explain_file" > "$plan_file" 2> "$plan_err"
		fi
	); then
		cat "$plan_err" > "$err_file"
		rm -f "$tmpfile" "$exec_file" "$explain_file" "$plan_err"
		return 1
	fi

	rm -f "$tmpfile" "$exec_file" "$explain_file" "$plan_err"
}

run_results() {
	local graph="$1"
	local nodetype="$2"
	local reltype="$3"
	local rootid="$4"
	local query_set="$5"
	local query_file="$6"
	local query_root="$7"
	local query_base
	query_base="${query_file%.sql}"

	local full_path="$query_root/$query_set/$query_file"
	local tmpfile
	tmpfile="$(mktemp)"
	render_query_template "$full_path" "$tmpfile" "$nodetype" "$reltype" "$rootid" "$graph" "$query_set"
	local exec_file
	exec_file="$(mktemp)"
	prepare_execution_query "$tmpfile" "$exec_file" "$query_set"

	local err_file
	err_file="$ERROR_DIR/$(escape_filename "${graph}_${query_set}_${query_base}_result").log"
	local result_file
	result_file="$RESULT_DIR/$(escape_filename "${graph}_${query_set}_${query_base}").results.txt"
	local query_err
	query_err="$(mktemp)"

	if ! (
		if [[ "$query_set" == "baseline" ]]; then
			PGOPTIONS="-c statement_timeout=${TIMEOUT_MS} -c max_parallel_workers_per_gather=0" "${PSQL_BASE[@]}" \
				-v graphname="$graph" \
				-v nodetype="$nodetype" \
				-v reltype="$reltype" \
				-v rootid="$rootid" \
				-f "$exec_file" > "$result_file" 2> "$query_err"
		else
			"${PSQL_BASE[@]}" \
				-v graphname="$graph" \
				-v nodetype="$nodetype" \
				-v reltype="$reltype" \
				-v rootid="$rootid" \
				-f "$exec_file" > "$result_file" 2> "$query_err"
		fi
	); then
		cat "$query_err" > "$err_file"
		rm -f "$tmpfile" "$exec_file" "$query_err"
		return 1
	fi

	if [ ! -s "$result_file" ]; then
		echo "graph=$graph" >> "$EMPTY_RESULTS_LOG"
		echo "query=${query_set}/${query_file}" >> "$EMPTY_RESULTS_LOG"
		echo "result=$result_file" >> "$EMPTY_RESULTS_LOG"
		echo "" >> "$EMPTY_RESULTS_LOG"
	fi

	rm -f "$tmpfile" "$exec_file" "$query_err"
}

declare -A graph_map
declare -A base_map

for graph in "${GRAPHS[@]}"; do
	graph_map["$graph"]=1
	base="$graph"
	base="${base%_baseline}"
	base="${base%_dewey}"
	base="${base%_prepost}"
	base_map["$base"]=1
done

mapfile -t BASES < <(printf "%s\n" "${!base_map[@]}" | sort)

if [ -n "$DATASET_FILTER" ]; then
	IFS=',' read -r -a DATASET_TOKENS <<< "$DATASET_FILTER"
	FILTERED_BASES=()
	for base in "${BASES[@]}"; do
		for token in "${DATASET_TOKENS[@]}"; do
			token="${token//[[:space:]]/}"
			[ -z "$token" ] && continue
			if [[ "$token" == *"*"* || "$token" == *"?"* ]]; then
				if [[ "$base" == $token || "${base}_baseline" == $token || "${base}_dewey" == $token || "${base}_prepost" == $token ]]; then
					FILTERED_BASES+=("$base")
					break
				fi
			elif [[ "$token" == "$base" || "$token" == "${base}_baseline" || "$token" == "${base}_dewey" || "$token" == "${base}_prepost" ]]; then
				FILTERED_BASES+=("$base")
				break
			fi
		done
	done
	BASES=("${FILTERED_BASES[@]}")
fi

mapfile -t QUERY_FILES < <(
	find "$QUERY_ROOT/baseline" -maxdepth 1 -type f -name "[0-9]*_*.sql" -printf "%f\n" \
	| awk -F_ '{print $1"\t"$0}' \
	| sort -n -k1,1 -k2,2 \
	| cut -f2
)

if [ -n "$QUERY_FILTER" ]; then
	IFS=',' read -r -a FILTER_TOKENS <<< "$QUERY_FILTER"
	FILTERED_QUERY_FILES=()
	for query_file in "${QUERY_FILES[@]}"; do
		query_id="${query_file%%_*}"
		for token in "${FILTER_TOKENS[@]}"; do
			token="${token//[[:space:]]/}"
			[ -z "$token" ] && continue
			if [[ "$token" == *"*"* || "$token" == *"?"* ]]; then
				if [[ "$query_file" == $token ]]; then
					FILTERED_QUERY_FILES+=("$query_file")
					break
				fi
			elif [[ "$token" == "$query_id" || "$token" == "$query_file" ]]; then
				FILTERED_QUERY_FILES+=("$query_file")
				break
			fi
		done
	done
	QUERY_FILES=("${FILTERED_QUERY_FILES[@]}")
fi

if [ "${#BASES[@]}" -eq 0 ]; then
	echo "No datasets matched filter: ${DATASET_FILTER:-<none>}" >&2
	exit 0
fi

if [ "${#QUERY_FILES[@]}" -eq 0 ]; then
	echo "No queries matched filter: ${QUERY_FILTER:-<none>}" >&2
	exit 0
fi

total_jobs=0
for base in "${BASES[@]}"; do
	for query_file in "${QUERY_FILES[@]}"; do
		for graph in \
			"${base}_baseline" \
			"${base}_dewey" \
			"${base}_prepost"; do
			if [[ -n "${graph_map[$graph]+x}" ]]; then
				((total_jobs+=1))
			fi
		done
	done
done

echo "Starting AGE experiments"
echo "  Output directory: $OUTPUT_DIR"
echo "  Datasets selected: ${#BASES[@]}"
echo "  Queries selected: ${#QUERY_FILES[@]}"
echo "  Total graph-query jobs: $total_jobs"

if [ "$total_jobs" -eq 0 ]; then
	echo "Nothing to run: no matching graph/query combinations found." >&2
	exit 0
fi

current_job=0

for base in "${BASES[@]}"; do
	for query_file in "${QUERY_FILES[@]}"; do
		for graph in \
			"${base}_baseline" \
			"${base}_dewey" \
			"${base}_prepost"; do
			if [[ -z "${graph_map[$graph]+x}" ]]; then
				continue
			fi

			if [[ "$graph" == *_baseline ]]; then
				query_set="baseline"
			elif [[ "$graph" == *_dewey ]]; then
				query_set="dewey"
			elif [[ "$graph" == *_prepost ]]; then
				query_set="prepost"
			else
				continue
			fi

			((current_job+=1))

			query_root="$QUERY_ROOT"

			rootid="$(resolve_rootid "$graph")"

			readarray -t node_labels < <(get_label "$graph" "v")
			readarray -t edge_labels < <(get_label "$graph" "e")

			label_pair="$(resolve_labels_for_graph "$graph" || true)"

			if [[ -n "$label_pair" ]]; then
				expected_node="${label_pair%%|*}"
				expected_edge="${label_pair##*|}"
				if contains_label "$expected_node" "${node_labels[@]}" && contains_label "$expected_edge" "${edge_labels[@]}"; then
					nodetype="$expected_node"
					reltype="$expected_edge"
				else
					err_file="$ERROR_DIR/$(escape_filename "${graph}_nodetype").log"
					printf "Expected tree labels not found.\nexpected_node=%s expected_edge=%s\nnode_labels(%s): %s\nedge_labels(%s): %s\n" "$expected_node" "$expected_edge" "${#node_labels[@]}" "${node_labels[*]}" "${#edge_labels[@]}" "${edge_labels[*]}" > "$err_file"
					continue
				fi
			elif [ "${#node_labels[@]}" -eq 1 ] && [ "${#edge_labels[@]}" -eq 1 ]; then
				nodetype="${node_labels[0]}"
				reltype="${edge_labels[0]}"
			else
				err_file="$ERROR_DIR/$(escape_filename "${graph}_nodetype").log"
				printf "Could not resolve tree-specific labels.\nnode_labels(%s): %s\nedge_labels(%s): %s\n" "${#node_labels[@]}" "${node_labels[*]}" "${#edge_labels[@]}" "${edge_labels[*]}" > "$err_file"
				continue
			fi

			if [ "$WARMUP" -eq 1 ]; then
				run_warmup "$graph" "$nodetype" "$reltype" "$rootid" "$query_set" "$query_file" "$query_root"
			fi

			runs_ok=0
			runs_failed=0
			runs_skipped=0
			timeout_on_first_run=0
			for ((run_idx=1; run_idx<=RUNS; run_idx++)); do
				local_tmpfile="$(mktemp)"
				full_path="$query_root/$query_set/$query_file"
				render_query_template "$full_path" "$local_tmpfile" "$nodetype" "$reltype" "$rootid" "$graph" "$query_set"

				runtime=""
				if runtime=$(measure_psql_timing_ms_null "$local_tmpfile" "$graph" "$nodetype" "$reltype" "$rootid" "$query_set"); then
					((runs_ok+=1))
					echo "${graph},${query_file%.sql},${run_idx},${runtime}" >> "$CSV_FILE"
				else
					((runs_failed+=1))
					rc=$?
					err_file="$ERROR_DIR/$(escape_filename "${graph}_${query_set}_${query_file%.sql}_run${run_idx}").log"
					if [[ "$rc" -eq 124 ]]; then
						echo "Query timed out after ${TIMEOUT_MS} ms." > "$err_file"
					else
						echo "Failed to measure timing." > "$err_file"
					fi
					echo "${graph},${query_file%.sql},${run_idx}," >> "$CSV_FILE"

					if [[ "$rc" -eq 124 && "$run_idx" -eq 1 ]]; then
						timeout_on_first_run=1
						if (( RUNS > 1 )); then
							runs_skipped=$((RUNS - 1))
							for ((skip_idx=2; skip_idx<=RUNS; skip_idx++)); do
								skip_err_file="$ERROR_DIR/$(escape_filename "${graph}_${query_set}_${query_file%.sql}_run${skip_idx}").log"
								echo "Skipped run ${skip_idx}: run 1 timed out after ${TIMEOUT_MS} ms." > "$skip_err_file"
								echo "${graph},${query_file%.sql},${skip_idx}," >> "$CSV_FILE"
							done
						fi
					fi
				fi

				rm -f "$local_tmpfile"

				if [[ "$timeout_on_first_run" -eq 1 ]]; then
					break
				fi
			done

			plan_status="off"
			if [ "$SAVE_PLANS" -eq 1 ]; then
				if [[ "$timeout_on_first_run" -eq 1 ]]; then
					if run_plan "$graph" "$nodetype" "$reltype" "$rootid" "$query_set" "$query_file" "$query_root" "explain_only"; then
						plan_status="saved(explain-only)"
					else
						plan_status="failed(explain-only)"
					fi
				else
					if run_plan "$graph" "$nodetype" "$reltype" "$rootid" "$query_set" "$query_file" "$query_root"; then
						plan_status="saved"
					else
						plan_status="failed"
					fi
				fi
			fi

			result_status="off"
			if [ "$SAVE_RESULTS" -eq 1 ] && [[ "$timeout_on_first_run" -eq 0 ]]; then
				if run_results "$graph" "$nodetype" "$reltype" "$rootid" "$query_set" "$query_file" "$query_root"; then
					result_status="saved"
				else
					result_status="failed"
				fi
			elif [ "$SAVE_RESULTS" -eq 1 ] && [[ "$timeout_on_first_run" -eq 1 ]]; then
				err_file="$ERROR_DIR/$(escape_filename "${graph}_${query_set}_${query_file%.sql}_result").log"
				echo "Skipped result execution: run 1 timed out after ${TIMEOUT_MS} ms." > "$err_file"
				result_status="skipped(timeout)"
			fi

			echo "[$current_job/$total_jobs] graph=$graph query_set=$query_set query=${query_file%.sql} runs_ok=$runs_ok runs_failed=$runs_failed runs_skipped=$runs_skipped plan=$plan_status result=$result_status"
		done
	done
done

echo "Done. Results CSV: $CSV_FILE"
