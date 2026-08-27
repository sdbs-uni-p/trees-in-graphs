#!/usr/bin/env bash
# SPDX-License-Identifier: GPL-3.0-only

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QUERY_ROOT="${QUERY_ROOT:-/queries}"
[[ -d "$QUERY_ROOT" ]] || QUERY_ROOT="$SCRIPT_DIR/../../queries/age_maintenance"
RESULTS_BASE="${RESULTS_BASE:-/results}"
RUN_TS="$(date -u +%Y%m%d_%H%M%S)"
OUTPUT_DIR="${OUTPUT_DIR:-$RESULTS_BASE/age_maintenance/$RUN_TS}"
CSV_FILE="$OUTPUT_DIR/runtimes.csv"
ERROR_DIR="$OUTPUT_DIR/errors"

QUERY_FILTER="${QUERY_FILTER:-}"
DATASET_FILTER="${DATASET_FILTER:-}"
RUNS="${RUNS:-1}"
WARMUP="${WARMUP:-0}"
TIMEOUT_MS="${TIMEOUT_MS:-3600000}"
SAVE_QUERIES="${SAVE_QUERIES:-0}"
SAVE_PLANS="${SAVE_PLANS:-0}"
SAVE_RESULTS="${SAVE_RESULTS:-0}"
TIMING_OFF="${TIMING_OFF:-0}"
VACUUM_BETWEEN_RUNS="${VACUUM_BETWEEN_RUNS:-1}"
COMPACT_BEFORE="${COMPACT_BEFORE:-0}"
NOTE="${NOTE:-}"
PARAMETERS_FILE="${PARAMETERS_FILE:-/config/maintenance_parameters.csv}"
[[ -f "$PARAMETERS_FILE" ]] || PARAMETERS_FILE="$SCRIPT_DIR/../maintenance_parameters.csv"

usage() {
	cat <<'EOF'
Usage: run_experiments.sh [OPTIONS]

Options:
  -q, --queries LIST    IDs, names, or globs (01..04).
  -d, --datasets LIST   Base graph names or globs.
  -r, --runs N          Measured runs per operation (default: 1).
  -w, --warmup          Run one rolled-back warmup first.
  -t, --timeout-ms N    Statement timeout (default: 3600000).
  -n, --note TEXT       Append a run note.
      --parameters-file FILE
                        Root parameters (default: maintenance_parameters.csv).
      --save-queries    Save rendered SQL.
      --save-plans      Save EXPLAIN ANALYZE plans (rolled back).
      --save-results    Save command results (rolled back).
      --timing-off      Use TIMING OFF for saved plans.
      --compact-before  Run VACUUM FULL ANALYZE once on every selected tree
                        node/edge table before measuring. Use this to recover
                        an already bloated maintenance container.
      --no-vacuum-between-runs
                        Do not VACUUM changed tables after rolled-back runs
                        (default: vacuum after every execution).
  -h, --help            Show this help.

Every execution runs in a transaction and is rolled back. Setup and rollback
are excluded from runtime_ms.

ENV:
  QUERY_FILTER, DATASET_FILTER, RUNS, WARMUP, TIMEOUT_MS, NOTE,
  SAVE_QUERIES, SAVE_PLANS, SAVE_RESULTS, TIMING_OFF, VACUUM_BETWEEN_RUNS,
  COMPACT_BEFORE, PARAMETERS_FILE, QUERY_ROOT, RESULTS_BASE, OUTPUT_DIR.
EOF
}

while (($#)); do
	case "$1" in
		-q|--queries) QUERY_FILTER="${2:-}"; shift 2 ;;
		-d|--datasets) DATASET_FILTER="${2:-}"; shift 2 ;;
		-r|--runs) RUNS="${2:-}"; shift 2 ;;
		-w|--warmup) WARMUP=1; shift ;;
		-t|--timeout-ms) TIMEOUT_MS="${2:-}"; shift 2 ;;
		-n|--note) NOTE="${2:-}"; shift 2 ;;
		--parameters-file) PARAMETERS_FILE="${2:-}"; shift 2 ;;
		--save-queries) SAVE_QUERIES=1; shift ;;
		--save-plans) SAVE_PLANS=1; shift ;;
		--save-results) SAVE_RESULTS=1; shift ;;
		--timing-off) TIMING_OFF=1; shift ;;
		--compact-before) COMPACT_BEFORE=1; shift ;;
		--no-vacuum-between-runs) VACUUM_BETWEEN_RUNS=0; shift ;;
		-h|--help) usage; exit 0 ;;
		*) echo "Unknown option: $1" >&2; usage >&2; exit 1 ;;
	esac
done

[[ "$RUNS" =~ ^[1-9][0-9]*$ ]] || { echo "Invalid runs: $RUNS" >&2; exit 1; }
[[ "$TIMEOUT_MS" =~ ^[0-9]+$ ]] || { echo "Invalid timeout: $TIMEOUT_MS" >&2; exit 1; }
for flag_name in WARMUP SAVE_QUERIES SAVE_PLANS SAVE_RESULTS TIMING_OFF VACUUM_BETWEEN_RUNS COMPACT_BEFORE; do
	flag_value="${!flag_name}"
	[[ "$flag_value" == 0 || "$flag_value" == 1 ]] || {
		echo "Invalid $flag_name value: $flag_value (expected 0 or 1)" >&2
		exit 1
	}
done
[[ -f "$PARAMETERS_FILE" ]] || { echo "Parameter CSV not found: $PARAMETERS_FILE" >&2; exit 1; }

POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_DB="${POSTGRES_DB:-postgresDB}"
POSTGRES_USER="${POSTGRES_USER:-postgresUser}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-postgresPW}"
export PGPASSWORD="$POSTGRES_PASSWORD"
export PGOPTIONS="${PGOPTIONS:-} -c statement_timeout=$TIMEOUT_MS"
PSQL=(psql -X -v ON_ERROR_STOP=1 -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB")
PSQL_AT=("${PSQL[@]}" -At)

if ! "${PSQL_AT[@]}" -c "SELECT 1 FROM ag_catalog.ag_graph LIMIT 1" >/dev/null 2>&1; then
	echo "AGE maintenance is not initialized or PostgreSQL is unavailable." >&2
	echo "Check: docker compose -f docker/age_maintenance/docker-compose.yml ps" >&2
	echo "Logs:  docker compose -f docker/age_maintenance/docker-compose.yml logs -f age_treebench_maintenance" >&2
	exit 0
fi

if ! "${PSQL[@]}" -c "CREATE EXTENSION IF NOT EXISTS pg_hint_plan;" >/dev/null 2>&1; then
	echo "WARNING: pg_hint_plan extension could not be enabled; continuing." >&2
fi

mkdir -p "$OUTPUT_DIR" "$ERROR_DIR"
cp "$PARAMETERS_FILE" "$OUTPUT_DIR/parameters.csv"
((SAVE_QUERIES)) && mkdir -p "$OUTPUT_DIR/queries"
((SAVE_PLANS)) && mkdir -p "$OUTPUT_DIR/plans"
if ((SAVE_RESULTS)); then
	mkdir -p "$OUTPUT_DIR/results" "$OUTPUT_DIR/diffs"
	printf '# empty results\n' > "$OUTPUT_DIR/empty_results.log"
fi
printf 'graph,query,run,runtime_ms\n' > "$CSV_FILE"

if [[ -n "${NOTE//[[:space:]]/}" ]]; then
	NOTES_FILE="$RESULTS_BASE/age_maintenance/notes.txt"
	mkdir -p "$(dirname "$NOTES_FILE")"
	notes_ts="${RUN_TS:6:2}-${RUN_TS:4:2}-${RUN_TS:0:4} ${RUN_TS:9:2}:${RUN_TS:11:2}:${RUN_TS:13:2}"
	printf '%s %s\n' "$notes_ts" "$NOTE" >> "$NOTES_FILE"
fi

# This is idempotent and happens before any measurement.
"${PSQL[@]}" -f "$SCRIPT_DIR/prepare_constraints.sql" >/dev/null

matches_filter() {
	local value="$1" filter="$2" token
	[[ -z "$filter" ]] && return 0
	IFS=',' read -ra tokens <<< "$filter"
	for token in "${tokens[@]}"; do
		token="${token//[[:space:]]/}"
		[[ -n "$token" && "$value" == $token ]] && return 0
	done
	return 1
}

declare -A PARAMETERS
load_parameters() {
	local header graph query parameter value extra key line=1
	IFS= read -r header < "$PARAMETERS_FILE" || true
	[[ "$header" == "graph,query,parameter,value" ]] || {
		echo "Invalid parameter header in $PARAMETERS_FILE" >&2; exit 1;
	}
	while IFS=',' read -r graph query parameter value extra; do
		((line+=1)); value="${value//$'\r'/}"
		[[ -z "$graph" ]] && continue
		if [[ -n "$extra" || "$parameter" != rootid || ! "$value" =~ ^[0-9]+$ ]]; then
			echo "Invalid parameter row $line in $PARAMETERS_FILE" >&2; exit 1
		fi
		key="$graph|$query|$parameter"
		[[ -z "${PARAMETERS[$key]+x}" ]] || { echo "Duplicate parameter: $key" >&2; exit 1; }
		PARAMETERS["$key"]="$value"
	done < <(tail -n +2 "$PARAMETERS_FILE")
}

load_parameters

mapfile -t GRAPHS < <("${PSQL_AT[@]}" -c "SELECT name FROM ag_catalog.ag_graph WHERE name ~ '_(baseline|dewey|prepost)$' ORDER BY name")
mapfile -t QUERY_FILES < <(find "$QUERY_ROOT/baseline" -maxdepth 1 -name '[0-9][0-9]_*.sql' -printf '%f\n' | sort)

if [[ ${#GRAPHS[@]} -eq 0 ]]; then
	echo "No initialized AGE maintenance graphs found." >&2
	exit 0
fi
if [[ ${#QUERY_FILES[@]} -eq 0 ]]; then
	echo "No maintenance queries found in $QUERY_ROOT/baseline." >&2
	exit 0
fi

sql_literal() {
	local value="$1"
	printf '%s' "${value//\'/\'\'}"
}

require_identifier() {
	local value="$1"
	[[ "$value" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || {
		echo "Unsafe SQL identifier: $value" >&2
		return 1
	}
}

resolve_tree_label() {
	local graph="$1" column="$2" graph_sql column_sql expected_sql
	local expected=""
	case "${graph,,}" in
		*_comment_*) expected=Comment ;;
		*_place_*) expected=Place ;;
		*_tagclass_*) expected=TagClass ;;
	esac
	graph_sql="$(sql_literal "$graph")"
	column_sql="$(sql_literal "$column")"
	expected_sql="$(sql_literal "$expected")"
	"${PSQL_AT[@]}" -c "
SELECT l.name
FROM ag_catalog.ag_graph g
JOIN ag_catalog.ag_label l ON l.graph=g.graphid AND l.kind='v'
JOIN information_schema.columns c ON c.table_schema=g.name AND c.table_name=l.name
WHERE g.name='$graph_sql' AND l.name !~ '^_ag_'
  AND ('$expected_sql' = '' OR l.name = '$expected_sql')
GROUP BY l.name
HAVING bool_or(c.column_name='$column_sql')
ORDER BY l.name LIMIT 1;"
}

resolve_tree_edge() {
	local graph="$1" label="$2" expected="" graph_sql
	case "$label" in
		Comment) expected=REPLY_OF ;;
		Place) expected=IS_PART_OF ;;
		TagClass) expected=IS_SUBCLASS_OF ;;
	esac
	if [[ -n "$expected" ]]; then
		echo "$expected"
	else
		graph_sql="$(sql_literal "$graph")"
		"${PSQL_AT[@]}" -c "SELECT l.name FROM ag_catalog.ag_graph g JOIN ag_catalog.ag_label l ON l.graph=g.graphid AND l.kind='e' WHERE g.name='$graph_sql' AND l.name !~ '^_ag_' ORDER BY l.name LIMIT 1;"
	fi
}

vacuum_table() {
	local graph="$1" table="$2" mode="${3:-regular}"
	require_identifier "$graph" || return 1
	require_identifier "$table" || return 1
	if [[ "$mode" == full ]]; then
		# Docker's default /dev/shm is only 64 MiB. Parallel index rebuilding
		# during VACUUM FULL can exceed that limit on the large SNB labels.
		# Keep compaction serial; it is outside the measured runtime anyway.
		"${PSQL[@]}" -q \
			-c "SET max_parallel_maintenance_workers = 0;" \
			-c "VACUUM (FULL, ANALYZE) \"$graph\".\"$table\";" >/dev/null
	else
		# PARALLEL 0 also matters for regular VACUUM: parallel index cleanup
		# otherwise allocates a ~64 MiB DSM segment on the large SNB indexes.
		"${PSQL[@]}" -q \
			-c "VACUUM (ANALYZE FALSE, PARALLEL 0) \"$graph\".\"$table\";" >/dev/null
	fi
}

cleanup_after_rollback() {
	local graph="$1" label="$2" reltype="$3" include_edge="$4"
	((VACUUM_BETWEEN_RUNS)) || return 0
	vacuum_table "$graph" "$label"
	((include_edge)) && vacuum_table "$graph" "$reltype"
	return 0
}

render_query() {
	local source="$1" destination="$2" graph="$3" label="$4" reltype="$5" newid="$6" rootid="$7"
	sed -e "s/:\"graphname\"/\"${graph}\"/g" \
		-e "s/:'graphname'/'${graph}'/g" \
		-e "s/:\"nodetype\"/\"${label}\"/g" \
		-e "s/:\"reltype\"/\"${reltype}\"/g" \
		-e "s/:'nodetype'/'${label}'/g" \
		-e 's/\$NODE_TYPE/'"$label"'/g' \
		-e 's/\$REL_TYPE/'"$reltype"'/g' \
		-e 's/\$NEW_ID/'"$newid"'/g' \
		-e 's/\$ROOT_ID/'"$rootid"'/g' \
		-e "s/:rootid/${rootid}/g" \
		-e "s/:newid/${newid}/g" "$source" > "$destination"
}

measure_query() {
	local sql_file="$1" output_file="$2" timing_file raw
	timing_file="$(mktemp)"
	{
		echo 'BEGIN;'
		echo 'SET CONSTRAINTS ALL DEFERRED;'
		echo '\timing on'
		echo '\o /dev/null'
		cat "$sql_file"
		echo 'SET CONSTRAINTS ALL IMMEDIATE;'
		echo '\o'
		echo '\timing off'
		echo 'ROLLBACK;'
	} > "$timing_file"
	if ! raw=$("${PSQL[@]}" -f "$timing_file" 2>"$output_file"); then
		rm -f "$timing_file"
		if grep -Eqi 'statement timeout|canceling statement due to statement timeout' "$output_file"; then
			return 124
		fi
		return 1
	fi
	rm -f "$timing_file"
	awk '/^Time:/{found=1; value=$2; if($3=="s") value*=1000; else if($3!="ms") exit 1; total+=value} END {if(!found) exit 1; printf "%.3f",total}' <<< "$raw"
}

save_plan() {
	local sql_file="$1" destination="$2" mode="${3:-analyze}" prefix
	if [[ "$mode" == explain_only ]]; then
		prefix='EXPLAIN'
	else
		prefix='EXPLAIN (ANALYZE'
		((TIMING_OFF)) && prefix+=', TIMING OFF'
		prefix+=')'
	fi
	{
		echo 'BEGIN;'
		echo 'SET CONSTRAINTS ALL DEFERRED;'
		echo "$prefix"
		cat "$sql_file"
		echo 'SET CONSTRAINTS ALL IMMEDIATE;'
		echo 'ROLLBACK;'
	} | "${PSQL[@]}" > "$destination"
}

write_snapshot_sql() {
	local graph="$1" label="$2" reltype="$3" method="$4" include_edge="$5"
	printf "SELECT 'TABLE \"%s\".\"%s\"';\n" "$graph" "$label"
	case "$method" in
		baseline)
			printf 'SELECT id::text, properties FROM "%s"."%s" ORDER BY id;\n' "$graph" "$label"
			;;
		dewey)
			printf 'SELECT id::text, properties, depth::text, dewey FROM "%s"."%s" ORDER BY id;\n' "$graph" "$label"
			;;
		prepost)
			printf 'SELECT id::text, properties, depth::text, pre::text, post::text FROM "%s"."%s" ORDER BY id;\n' "$graph" "$label"
			;;
	esac
	if [[ "$include_edge" == 1 ]]; then
		printf "SELECT 'TABLE \"%s\".\"%s\"';\n" "$graph" "$reltype"
		printf 'SELECT id::text, start_id::text, end_id::text, properties FROM "%s"."%s" ORDER BY id;\n' "$graph" "$reltype"
	fi
}

save_table_state_and_diff() {
	local sql_file="$1" graph="$2" label="$3" reltype="$4" method="$5" include_edge="$6"
	local result_file="$7" diff_file="$8" error_file="$9"
	local before_file script_file diff_rc=0
	before_file="$(mktemp)"
	script_file="$(mktemp)"
	{
		echo '\pset tuples_only on'
		echo '\pset format unaligned'
		echo "\pset fieldsep '|'"
		echo 'BEGIN;'
		echo 'SET CONSTRAINTS ALL DEFERRED;'
		printf '\\o %s\n' "$before_file"
		write_snapshot_sql "$graph" "$label" "$reltype" "$method" "$include_edge"
		echo '\o /dev/null'
		cat "$sql_file"
		echo 'SET CONSTRAINTS ALL IMMEDIATE;'
		printf '\\o %s\n' "$result_file"
		write_snapshot_sql "$graph" "$label" "$reltype" "$method" "$include_edge"
		echo '\o'
		echo 'ROLLBACK;'
	} > "$script_file"
	if ! "${PSQL[@]}" -qAt -f "$script_file" >/dev/null 2>"$error_file"; then
		rm -f "$before_file" "$script_file"
		return 1
	fi
	diff -u --label before --label after "$before_file" "$result_file" > "$diff_file" || diff_rc=$?
	rm -f "$before_file" "$script_file"
	if [[ $diff_rc -gt 1 ]]; then
		printf 'Failed to create diff (exit %d).\n' "$diff_rc" >> "$error_file"
		return 1
	fi
	chmod 0644 "$result_file" "$diff_file"
}

FILTERED_GRAPHS=()
for graph in "${GRAPHS[@]}"; do
	base="${graph%_baseline}"; base="${base%_dewey}"; base="${base%_prepost}"
	if matches_filter "$base" "$DATASET_FILTER" || matches_filter "$graph" "$DATASET_FILTER"; then
		FILTERED_GRAPHS+=("$graph")
	fi
done
FILTERED_QUERIES=()
for query_file in "${QUERY_FILES[@]}"; do
	query="${query_file%.sql}"; query_id="${query%%_*}"
	if matches_filter "$query" "$QUERY_FILTER" || matches_filter "$query_id" "$QUERY_FILTER" || matches_filter "$query_file" "$QUERY_FILTER"; then
		FILTERED_QUERIES+=("$query_file")
	fi
done

if [[ ${#FILTERED_GRAPHS[@]} -eq 0 || ${#FILTERED_QUERIES[@]} -eq 0 ]]; then
	echo "Nothing to run: no graph/query combinations matched the filters." >&2
	exit 0
fi

total_jobs=$((${#FILTERED_GRAPHS[@]} * ${#FILTERED_QUERIES[@]}))
current_job=0
echo "Starting AGE maintenance experiments"
echo "  Output directory: $OUTPUT_DIR"
echo "  Graphs selected: ${#FILTERED_GRAPHS[@]}"
echo "  Queries selected: ${#FILTERED_QUERIES[@]}"
echo "  Total graph-query jobs: $total_jobs"

for graph in "${FILTERED_GRAPHS[@]}"; do
	base="${graph%_baseline}"; base="${base%_dewey}"; base="${base%_prepost}"
	method="${graph##*_}"
	column=id; [[ "$method" == dewey ]] && column=dewey; [[ "$method" == prepost ]] && column=pre
	label="$(resolve_tree_label "$graph" "$column")"
	[[ -n "$label" ]] || { echo "Skipping $graph: no tree label" >&2; continue; }
	require_identifier "$graph" || exit 1
	require_identifier "$label" || exit 1
	reltype="$(resolve_tree_edge "$graph" "$label")"
	[[ -n "$reltype" ]] || { echo "Skipping $graph: no tree edge label" >&2; continue; }
	require_identifier "$reltype" || exit 1
	if ((COMPACT_BEFORE)); then
		echo "[compact] graph=$graph node=$label edge=$reltype"
		vacuum_table "$graph" "$label" full
		vacuum_table "$graph" "$reltype" full
	fi
	newid=$("${PSQL_AT[@]}" -c "SELECT COALESCE(MAX((properties ->> '\"__id__\"'::agtype)::bigint),0)+1 FROM \"$graph\".\"$label\";")

	for query_file in "${FILTERED_QUERIES[@]}"; do
		query="${query_file%.sql}"; query_id="${query%%_*}"
		parameter_key="$base|$query|rootid"
		rootid="${PARAMETERS[$parameter_key]-}"
		if [[ "$query" == 01_insert_last_child_under_last_root || "$query" == 02_insert_first_child_under_first_root ]]; then
			[[ -n "$rootid" ]] || {
				echo "Skipping $base $query: missing rootid in $PARAMETERS_FILE" >&2
				continue
			}
		fi
		source="$QUERY_ROOT/$method/$query_file"
		[[ -f "$source" ]] || { echo "Missing query: $source" >&2; exit 1; }
		rendered="$(mktemp)"
		render_query "$source" "$rendered" "$graph" "$label" "$reltype" "$newid" "$rootid"
		artifact="${graph}__${query}"
		((SAVE_QUERIES)) && install -m 0644 "$rendered" "$OUTPUT_DIR/queries/$artifact.sql"
		((current_job+=1))

		if ((WARMUP)); then
			warmup_err="$ERROR_DIR/${artifact}_warmup.log"
			measure_query "$rendered" "$warmup_err" >/dev/null || true
			warmup_include_edge=0
			[[ "$query_id" == 01 || "$query_id" == 02 ]] && warmup_include_edge=1
			cleanup_after_rollback "$graph" "$label" "$reltype" "$warmup_include_edge"
			[[ -s "$warmup_err" ]] || rm -f "$warmup_err"
		fi
		runs_ok=0
		runs_failed=0
		runs_skipped=0
		timeout_on_first_run=0
		for ((run=1; run<=RUNS; run++)); do
			err="$ERROR_DIR/${artifact}_run${run}.log"
			run_include_edge=0
			[[ "$query_id" == 01 || "$query_id" == 02 ]] && run_include_edge=1
			if runtime=$(measure_query "$rendered" "$err"); then
				((runs_ok+=1))
				printf '%s,%s,%d,%s\n' "$graph" "$query" "$run" "$runtime" >> "$CSV_FILE"
				[[ -s "$err" ]] || rm -f "$err"
			else
				rc=$?
				((runs_failed+=1))
				printf '%s,%s,%d,\n' "$graph" "$query" "$run" >> "$CSV_FILE"
				if [[ $rc -eq 124 ]]; then
					printf 'Query timed out after %s ms.\n' "$TIMEOUT_MS" > "$err"
				else
					printf 'Failed to measure query.\n' >> "$err"
				fi
				if [[ $rc -eq 124 && $run -eq 1 ]]; then
					timeout_on_first_run=1
					for ((skip=2; skip<=RUNS; skip++)); do
						((runs_skipped+=1))
						printf '%s,%s,%d,\n' "$graph" "$query" "$skip" >> "$CSV_FILE"
						printf 'Skipped run %d: run 1 timed out after %s ms.\n' "$skip" "$TIMEOUT_MS" > "$ERROR_DIR/${artifact}_run${skip}.log"
					done
					cleanup_after_rollback "$graph" "$label" "$reltype" "$run_include_edge"
					break
				fi
			fi
			cleanup_after_rollback "$graph" "$label" "$reltype" "$run_include_edge"
		done

		plan_status=off
		if ((SAVE_PLANS)); then
			plan_mode=analyze; [[ $timeout_on_first_run -eq 1 ]] && plan_mode=explain_only
			if save_plan "$rendered" "$OUTPUT_DIR/plans/$artifact.plan.txt" "$plan_mode" 2>"$ERROR_DIR/${artifact}_plan.log"; then
				plan_status=saved
				[[ -s "$ERROR_DIR/${artifact}_plan.log" ]] || rm -f "$ERROR_DIR/${artifact}_plan.log"
			else
				plan_status=failed
			fi
			if [[ "$plan_mode" == analyze ]]; then
				plan_include_edge=0
				[[ "$query_id" == 01 || "$query_id" == 02 ]] && plan_include_edge=1
				cleanup_after_rollback "$graph" "$label" "$reltype" "$plan_include_edge"
			fi
		fi

		result_status=off
		if ((SAVE_RESULTS)) && [[ $timeout_on_first_run -eq 0 ]]; then
			result_file="$OUTPUT_DIR/results/$artifact.results.txt"
			diff_file="$OUTPUT_DIR/diffs/$artifact.diff.txt"
			result_err="$ERROR_DIR/${artifact}_result.log"
			include_edge=0
			[[ "$query_id" == 01 || "$query_id" == 02 ]] && include_edge=1
			if save_table_state_and_diff "$rendered" "$graph" "$label" "$reltype" "$method" "$include_edge" "$result_file" "$diff_file" "$result_err"; then
				result_status=saved
				[[ -s "$result_err" ]] || rm -f "$result_err"
				if [[ ! -s "$result_file" ]]; then
					printf 'graph=%s query=%s result=%s\n' "$graph" "$query" "$result_file" >> "$OUTPUT_DIR/empty_results.log"
				fi
			else
				result_status=failed
			fi
			cleanup_after_rollback "$graph" "$label" "$reltype" "$include_edge"
		elif ((SAVE_RESULTS)); then
			result_status='skipped(timeout)'
			printf 'Skipped result execution: run 1 timed out after %s ms.\n' "$TIMEOUT_MS" > "$ERROR_DIR/${artifact}_result.log"
		fi
		rm -f "$rendered"
		echo "[$current_job/$total_jobs] graph=$graph method=$method query=$query runs_ok=$runs_ok runs_failed=$runs_failed runs_skipped=$runs_skipped plan=$plan_status result=$result_status"
	done
done

echo "Done. Results CSV: $CSV_FILE"
