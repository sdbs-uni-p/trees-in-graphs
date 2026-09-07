#!/usr/bin/env bash
# SPDX-License-Identifier: GPL-3.0-only

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
	cat <<'EOF'
Usage: run_experiments.sh [OPTIONS]

Options:
	-q, --queries LIST   Comma-separated query ids or filenames to run.
	                     Examples: interactive-short-2 or interactive-short-2.sql or interactive-*
	-d, --datasets LIST  Comma-separated dataset/base names or graph names to run.
	                     Examples: snb_sf1 or snb_sf1_baseline or snb*
	-n, --note TEXT      Optional note text to append to notes.txt for this run.
	-w, --warmup         Run one warmup execution per query before measurements.
	-r, --runs N         Number of measurement runs per query (default: 1).
	-t, --timeout-ms N   Statement timeout in ms (default: 3600000 = 1 hour).
	--save-plans         After runs, save one EXPLAIN ANALYZE plan per graph/query.
	--save-results       After runs, save one result output per graph/query.
	--save-queries       Persist the original SQL query files (default: off).
	-h, --help           Show this help.

ENV:
	QUERY_FILTER         Same as --queries / -q; CLI overrides ENV.
	DATASET_FILTER       Same as --datasets; CLI overrides ENV.
	WARMUP               If set to 1, run one warmup execution per query.
	RUNS                 Number of measurement runs per query.
	SAVE_PLANS           If set to 1, save one EXPLAIN ANALYZE plan.
	SAVE_RESULTS         If set to 1, save one result output per graph/query.
	SAVE_QUERIES         If set to 1, persist query files (default: 0).
	TIMEOUT_MS           Same as --timeout-ms; CLI overrides ENV.
	NOTE                 Optional note text (same as --note).
EOF
}

if [ -d "/queries/age_ldbc" ]; then
	QUERY_ROOT="/queries/age_ldbc"
else
	QUERY_ROOT="${SCRIPT_DIR}/../../queries/age_ldbc"
fi

RESULTS_BASE="${RESULTS_BASE:-/results}"
RUN_TS="$(date -u +"%Y%m%d_%H%M%S")"
OUTPUT_DIR="${RESULTS_BASE}/age_ldbc/${RUN_TS}"
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
TIMING_OFF="${TIMING_OFF:-0}"
TIMEOUT_MS="${TIMEOUT_MS:-3600000}"

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

print_not_initialized_hint() {
	echo "AGE LDBC is not fully initialized yet. Skipping experiment run." >&2
	echo "Check status: docker compose -f docker/age_ldbc/docker-compose.yml ps" >&2
	echo "View logs:    docker compose -f docker/age_ldbc/docker-compose.yml logs -f age_ldbc_treebench" >&2
}

is_age_ldbc_initialized() {
	if command -v docker >/dev/null 2>&1; then
		local health
		health="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' age_ldbc_treebench 2>/dev/null || true)"
		if [[ "$health" == "healthy" ]]; then
			return 0
		fi
		if [[ "$health" == "starting" || "$health" == "unhealthy" ]]; then
			return 1
		fi
	fi

	local graph_count
	graph_count="$(${PSQL_BASE[@]} -c "SELECT count(*)::int FROM ag_catalog.ag_graph WHERE name IN ('snb_sf1_baseline', 'snb_sf1_dewey', 'snb_sf1_prepost');")" || return 1
	[[ "$graph_count" -ge 3 ]]
}

if ! is_age_ldbc_initialized; then
	print_not_initialized_hint
	exit 0
fi

mkdir -p "$ERROR_DIR"
if [ "$SAVE_QUERIES" -eq 1 ]; then
	mkdir -p "$QUERIES_DIR"
fi

if [ -n "${NOTE//[[:space:]]/}" ]; then
	NOTES_FILE="${RESULTS_BASE}/age_ldbc/notes.txt"
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

# Record the effective CLI/ENV settings before measurements. Git may not be mounted in Docker.
metadata_commit="${GIT_COMMIT:-}"
if [[ -z "$metadata_commit" ]] && command -v git >/dev/null 2>&1; then
    metadata_commit="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse HEAD 2>/dev/null || true)"
fi
"${PSQL_BASE[@]}" -X -A -t -v ON_ERROR_STOP=1 \
    -v metadata_commit="$metadata_commit" -v runs="$RUNS" -v warmup="$WARMUP" \
    -v timeout_ms="$TIMEOUT_MS" -v query_filter="$QUERY_FILTER" \
    -v dataset_filter="$DATASET_FILTER" -v scenario_filter="${SCENARIO_FILTER:-}" \
    -v save_plans="$SAVE_PLANS" -v save_results="$SAVE_RESULTS" -v save_queries="$SAVE_QUERIES" \
    -v parameters_file="${PARAMETERS_FILE:-}" -v query_root="${QUERY_ROOT:-}" \
    -v note="$NOTE" > "$OUTPUT_DIR/metadata.json" <<'METADATA_SQL'
SELECT jsonb_pretty(jsonb_build_object(
    'schema_version', 1, 'created_at', CURRENT_TIMESTAMP, 'system', 'Apache AGE',
    'workload', 'ldbc', 'git_commit', NULLIF(:'metadata_commit', ''),
    'runs', :'runs'::integer, 'warmup_runs', :'warmup'::integer,
    'timeout_ms', :'timeout_ms'::bigint, 'timeout_source', 'runner statement_timeout',
    'settings', jsonb_build_object(
        'query_filter', :'query_filter', 'dataset_filter', :'dataset_filter',
        'scenario_filter', :'scenario_filter', 'parameters_file', :'parameters_file',
        'query_root', :'query_root', 'note', :'note',
        'save_plans', :'save_plans'::integer = 1,
        'save_results', :'save_results'::integer = 1,
        'save_queries', :'save_queries'::integer = 1
    )
));
METADATA_SQL


readarray -t GRAPHS < <("${PSQL_BASE[@]}" -c "SELECT name FROM ag_catalog.ag_graph WHERE name IN ('snb_sf1_baseline', 'snb_sf1_dewey', 'snb_sf1_prepost') ORDER BY name;")

if [ "${#GRAPHS[@]}" -eq 0 ]; then
	echo "No graphs found in ag_catalog.ag_graph" >&2
	exit 0
fi

resolve_base() {
	local graph="$1"
	local base="$graph"
	base="${base%_baseline}"
	base="${base%_dewey}"
	base="${base%_prepost}"
	echo "$base"
}

escape_filename() {
	echo "$1" | tr '/\\: ' '____'
}

parse_timing_ms() {
	awk '/^Time:/{val=$2; unit=$3} END {if (val == "") exit 1; if (unit == "ms") printf "%.3f", val; else if (unit == "s") printf "%.3f", (val * 1000); else exit 1}'
}

measure_psql_timing_ms_null() {
	local source_file="$1"
	local graph="$2"
	local timing_script
	timing_script="$(mktemp)"
	{
		echo "\\timing on"
		echo "\\o /dev/null"
		cat "$source_file"
		echo "\\o"
	} > "$timing_script"

	local timing_out
	timing_out="$(mktemp)"
	if ! ("${PSQL_BASE[@]}" -v graphname="$graph" -f "$timing_script" > "$timing_out" 2>&1); then
		if grep -Eqi 'statement timeout|canceling statement due to statement timeout' "$timing_out"; then
			rm -f "$timing_script" "$timing_out"
			return 124
		fi
		rm -f "$timing_script" "$timing_out"
		return 1
	fi

	local runtime_ms
	if ! runtime_ms="$(parse_timing_ms < "$timing_out")"; then
		rm -f "$timing_script" "$timing_out"
		return 1
	fi

	echo "$runtime_ms"
	rm -f "$timing_script" "$timing_out"
}

run_warmup() {
	local graph="$1"
	local source_file="$2"
	measure_psql_timing_ms_null "$source_file" "$graph" >/dev/null 2>&1 || true
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

run_plan() {
	local graph="$1"
	local query_set="$2"
	local query_file="$3"
	local source_file="$4"
	local explain_mode="${5:-analyze}"
	local query_base="${query_file%.sql}"
	local explain_file
	explain_file="$(mktemp)"
	build_explain_script "$source_file" "$explain_file" "$explain_mode"

	local err_file
	err_file="$ERROR_DIR/$(escape_filename "${graph}_${query_set}_${query_base}_plan").log"
	local plan_file
	plan_file="$PLAN_DIR/$(escape_filename "${graph}_${query_set}_${query_base}").plan.txt"
	local plan_err
	plan_err="$(mktemp)"

	if ! ("${PSQL_BASE[@]}" -v graphname="$graph" -f "$explain_file" > "$plan_file" 2> "$plan_err"); then
		cat "$plan_err" > "$err_file"
		rm -f "$explain_file" "$plan_err"
		return 1
	fi

	rm -f "$explain_file" "$plan_err"
}

run_results() {
	local graph="$1"
	local query_set="$2"
	local query_file="$3"
	local source_file="$4"
	local query_base="${query_file%.sql}"
	local err_file
	err_file="$ERROR_DIR/$(escape_filename "${graph}_${query_set}_${query_base}_result").log"
	local result_file
	result_file="$RESULT_DIR/$(escape_filename "${graph}_${query_set}_${query_base}").results.txt"
	local query_err
	query_err="$(mktemp)"

	if ! ("${PSQL_BASE[@]}" -v graphname="$graph" -f "$source_file" > "$result_file" 2> "$query_err"); then
		cat "$query_err" > "$err_file"
		rm -f "$query_err"
		return 1
	fi

	if [ ! -s "$result_file" ]; then
		echo "graph=$graph" >> "$EMPTY_RESULTS_LOG"
		echo "query=${query_set}/${query_file}" >> "$EMPTY_RESULTS_LOG"
		echo "result=$result_file" >> "$EMPTY_RESULTS_LOG"
		echo "" >> "$EMPTY_RESULTS_LOG"
	fi

	rm -f "$query_err"
}

declare -A graph_map
declare -A base_map

for graph in "${GRAPHS[@]}"; do
	graph_map["$graph"]=1
	base="$(resolve_base "$graph")"
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
	find "$QUERY_ROOT/baseline" -maxdepth 1 -type f -name "*.sql" -printf "%f\n" \
	| sort
)

if [ -n "$QUERY_FILTER" ]; then
	IFS=',' read -r -a FILTER_TOKENS <<< "$QUERY_FILTER"
	FILTERED_QUERY_FILES=()
	for query_file in "${QUERY_FILES[@]}"; do
		query_id="${query_file%.sql}"
		for token in "${FILTER_TOKENS[@]}"; do
			token="${token//[[:space:]]/}"
			[ -z "$token" ] && continue
			if [[ "$token" == *"*"* || "$token" == *"?"* ]]; then
				if [[ "$query_file" == $token || "$query_id" == $token ]]; then
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

echo "Starting AGE LDBC experiments"
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
			source_file="$QUERY_ROOT/$query_set/$query_file"

			if [ "$SAVE_QUERIES" -eq 1 ]; then
				graph_safe="$(escape_filename "$graph")"
				query_safe="$(escape_filename "${query_set}__${query_file}")"
				cp "$source_file" "$QUERIES_DIR/${graph_safe}__${query_safe}"
			fi

			if [ "$WARMUP" -eq 1 ]; then
				run_warmup "$graph" "$source_file"
			fi

			runs_ok=0
			runs_failed=0
			runs_skipped=0
			timeout_on_first_run=0
			for ((run_idx=1; run_idx<=RUNS; run_idx++)); do
				runtime=""
				if runtime="$(measure_psql_timing_ms_null "$source_file" "$graph")"; then
					((runs_ok+=1))
					echo "${graph},${query_file%.sql},${run_idx},${runtime}" >> "$CSV_FILE"
				else
					rc=$?
					((runs_failed+=1))
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

				if [[ "$timeout_on_first_run" -eq 1 ]]; then
					break
				fi
			done

			plan_status="off"
			if [ "$SAVE_PLANS" -eq 1 ]; then
				if [[ "$timeout_on_first_run" -eq 1 ]]; then
					if run_plan "$graph" "$query_set" "$query_file" "$source_file" "explain_only"; then
						plan_status="saved(explain-only)"
					else
						plan_status="failed(explain-only)"
					fi
				else
					if run_plan "$graph" "$query_set" "$query_file" "$source_file"; then
						plan_status="saved"
					else
						plan_status="failed"
					fi
				fi
			fi

			result_status="off"
			if [ "$SAVE_RESULTS" -eq 1 ] && [[ "$timeout_on_first_run" -eq 0 ]]; then
				if run_results "$graph" "$query_set" "$query_file" "$source_file"; then
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