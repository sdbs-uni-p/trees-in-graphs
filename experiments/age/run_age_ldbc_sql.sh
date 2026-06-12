#!/usr/bin/env bash
# SPDX-License-Identifier: GPL-3.0-only

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [ -d "/queries" ]; then
  SQL_DIR="/queries/ldbc/sql"
  RESULTS_BASE="/results/age"
else
  SQL_DIR="${ROOT_DIR}/queries/age/ldbc/sql"
  RESULTS_BASE="${ROOT_DIR}/results/age"
fi

if [ ! -d "${SQL_DIR}" ]; then
  echo "SQL directory not found: ${SQL_DIR}" >&2
  exit 1
fi

TS="$(date -u +"%Y%m%d_%H%M%S")"
RUN_ID="${RUN_ID:-${TS}_pid$$}"
OUT_DIR="${RESULTS_BASE}/ldbc_sql_${RUN_ID}"
RESULTS_DIR="${OUT_DIR}/results"
PLANS_DIR="${OUT_DIR}/plans"
mkdir -p "${RESULTS_DIR}" "${PLANS_DIR}"

PSQL_BIN="${AGE_PSQL:-psql}"
POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_DB="${POSTGRES_DB:-postgresDB}"
POSTGRES_USER="${POSTGRES_USER:-postgresUser}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-postgresPW}"
TIMEOUT_MS="${TIMEOUT_MS:-3600000}"
EXPLAIN_OPTIONS="${EXPLAIN_OPTIONS:-ANALYZE, BUFFERS, FORMAT TEXT}"
ONLY_NAMES=()
ONLY_REGEXES=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    -t|--timeout-ms)
      TIMEOUT_MS="${2:-}"
      shift 2
      ;;
    --explain-options)
      EXPLAIN_OPTIONS="${2:-}"
      shift 2
      ;;
    -h|--help)
      echo "Usage: $0 [--timeout-ms N] [--only NAME]... [--only-regex REGEX]..." >&2
      echo "  --only NAME        Run only files whose base name matches NAME." >&2
      echo "  --only-regex REGEX Run only files whose base name matches REGEX (bash regex)." >&2
      echo "  --explain-options  EXPLAIN options string, e.g. 'ANALYZE, TIMING OFF'." >&2
      exit 0
      ;;
    --only)
      ONLY_NAMES+=("${2:-}")
      shift 2
      ;;
    --only-regex)
      ONLY_REGEXES+=("${2:-}")
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

if ! [[ "$TIMEOUT_MS" =~ ^[0-9]+$ ]]; then
  echo "Invalid TIMEOUT_MS value: $TIMEOUT_MS" >&2
  exit 1
fi

export PGPASSWORD="${POSTGRES_PASSWORD}"

PSQL_BASE=(
  "${PSQL_BIN}"
  -v ON_ERROR_STOP=1
  -h "${POSTGRES_HOST}"
  -p "${POSTGRES_PORT}"
  -U "${POSTGRES_USER}"
  -d "${POSTGRES_DB}"
  -At
)

GRAPHNAME="${GRAPHNAME:-snb_sf1_comment_baseline}"

COMMENT_ID="${COMMENT_ID:-1236950581249}"
PERSON_ID="${PERSON_ID:-10995116277795}"
PERSON_ID_IC12="${PERSON_ID_IC12:-10995116278009}"
TAG_CLASS_NAME="${TAG_CLASS_NAME:-Monarch}"
TAG_CLASS="${TAG_CLASS:-MusicalArtist}"
COUNTRY="${COUNTRY:-Burma}"
DATE_BI4="${DATE_BI4:-datetime('2010-01-29')}"
START_DATE_BI9="${START_DATE_BI9:-datetime('2011-10-01')}"
END_DATE_BI9="${END_DATE_BI9:-datetime('2011-10-15')}"
START_DATE_BI12="${START_DATE_BI12:-datetime('2010-07-22')}"
LENGTH_THRESHOLD="${LENGTH_THRESHOLD:-20}"
LANGUAGES="${LANGUAGES:-['ar','hu']}"
PERSON1_ID="${PERSON1_ID:-14}"
PERSON2_ID="${PERSON2_ID:-16}"
START_DATE_EPOCH_MS="${START_DATE_EPOCH_MS:-1288569600000}"
END_DATE_EPOCH_MS="${END_DATE_EPOCH_MS:-1291161600000}"
TAG="${TAG:-Slavoj_Zizek}"
DELTA="${DELTA:-4}"

run_query() {
  local sql_file="$1"
  local base
  base="$(basename "${sql_file}" .sql)"

  local result_file plan_file
  result_file="${RESULTS_DIR}/${base}.out"
  plan_file="${PLANS_DIR}/${base}.plan"
  local plan_stderr_file result_stderr_file
  plan_stderr_file="${PLANS_DIR}/${base}.stderr"
  result_stderr_file="${RESULTS_DIR}/${base}.stderr"

  local extra_vars=()
  case "${base}" in
    interactive-short-6)
      extra_vars=( -v commentId="${COMMENT_ID}" )
      ;;
    interactive-short-2)
      extra_vars=( -v personId="${PERSON_ID}" )
      ;;
    interactive-complex-12)
      extra_vars=( -v personId="${PERSON_ID_IC12}" -v tagClassName="${TAG_CLASS_NAME}" )
      ;;
    bi-3)
      extra_vars=( -v tagClass="${TAG_CLASS}" -v country="${COUNTRY}" )
      ;;
    bi-4)
      extra_vars=( -v date="${DATE_BI4}" )
      ;;
    bi-9)
      extra_vars=( -v startDate="${START_DATE_BI9}" -v endDate="${END_DATE_BI9}" )
      ;;
    bi-12)
      extra_vars=( -v startDate="${START_DATE_BI12}" -v lengthThreshold="${LENGTH_THRESHOLD}" -v languages="${LANGUAGES}" )
      ;;
    bi-15)
      extra_vars=( -v person1Id="${PERSON1_ID}" -v person2Id="${PERSON2_ID}" -v startDateEpochMillis="${START_DATE_EPOCH_MS}" -v endDateEpochMillis="${END_DATE_EPOCH_MS}" )
      ;;
    bi-17)
      extra_vars=( -v tag="${TAG}" -v delta="${DELTA}" )
      ;;
  esac

  echo "[run] ${base}"

  set +e
  local result_status plan_status
  local skip_result_run=false
  local plan_sql result_sql use_file_input=false
  local explain_stmt

  if [ "${base}" = "interactive-short-6" ]; then
    local rendered_sql
    rendered_sql="$(cat "${sql_file}")"
    rendered_sql="${rendered_sql//\$commentId/${COMMENT_ID}}"
    rendered_sql="${rendered_sql//:'graphname'/${GRAPHNAME}}"
    plan_sql="$(printf '%s\n' "${rendered_sql}" | sed -e 's/--.*$//' -e '/^[[:space:]]*$/d')"
    result_sql="${rendered_sql}"
  else
    plan_sql="$(sed -e 's/--.*$//' -e '/^[[:space:]]*$/d' "${sql_file}")"
    result_sql="${sql_file}"
    use_file_input=true
  fi

  if [ -n "${EXPLAIN_OPTIONS}" ]; then
    explain_stmt="EXPLAIN (${EXPLAIN_OPTIONS})"
  else
    explain_stmt="EXPLAIN"
  fi

  plan_start=$(date +%s%3N)
  {
    printf '%s\n' "${explain_stmt}"
    printf '%s\n' "${plan_sql}"
  } | PGOPTIONS="${PGOPTIONS:-} -c statement_timeout=${TIMEOUT_MS}" "${PSQL_BASE[@]}" -v graphname="${GRAPHNAME}" "${extra_vars[@]}" > "${plan_file}" 2> "${plan_stderr_file}"
  plan_status=$?
  plan_end=$(date +%s%3N)
  plan_ms=$((plan_end - plan_start))
  echo "[plan] ${base} status=${plan_status} time=${plan_ms}ms"

  if [ "${plan_status}" -ne 0 ] && grep -Eqi 'statement timeout|temp(_| )?file|temporary file|temp_file_limit' "${plan_stderr_file}"; then
    skip_result_run=true
    skip_reason="timeout/temp-file-limit"
  fi

  if [ "${skip_result_run}" = false ]; then
    result_start=$(date +%s%3N)
    if [ "${use_file_input}" = true ]; then
      PGOPTIONS="${PGOPTIONS:-} -c statement_timeout=${TIMEOUT_MS}" "${PSQL_BASE[@]}" -v graphname="${GRAPHNAME}" "${extra_vars[@]}" -f "${result_sql}" > "${result_file}" 2> "${result_stderr_file}"
    else
      PGOPTIONS="${PGOPTIONS:-} -c statement_timeout=${TIMEOUT_MS}" "${PSQL_BASE[@]}" -v graphname="${GRAPHNAME}" "${extra_vars[@]}" -c "${result_sql}" > "${result_file}" 2> "${result_stderr_file}"
    fi
    result_status=$?
    result_end=$(date +%s%3N)
    result_ms=$((result_end - result_start))
    echo "[result] ${base} status=${result_status} time=${result_ms}ms"
  else
    result_status=0
    echo "[skip] ${base} skipped result run (${skip_reason}), plan time=${plan_ms}ms"
  fi
  set -e

  if [ "$result_status" -ne 0 ] || [ "$plan_status" -ne 0 ]; then
    if [ "${skip_result_run}" = true ]; then
      echo "[warn] ${base} plan failed due to timeout/temp-file limit; skipping result run." >&2
    else
      echo "[warn] ${base} failed (result=${result_status}, plan=${plan_status}), continuing." >&2
    fi
  fi
}

for sql_file in "${SQL_DIR}"/*.sql; do
  base_name="$(basename "${sql_file}" .sql)"
  if [ "${#ONLY_NAMES[@]}" -gt 0 ] || [ "${#ONLY_REGEXES[@]}" -gt 0 ]; then
    local_match=false
    for name in "${ONLY_NAMES[@]}"; do
      if [ "${base_name}" = "${name}" ]; then
        local_match=true
        break
      fi
    done
    if [ "${local_match}" = false ]; then
      for regex in "${ONLY_REGEXES[@]}"; do
        if [[ "${base_name}" =~ ${regex} ]]; then
          local_match=true
          break
        fi
      done
    fi
    if [ "${local_match}" = false ]; then
      continue
    fi
  fi
  run_query "${sql_file}"
done

echo "Done. Results: ${RESULTS_DIR}"
echo "Plans:   ${PLANS_DIR}"
