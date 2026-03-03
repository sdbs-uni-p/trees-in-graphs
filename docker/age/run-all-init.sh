#!/usr/bin/env bash
set -euo pipefail

INIT_ROOT="${INIT_ROOT:-/opt/age-init}"
PGDATA_DIR="${PGDATA:-/var/lib/postgresql/data/pgdata}"
INIT_STATE_DIR="${INIT_STATE_DIR:-${PGDATA_DIR}/.init_state}"
COMPLETE_MARKER="${PGDATA_DIR}/.init_complete"

SCRIPTS=(
  "00_init-age.sh"
  "10_create_graphs.sh"
  "20_load_data.sh"
  "30_add_tree_indexes.sh"
  "99_init_complete.sh"
)

mkdir -p "$INIT_STATE_DIR"

if [[ -f "$COMPLETE_MARKER" ]]; then
  echo "[init-runner] complete marker already present: $COMPLETE_MARKER"
  exit 0
fi

for script_name in "${SCRIPTS[@]}"; do
  script_path="$INIT_ROOT/$script_name"
  done_marker="$INIT_STATE_DIR/$script_name.done"

  if [[ -f "$done_marker" ]]; then
    echo "[init-runner] skipping $script_name (already done)"
    continue
  fi

  if [[ ! -f "$script_path" ]]; then
    echo "[init-runner] ERROR missing script: $script_path" >&2
    exit 1
  fi

  echo "[init-runner] running $script_name"
  bash "$script_path"
  touch "$done_marker"
done

if [[ ! -f "$COMPLETE_MARKER" ]]; then
  echo "[init-runner] WARNING completion marker missing after script chain; creating fallback marker"
  touch "$COMPLETE_MARKER"
fi

echo "[init-runner] initialization chain finished"
