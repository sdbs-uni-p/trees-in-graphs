#!/usr/bin/env bash
# SPDX-License-Identifier: GPL-3.0-only

set -euo pipefail

UPSTREAM_ENTRYPOINT="/usr/local/bin/docker-entrypoint.sh"
PGDATA_DIR="${PGDATA:-/var/lib/postgresql/data/pgdata}"
COMPLETE_MARKER="${PGDATA_DIR}/.init_complete"

if [[ ! -x "$UPSTREAM_ENTRYPOINT" ]]; then
  echo "[entrypoint] ERROR upstream entrypoint not executable: $UPSTREAM_ENTRYPOINT" >&2
  exit 1
fi

echo "[entrypoint] starting upstream postgres entrypoint"
"$UPSTREAM_ENTRYPOINT" "$@" &
UPSTREAM_PID=$!

run_init_until_complete() {
  while [[ ! -f "$COMPLETE_MARKER" ]]; do
    if ! kill -0 "$UPSTREAM_PID" 2>/dev/null; then
      echo "[entrypoint] ERROR postgres process exited before init completion" >&2
      return 1
    fi

    until pg_isready -h 127.0.0.1 -p 5432 -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_DB:-postgres}" >/dev/null 2>&1; do
      if ! kill -0 "$UPSTREAM_PID" 2>/dev/null; then
        echo "[entrypoint] ERROR postgres process exited while waiting for readiness" >&2
        return 1
      fi
      sleep 1
    done

    echo "[entrypoint] postgres ready; executing resumable init chain"
    if /usr/local/bin/run-all-init.sh; then
      break
    fi

    echo "[entrypoint] init chain failed, retrying in 2s"
    sleep 2
  done
}

if [[ -f "$COMPLETE_MARKER" ]]; then
  echo "[entrypoint] completion marker exists; skipping init chain"
else
  run_init_until_complete
fi

echo "[entrypoint] init complete; streaming postgres process"
wait "$UPSTREAM_PID"
