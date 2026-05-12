#!/usr/bin/env bash
# Periodic whitelist refresh + shard split migration.
#
# 1. Refresh shard_split_subdomain from live counts (router can stay up,
#    it only re-reads the table on restart).
# 2. Pause scheduler_ingest (router must not race the row migration).
# 3. Move rows for any newly-whitelisted hosts (pending only, deadline-bounded).
# 4. Restart scheduler_ingest, which now picks up the new whitelist.
#
# Restart is in a trap so the container comes back even if a step fails or
# the deadline fires. Hosts not finished within the deadline keep
# migrated_at = NULL and resume on the next run.

set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO"

LOG_DIR="$HOME/logs"
mkdir -p "$LOG_DIR"
LOG="$LOG_DIR/refresh_split_whitelist.log"
exec >>"$LOG" 2>&1

# Downtime budget for scheduler_ingest. The Python script polices itself via
# --deadline-sec; the outer `timeout` is the hard backstop.
MIGRATE_DEADLINE_SEC="${MIGRATE_DEADLINE_SEC:-1800}"
MIGRATE_HARD_TIMEOUT_SEC="$((MIGRATE_DEADLINE_SEC + 120))"

echo "=== $(date -Iseconds) start ==="

uv run scripts/refresh_split_whitelist.py --top-error-rate 10 --execute

echo "=== $(date -Iseconds) pausing scheduler_ingest ==="
docker stop scheduler_ingest
trap 'echo "=== $(date -Iseconds) restarting scheduler_ingest ==="; docker start scheduler_ingest' EXIT

timeout "${MIGRATE_HARD_TIMEOUT_SEC}s" \
    uv run scripts/migrate_shard_split.py --execute \
        --deadline-sec "${MIGRATE_DEADLINE_SEC}" \
    || echo "=== $(date -Iseconds) migrate exited nonzero (deadline or error); resumable on next run ==="

echo "=== $(date -Iseconds) done ==="
