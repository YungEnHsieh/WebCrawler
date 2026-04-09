#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <fixed|autothrottle>" >&2
  exit 1
fi

MODE="$1"
if [[ "$MODE" != "fixed" && "$MODE" != "autothrottle" ]]; then
  echo "mode must be 'fixed' or 'autothrottle'" >&2
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

BENCHMARK_LABEL="${BENCHMARK_LABEL:-bench-$(date -u +"%Y%m%dT%H%M%SZ")}"
BENCHMARK_HOURS="${BENCHMARK_HOURS:-6}"
BENCHMARK_SECONDS="${BENCHMARK_SECONDS:-}"
BENCHMARK_RESET="${BENCHMARK_RESET:-false}"
BENCHMARK_DOMAIN_QPS_LOG_ENABLED="${BENCHMARK_DOMAIN_QPS_LOG_ENABLED:-true}"
BENCHMARK_DOMAIN_QPS_LOG_INTERVAL="${BENCHMARK_DOMAIN_QPS_LOG_INTERVAL:-1.0}"
BENCHMARK_DOMAIN_QPS_WINDOW_SECONDS="${BENCHMARK_DOMAIN_QPS_WINDOW_SECONDS:-5.0}"

SMALL_URL_FILE="${SMALL_URL_FILE:-/app/seeds/urls.txt}"
SMALL_NUM_SHARDS="${SMALL_NUM_SHARDS:-256}"
SMALL_SHARDS_PER_INGESTOR="${SMALL_SHARDS_PER_INGESTOR:-256}"
SMALL_INGEST_SERVICE="${SMALL_INGEST_SERVICE:-scheduler_ingest}"
SMALL_POSTGRES_MAX_CONNECTIONS="${SMALL_POSTGRES_MAX_CONNECTIONS:-50}"
SMALL_POSTGRES_MAX_WAL_SIZE="${SMALL_POSTGRES_MAX_WAL_SIZE:-1GB}"
SMALL_POSTGRES_CHECKPOINT_TIMEOUT="${SMALL_POSTGRES_CHECKPOINT_TIMEOUT:-5min}"
SMALL_SCHEDULER_CONTROL_CONFIG="${SMALL_SCHEDULER_CONTROL_CONFIG:-/app/containers/scheduler_control/config/control.small.yaml}"
SMALL_SCHEDULER_INGEST_CONFIG="${SMALL_SCHEDULER_INGEST_CONFIG:-/app/containers/scheduler_ingest/config/ingest.small.yaml}"
SMALL_SCHEDULER_CONTROL_OFFERER_PROCS="${SMALL_SCHEDULER_CONTROL_OFFERER_PROCS:-1}"
SMALL_SCHEDULER_INGEST_ROUTER_PROCS="${SMALL_SCHEDULER_INGEST_ROUTER_PROCS:-1}"
SMALL_SCHEDULER_INGEST_INGESTOR_PROCS="${SMALL_SCHEDULER_INGEST_INGESTOR_PROCS:-1}"
SMALL_SCHEDULER_INGEST_EXTRACTOR_PROCS="${SMALL_SCHEDULER_INGEST_EXTRACTOR_PROCS:-1}"
SMALL_CRAWLER_NUMPROCS="${SMALL_CRAWLER_NUMPROCS:-1}"
SMALL_CRAWLER_CONCURRENT_REQUESTS="${SMALL_CRAWLER_CONCURRENT_REQUESTS:-8}"
SMALL_CRAWLER_CONCURRENT_REQUESTS_PER_DOMAIN="${SMALL_CRAWLER_CONCURRENT_REQUESTS_PER_DOMAIN:-2}"
SMALL_CRAWLER_DOWNLOAD_DELAY="${SMALL_CRAWLER_DOWNLOAD_DELAY:-1.0}"
SMALL_CRAWLER_AUTOTHROTTLE_START_DELAY="${SMALL_CRAWLER_AUTOTHROTTLE_START_DELAY:-1.0}"
SMALL_CRAWLER_AUTOTHROTTLE_MAX_DELAY="${SMALL_CRAWLER_AUTOTHROTTLE_MAX_DELAY:-15.0}"
SMALL_CRAWLER_AUTOTHROTTLE_TARGET_CONCURRENCY="${SMALL_CRAWLER_AUTOTHROTTLE_TARGET_CONCURRENCY:-1.0}"
SMALL_CRAWLER_LOG_LEVEL="${SMALL_CRAWLER_LOG_LEVEL:-WARNING}"

if [[ -n "$BENCHMARK_SECONDS" ]]; then
  DURATION_SECONDS="$BENCHMARK_SECONDS"
else
  DURATION_SECONDS="$(( BENCHMARK_HOURS * 3600 ))"
fi

if ! [[ "$DURATION_SECONDS" =~ ^[0-9]+$ ]] || [[ "$DURATION_SECONDS" -le 0 ]]; then
  echo "benchmark duration must be a positive integer number of seconds" >&2
  exit 1
fi

SLUG="$(printf '%s' "${BENCHMARK_LABEL}-${MODE}" | tr '[:upper:]' '[:lower:]' | tr -cs 'a-z0-9' '-')"
SLUG="${SLUG#-}"
SLUG="${SLUG%-}"
if [[ -z "$SLUG" ]]; then
  SLUG="benchmark"
fi

BENCHMARK_ROOT="data/benchmarks/${BENCHMARK_LABEL}/${MODE}"
HOST_POSTGRES_DIR="${ROOT_DIR}/${BENCHMARK_ROOT}/postgres"
HOST_IPC_DIR="${ROOT_DIR}/${BENCHMARK_ROOT}/ipc"
HOST_SUMMARY_DIR="${ROOT_DIR}/${BENCHMARK_ROOT}/summary"

if [[ -d "${ROOT_DIR}/${BENCHMARK_ROOT}" ]] && [[ "$BENCHMARK_RESET" == "true" ]]; then
  rm -rf "${ROOT_DIR:?}/${BENCHMARK_ROOT}"
elif [[ -d "${ROOT_DIR}/${BENCHMARK_ROOT}" ]] && [[ -n "$(find "${ROOT_DIR}/${BENCHMARK_ROOT}" -mindepth 1 -maxdepth 1 2>/dev/null)" ]]; then
  echo "benchmark output already exists: ${BENCHMARK_ROOT}" >&2
  echo "choose a new BENCHMARK_LABEL or set BENCHMARK_RESET=true" >&2
  exit 1
fi

export COMPOSE_PROJECT_NAME="${SLUG}"
export POSTGRES_CONTAINER_NAME="${SLUG}-postgres"
export SCHEDULER_CONTROL_CONTAINER_NAME="${SLUG}-scheduler-control"
export SCHEDULER_INGEST_CONTAINER_NAME="${SLUG}-scheduler-ingest"
export CRAWLER_CONTAINER_NAME="${SLUG}-crawler"
export DATA_POSTGRES_DIR="${HOST_POSTGRES_DIR}"
export DATA_IPC_DIR="${HOST_IPC_DIR}"
export POSTGRES_MAX_CONNECTIONS="${SMALL_POSTGRES_MAX_CONNECTIONS}"
export POSTGRES_MAX_WAL_SIZE="${SMALL_POSTGRES_MAX_WAL_SIZE}"
export POSTGRES_CHECKPOINT_TIMEOUT="${SMALL_POSTGRES_CHECKPOINT_TIMEOUT}"
export SCHEDULER_CONTROL_CONFIG="${SMALL_SCHEDULER_CONTROL_CONFIG}"
export SCHEDULER_INGEST_CONFIG="${SMALL_SCHEDULER_INGEST_CONFIG}"
export SCHEDULER_CONTROL_OFFERER_PROCS="${SMALL_SCHEDULER_CONTROL_OFFERER_PROCS}"
export SCHEDULER_INGEST_ROUTER_PROCS="${SMALL_SCHEDULER_INGEST_ROUTER_PROCS}"
export SCHEDULER_INGEST_INGESTOR_PROCS="${SMALL_SCHEDULER_INGEST_INGESTOR_PROCS}"
export SCHEDULER_INGEST_EXTRACTOR_PROCS="${SMALL_SCHEDULER_INGEST_EXTRACTOR_PROCS}"
export CRAWLER_NUMPROCS="${SMALL_CRAWLER_NUMPROCS}"
export CRAWLER_CONCURRENT_REQUESTS="${SMALL_CRAWLER_CONCURRENT_REQUESTS}"
export CRAWLER_CONCURRENT_REQUESTS_PER_DOMAIN="${SMALL_CRAWLER_CONCURRENT_REQUESTS_PER_DOMAIN}"
export CRAWLER_DOWNLOAD_DELAY="${SMALL_CRAWLER_DOWNLOAD_DELAY}"
export CRAWLER_AUTOTHROTTLE_START_DELAY="${SMALL_CRAWLER_AUTOTHROTTLE_START_DELAY}"
export CRAWLER_AUTOTHROTTLE_MAX_DELAY="${SMALL_CRAWLER_AUTOTHROTTLE_MAX_DELAY}"
export CRAWLER_AUTOTHROTTLE_TARGET_CONCURRENCY="${SMALL_CRAWLER_AUTOTHROTTLE_TARGET_CONCURRENCY}"
export CRAWLER_LOG_LEVEL="${SMALL_CRAWLER_LOG_LEVEL}"
export CRAWLER_DOMAIN_QPS_LOG_ENABLED="${BENCHMARK_DOMAIN_QPS_LOG_ENABLED}"
export CRAWLER_DOMAIN_QPS_LOG_INTERVAL="${BENCHMARK_DOMAIN_QPS_LOG_INTERVAL}"
export CRAWLER_DOMAIN_QPS_WINDOW_SECONDS="${BENCHMARK_DOMAIN_QPS_WINDOW_SECONDS}"

if [[ "$MODE" == "fixed" ]]; then
  export CRAWLER_USE_AUTOTHROTTLE="false"
else
  export CRAWLER_USE_AUTOTHROTTLE="true"
fi

cleanup() {
  docker compose down --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker compose down --remove-orphans >/dev/null 2>&1 || true
mkdir -p "$HOST_POSTGRES_DIR" "$HOST_IPC_DIR" "$HOST_SUMMARY_DIR"

printf '%s\n' "benchmark_root: ${BENCHMARK_ROOT}"
printf '%s\n' "mode: ${MODE}"
printf '%s\n' "duration_seconds: ${DURATION_SECONDS}"
printf '%s\n' "compose_project: ${COMPOSE_PROJECT_NAME}"

docker compose up -d postgres

docker compose run --rm "${SMALL_INGEST_SERVICE}" \
  python /app/scripts/init_db.py \
  --dsn "postgresql+psycopg2://crawler:crawler@postgres:5432/crawlerdb" \
  --num-shards "${SMALL_NUM_SHARDS}"

docker compose run --rm "${SMALL_INGEST_SERVICE}" \
  python /app/scripts/seed_urls.py \
  --dsn "postgresql+psycopg2://crawler:crawler@postgres:5432/crawlerdb" \
  --file "${SMALL_URL_FILE}" \
  --num-shards "${SMALL_NUM_SHARDS}" \
  --shards-per-ingestor "${SMALL_SHARDS_PER_INGESTOR}"

docker compose run --rm "${SMALL_INGEST_SERVICE}" \
  python /app/scripts/repair_crawl_flags.py \
  --dsn "postgresql+psycopg2://crawler:crawler@postgres:5432/crawlerdb" \
  --num-shards "${SMALL_NUM_SHARDS}"

START_TS="$(date -u +"%Y-%m-%dT%H:%M:%S+00:00")"
printf '%s\n' "start_utc: ${START_TS}"

docker compose up -d scheduler_control scheduler_ingest crawler

sleep "${DURATION_SECONDS}"

END_TS="$(date -u +"%Y-%m-%dT%H:%M:%S+00:00")"
printf '%s\n' "end_utc: ${END_TS}"

docker compose logs --no-color > "${HOST_SUMMARY_DIR}/compose.log" 2>&1 || true

python3 scripts/summarize_crawl_results.py \
  --root "${HOST_IPC_DIR}/crawl_result" \
  --since "${START_TS}" \
  --until "${END_TS}" \
  --label "${BENCHMARK_LABEL}-${MODE}" \
  | tee "${HOST_SUMMARY_DIR}/crawl_summary.txt"

cat > "${HOST_SUMMARY_DIR}/benchmark_meta.txt" <<EOF
label=${BENCHMARK_LABEL}
mode=${MODE}
compose_project=${COMPOSE_PROJECT_NAME}
benchmark_root=${BENCHMARK_ROOT}
start_utc=${START_TS}
end_utc=${END_TS}
duration_seconds=${DURATION_SECONDS}
url_file=${SMALL_URL_FILE}
num_shards=${SMALL_NUM_SHARDS}
shards_per_ingestor=${SMALL_SHARDS_PER_INGESTOR}
offerer_procs=${SMALL_SCHEDULER_CONTROL_OFFERER_PROCS}
router_procs=${SMALL_SCHEDULER_INGEST_ROUTER_PROCS}
ingestor_procs=${SMALL_SCHEDULER_INGEST_INGESTOR_PROCS}
extractor_procs=${SMALL_SCHEDULER_INGEST_EXTRACTOR_PROCS}
crawler_numprocs=${SMALL_CRAWLER_NUMPROCS}
crawler_concurrent_requests=${SMALL_CRAWLER_CONCURRENT_REQUESTS}
crawler_concurrent_requests_per_domain=${SMALL_CRAWLER_CONCURRENT_REQUESTS_PER_DOMAIN}
crawler_download_delay=${SMALL_CRAWLER_DOWNLOAD_DELAY}
crawler_autothrottle_start_delay=${SMALL_CRAWLER_AUTOTHROTTLE_START_DELAY}
crawler_autothrottle_max_delay=${SMALL_CRAWLER_AUTOTHROTTLE_MAX_DELAY}
crawler_autothrottle_target_concurrency=${SMALL_CRAWLER_AUTOTHROTTLE_TARGET_CONCURRENCY}
domain_qps_log_enabled=${BENCHMARK_DOMAIN_QPS_LOG_ENABLED}
domain_qps_log_interval=${BENCHMARK_DOMAIN_QPS_LOG_INTERVAL}
domain_qps_window_seconds=${BENCHMARK_DOMAIN_QPS_WINDOW_SECONDS}
EOF

printf '%s\n' "crawl_summary: ${HOST_SUMMARY_DIR}/crawl_summary.txt"
printf '%s\n' "benchmark_meta: ${HOST_SUMMARY_DIR}/benchmark_meta.txt"
printf '%s\n' "compose_log: ${HOST_SUMMARY_DIR}/compose.log"
printf '%s\n' "domain_qps_metrics_root: ${HOST_IPC_DIR}/metrics"
