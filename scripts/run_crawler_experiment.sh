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

EXPERIMENT_LABEL="${EXPERIMENT_LABEL:-quick}"
URL_FILE="${URL_FILE:-seeds/urls.txt}"
EXPERIMENT_LIMIT="${EXPERIMENT_LIMIT:-}"
EXPERIMENT_BATCH_SIZE="${EXPERIMENT_BATCH_SIZE:-100}"
EXPERIMENT_CRAWLER_ID="${EXPERIMENT_CRAWLER_ID:-0}"
CRAWLER_LOG_LEVEL="${CRAWLER_LOG_LEVEL:-WARNING}"
CRAWLER_DOMAIN_QPS_LOG_ENABLED="${CRAWLER_DOMAIN_QPS_LOG_ENABLED:-true}"
CRAWLER_DOMAIN_QPS_LOG_INTERVAL="${CRAWLER_DOMAIN_QPS_LOG_INTERVAL:-1.0}"
CRAWLER_DOMAIN_QPS_WINDOW_SECONDS="${CRAWLER_DOMAIN_QPS_WINDOW_SECONDS:-5.0}"

HOST_EXPERIMENT_ROOT="data/experiments/${EXPERIMENT_LABEL}/${MODE}"
CONTAINER_EXPERIMENT_ROOT="/app/${HOST_EXPERIMENT_ROOT}"
QUEUE_TEMPLATE="${CONTAINER_EXPERIMENT_ROOT}/url_queue/crawler_{id:02d}"
RESULT_TEMPLATE="${CONTAINER_EXPERIMENT_ROOT}/crawl_result/crawler_{id:02d}"
METRICS_TEMPLATE="${CONTAINER_EXPERIMENT_ROOT}/metrics/crawler_{id:02d}"

STAGE_ARGS=(
  python3
  scripts/stage_crawler_experiment.py
  --file "$URL_FILE"
  --experiment-root "$ROOT_DIR/$HOST_EXPERIMENT_ROOT"
  --crawler-id "$EXPERIMENT_CRAWLER_ID"
  --batch-size "$EXPERIMENT_BATCH_SIZE"
  --reset
)

if [[ -n "$EXPERIMENT_LIMIT" ]]; then
  STAGE_ARGS+=(--limit "$EXPERIMENT_LIMIT")
fi

"${STAGE_ARGS[@]}"

START_TS="$(date -u +"%Y-%m-%dT%H:%M:%S+00:00")"

if [[ "$MODE" == "fixed" ]]; then
  USE_AUTOTHROTTLE=false
else
  USE_AUTOTHROTTLE=true
fi

docker compose run --rm \
  -e CRAWLER_USE_AUTOTHROTTLE="$USE_AUTOTHROTTLE" \
  -e CRAWLER_EXIT_ON_IDLE=true \
  -e CRAWLER_URL_QUEUE_TEMPLATE="$QUEUE_TEMPLATE" \
  -e CRAWLER_RESULT_DIR_TEMPLATE="$RESULT_TEMPLATE" \
  -e CRAWLER_METRICS_DIR_TEMPLATE="$METRICS_TEMPLATE" \
  -e CRAWLER_DOMAIN_QPS_LOG_ENABLED="$CRAWLER_DOMAIN_QPS_LOG_ENABLED" \
  -e CRAWLER_DOMAIN_QPS_LOG_INTERVAL="$CRAWLER_DOMAIN_QPS_LOG_INTERVAL" \
  -e CRAWLER_DOMAIN_QPS_WINDOW_SECONDS="$CRAWLER_DOMAIN_QPS_WINDOW_SECONDS" \
  -e CRAWLER_LOG_LEVEL="$CRAWLER_LOG_LEVEL" \
  crawler \
  bash -lc "cd /app/containers/crawler && scrapy crawl html_spider -a crawler_id=${EXPERIMENT_CRAWLER_ID} --loglevel ${CRAWLER_LOG_LEVEL}"

END_TS="$(date -u +"%Y-%m-%dT%H:%M:%S+00:00")"

python3 scripts/summarize_crawl_results.py \
  --root "$ROOT_DIR/$HOST_EXPERIMENT_ROOT/crawl_result" \
  --since "$START_TS" \
  --until "$END_TS" \
  --label "${EXPERIMENT_LABEL}-${MODE}"

echo "domain_qps_metrics: $ROOT_DIR/$HOST_EXPERIMENT_ROOT/metrics/crawler_$(printf '%02d' "$EXPERIMENT_CRAWLER_ID")/domain_qps.jsonl"
