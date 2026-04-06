#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

DSN="${DSN:-postgresql+psycopg2://crawler:crawler@postgres:5432/crawlerdb}"
URL_FILE="${URL_FILE:-/app/seeds/urls.txt}"
NUM_SHARDS="${NUM_SHARDS:-256}"
SHARDS_PER_INGESTOR="${SHARDS_PER_INGESTOR:-16}"
INGEST_SERVICE="${INGEST_SERVICE:-scheduler_ingest}"

docker compose up -d postgres

docker compose run --rm "$INGEST_SERVICE" \
  python /app/scripts/init_db.py \
  --dsn "$DSN" \
  --num-shards "$NUM_SHARDS"

docker compose run --rm "$INGEST_SERVICE" \
  python /app/scripts/seed_urls.py \
  --dsn "$DSN" \
  --file "$URL_FILE" \
  --num-shards "$NUM_SHARDS" \
  --shards-per-ingestor "$SHARDS_PER_INGESTOR"

docker compose run --rm "$INGEST_SERVICE" \
  python /app/scripts/repair_crawl_flags.py \
  --dsn "$DSN" \
  --num-shards "$NUM_SHARDS"

docker compose up -d
