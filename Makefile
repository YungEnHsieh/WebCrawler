ifneq (,$(wildcard .env))
include .env
endif

COMPOSE_PROJECT_NAME ?= webcrawler
POSTGRES_PORT ?= 5432
POSTGRES_HOST_DIR ?= ./data/postgres
IPC_HOST_DIR ?= ./data/ipc
CRAWL_RESULT_ROOT ?= $(IPC_HOST_DIR)/crawl_result

DSN ?= postgresql+psycopg2://crawler:crawler@postgres:5432/crawlerdb
URL_FILE ?= /app/seeds/urls.txt
NUM_SHARDS ?= 256
SHARDS_PER_INGESTOR ?= 16
INGEST_SERVICE ?= scheduler_ingest
RESULT_MINUTES ?= 60
RESULT_LABEL ?= run
PLOT_MINUTES ?= 60
PLOT_BUCKET_SECONDS ?= 60
PLOT_TOP_DOMAINS ?= 8
PLOT_OUTPUT ?= tmp/crawl_metrics.html
CLEANUP_MAX_BUCKETS ?= 0

export COMPOSE_PROJECT_NAME
export POSTGRES_PORT
export POSTGRES_HOST_DIR
export IPC_HOST_DIR
export CRAWL_RESULT_ROOT
export DSN
export URL_FILE
export NUM_SHARDS
export SHARDS_PER_INGESTOR
export INGEST_SERVICE

.PHONY: bootstrap init-db seed-urls repair-crawl-flags up-postgres up crawler-fixed crawler-autothrottle summarize-crawl plot-crawl-metrics cleanup-ingestor-buckets cleanup-ingestor-buckets-apply

bootstrap:
	bash scripts/bootstrap.sh

up-postgres:
	docker compose up -d postgres

init-db:
	docker compose run --rm $(INGEST_SERVICE) \
		python /app/scripts/init_db.py \
		--dsn "$(DSN)" \
		--num-shards "$(NUM_SHARDS)"

seed-urls:
	docker compose run --rm $(INGEST_SERVICE) \
		python /app/scripts/seed_urls.py \
		--dsn "$(DSN)" \
		--file "$(URL_FILE)" \
		--num-shards "$(NUM_SHARDS)" \
		--shards-per-ingestor "$(SHARDS_PER_INGESTOR)"

repair-crawl-flags:
	docker compose run --rm $(INGEST_SERVICE) \
		python /app/scripts/repair_crawl_flags.py \
		--dsn "$(DSN)" \
		--num-shards "$(NUM_SHARDS)"

up:
	docker compose up -d

crawler-fixed:
	CRAWLER_USE_AUTOTHROTTLE=false docker compose up -d --force-recreate crawler

crawler-autothrottle:
	CRAWLER_USE_AUTOTHROTTLE=true docker compose up -d --force-recreate crawler

summarize-crawl:
	python3 scripts/summarize_crawl_results.py \
		--root "$(CRAWL_RESULT_ROOT)" \
		--minutes "$(RESULT_MINUTES)" \
		--label "$(RESULT_LABEL)"

plot-crawl-metrics:
	python3 scripts/plot_crawl_metrics.py \
		--root "$(CRAWL_RESULT_ROOT)" \
		--minutes "$(PLOT_MINUTES)" \
		--bucket-seconds "$(PLOT_BUCKET_SECONDS)" \
		--top-domains "$(PLOT_TOP_DOMAINS)" \
		--output "$(PLOT_OUTPUT)"

cleanup-ingestor-buckets:
	python3 scripts/cleanup_ingestor_buckets.py \
		--crawl-root "$(CRAWL_RESULT_ROOT)" \
		--max-buckets "$(CLEANUP_MAX_BUCKETS)"

cleanup-ingestor-buckets-apply:
	python3 scripts/cleanup_ingestor_buckets.py \
		--crawl-root "$(CRAWL_RESULT_ROOT)" \
		--max-buckets "$(CLEANUP_MAX_BUCKETS)" \
		--apply
